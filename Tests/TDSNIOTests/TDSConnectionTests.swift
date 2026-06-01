import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOSSL
import NIOTestUtils
import XCTest

@testable import TDSNIO

extension TDSTests {
    func testChannelQueryTaskStreamsOnlyFirstResultSet() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT 1; SELECT 2", streamPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneMetadataPayload()
            ))

        let stream = try streamPromise.futureResult.wait()
        let rowsFuture = stream.all()

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneRowPayload(id: 1, label: "one")
            ))
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .more)
            ))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneMetadataPayload()
            ))
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneRowPayload(id: 2, label: "two")
            ))
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))

        let rows = try rowsFuture.wait()
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0]["id"], .int32(1))
        XCTAssertEqual(rows[0]["label"], .string("one"))
    }

    func testDoneErrorStatusFailsActiveQueryWithoutErrorToken() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        XCTAssertThrowsError(try queryPromise.futureResult.wait()) { error in
            let sqlError = error as? TDSSQLError
            XCTAssertEqual(sqlError?.code, .server)
            XCTAssertEqual(sqlError?.query?.sql, "SELECT broken")
        }
    }

    func testDoneInProcErrorStatusWithoutErrorTokenDoesNotFailQuery() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("EXEC proc_with_internal_status", queryPromise))
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))

        var payload = Self.doneInProcPayload(status: .error)
        var done = Self.donePayload()
        payload.writeBuffer(&done)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.rows, [])
        XCTAssertNil(result.rowsAffected)
    }

    func testErrorTokenKeepsQueuedRequestUntilFinalDone() throws {
        let channel = try Self.loggedInChannel()

        let firstPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", firstPromise))
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.errorPayload(message: "Invalid object name")
            ))

        let firstCompleted = NIOLockedValueBox(false)
        firstPromise.futureResult.whenComplete { _ in
            firstCompleted.withLockedValue { $0 = true }
        }
        XCTAssertFalse(firstCompleted.withLockedValue { $0 })

        let secondPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", secondPromise))
        let secondCompleted = NIOLockedValueBox(false)
        secondPromise.futureResult.whenComplete { _ in
            secondCompleted.withLockedValue { $0 = true }
        }
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
        XCTAssertFalse(secondCompleted.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        XCTAssertThrowsError(try firstPromise.futureResult.wait()) { error in
            XCTAssertEqual((error as? TDSSQLError)?.serverInfo?.message, "Invalid object name")
        }
        let sqlBatch = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
    }

    func testConnectionQueuesRequestsAndSendsNextAfterDone() throws {
        let channel = try Self.loggedInChannel()

        let firstPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        let secondPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", firstPromise))
        let firstOutbound = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(firstOutbound.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 2", secondPromise))
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        let firstResult = try firstPromise.futureResult.wait()
        XCTAssertEqual(firstResult.rows.map(\.values), [[.int32(1), .string("one")]])
        let secondOutbound = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(secondOutbound.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        let secondResult = try secondPromise.futureResult.wait()
        XCTAssertEqual(secondResult.rows.map(\.values), [[.int32(1), .string("one")]])
    }

    func testResetConnectionEventAppliesToNextRequestOnly() throws {
        let channel = try Self.loggedInChannel()

        try channel.triggerUserOutboundEvent(TDSSQLEvent.resetConnectionOnNextRequest).wait()

        let firstPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", firstPromise))
        let firstOutbound = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(firstOutbound.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(
            firstOutbound.getInteger(at: 1, as: UInt8.self),
            TDSPacket.StatusFlag.eom.rawValue | TDSPacket.StatusFlag.resetConnection.rawValue
        )

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))
        _ = try firstPromise.futureResult.wait()

        let secondPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 2", secondPromise))
        let secondOutbound = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(secondOutbound.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(secondOutbound.getInteger(at: 1, as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
    }

    func testConnectionDoesNotFireReadyBetweenQueuedRequests() throws {
        let recorder = UserEventRecorder()
        let channel = try Self.loggedInChannel(recordingEventsWith: recorder)
        recorder.events.removeAll()

        let firstPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        let secondPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", firstPromise))
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 2", secondPromise))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        _ = try firstPromise.futureResult.wait()
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(Self.readyForQueryEventCount(in: recorder.events), 0)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        _ = try secondPromise.futureResult.wait()
        XCTAssertEqual(Self.readyForQueryEventCount(in: recorder.events), 1)
    }

    func testInfoTokenInvokesHandlerAndDoesNotFailQuery() throws {
        let infoMessages = NIOLockedValueBox<[TDSInfoMessage]>([])
        var configuration = Self.configuration()
        configuration.options.infoMessageHandler = { message in
            infoMessages.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("PRINT 'hello'; SELECT 1", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        var payload = Self.infoPayload(message: "hello from server", number: 0, severity: 0)
        var resultPayload = Self.selectOneTokenStreamPayload()
        payload.writeBuffer(&resultPayload)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.rows.map(\.values), [[.int32(1), .string("one")]])
        let messages = infoMessages.withLockedValue { $0 }
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages[0].message, "hello from server")
        XCTAssertEqual(messages[0].severity, 0)
    }

    func testErrorTokenInvokesHandlerAndFailsQuery() throws {
        let errorMessages = NIOLockedValueBox<[TDSErrorMessage]>([])
        var configuration = Self.configuration()
        configuration.options.errorMessageHandler = { message in
            errorMessages.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.errorPayload(message: "Invalid object name", number: 208, severity: 16)
            ))

        let queryCompleted = NIOLockedValueBox(false)
        queryPromise.futureResult.whenComplete { _ in
            queryCompleted.withLockedValue { $0 = true }
        }
        XCTAssertFalse(queryCompleted.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        XCTAssertThrowsError(try queryPromise.futureResult.wait()) { error in
            XCTAssertEqual((error as? TDSSQLError)?.serverInfo?.message, "Invalid object name")
        }
        let messages = errorMessages.withLockedValue { $0 }
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages[0].message, "Invalid object name")
        XCTAssertEqual(messages[0].number, 208)
        XCTAssertEqual(messages[0].severity, 16)
    }

    func testMultipleErrorTokensAreAggregatedUntilDone() throws {
        let errorMessages = NIOLockedValueBox<[TDSErrorMessage]>([])
        var configuration = Self.configuration()
        configuration.options.errorMessageHandler = { message in
            errorMessages.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", queryPromise))
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))

        var payload = Self.errorPayload(message: "First failure", number: 50001)
        var secondError = Self.errorPayload(message: "Second failure", number: 50002)
        payload.writeBuffer(&secondError)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let queryCompleted = NIOLockedValueBox(false)
        queryPromise.futureResult.whenComplete { _ in
            queryCompleted.withLockedValue { $0 = true }
        }
        XCTAssertFalse(queryCompleted.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        XCTAssertThrowsError(try queryPromise.futureResult.wait()) { error in
            let sqlError = error as? TDSSQLError
            XCTAssertEqual(sqlError?.serverInfo?.message, "First failure")
            XCTAssertEqual(sqlError?.serverErrors.map(\.message), ["First failure", "Second failure"])
            XCTAssertEqual(sqlError?.serverErrors.map(\.number), [50001, 50002])
        }

        let messages = errorMessages.withLockedValue { $0 }
        XCTAssertEqual(messages.map(\.message), ["First failure", "Second failure"])
    }

    func testEnvChangeTokenInvokesHandlerAndDoesNotFailQuery() throws {
        let envChanges = NIOLockedValueBox<[TDSEnvChangeMessage]>([])
        var configuration = Self.configuration()
        configuration.options.envChangeHandler = { message in
            envChanges.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("USE tempdb; SELECT 1", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        var payload = Self.stringEnvChangePayload(type: 1, new: "tempdb", old: "master")
        var resultPayload = Self.selectOneTokenStreamPayload()
        payload.writeBuffer(&resultPayload)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.rows.map(\.values), [[.int32(1), .string("one")]])
        let changes = envChanges.withLockedValue { $0 }
        XCTAssertEqual(changes.count, 1)
        XCTAssertEqual(changes[0].type, 1)
        XCTAssertEqual(changes[0].value, .string(new: "tempdb", old: "master"))
    }

    func testResetConnectionEnvChangeFiresResetEventAndDoesNotFailQuery() throws {
        let envChanges = NIOLockedValueBox<[TDSEnvChangeMessage]>([])
        let recorder = UserEventRecorder()
        var configuration = Self.configuration()
        configuration.options.envChangeHandler = { message in
            envChanges.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration, recordingEventsWith: recorder)
        recorder.events.removeAll()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        var payload = Self.resetConnectionEnvChangePayload()
        var resultPayload = Self.selectOneTokenStreamPayload()
        payload.writeBuffer(&resultPayload)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.rows.map(\.values), [[.int32(1), .string("one")]])
        XCTAssertEqual(Self.resetConnectionEventCount(in: recorder.events), 1)
        let changes = envChanges.withLockedValue { $0 }
        XCTAssertEqual(changes.count, 1)
        XCTAssertEqual(changes[0].type, 18)
        XCTAssertEqual(changes[0].value, .bytes(new: [], old: []))
    }

    func testSessionStateTokenInvokesHandlerAndDoesNotFailQuery() throws {
        let sessionStates = NIOLockedValueBox<[TDSSessionStateMessage]>([])
        let recorder = UserEventRecorder()
        var configuration = Self.configuration()
        configuration.options.sessionStateHandler = { message in
            sessionStates.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration, recordingEventsWith: recorder)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        var payload = Self.sessionStatePayload(
            sequenceNumber: 7,
            status: 0x01,
            entries: [(stateID: 9, value: [0xAA, 0xBB]), (stateID: 3, value: [0xCC])]
        )
        var resultPayload = Self.selectOneTokenStreamPayload()
        payload.writeBuffer(&resultPayload)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.rows.map(\.values), [[.int32(1), .string("one")]])
        let messages = sessionStates.withLockedValue { $0 }
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages[0].sequenceNumber, 7)
        XCTAssertEqual(messages[0].status, 0x01)
        XCTAssertTrue(messages[0].isRecoverable)
        XCTAssertEqual(
            messages[0].entries,
            [
                .init(stateID: 9, value: [0xAA, 0xBB]),
                .init(stateID: 3, value: [0xCC]),
            ])
        XCTAssertTrue(recorder.events.contains { $0 is TDSSessionStateMessage })
    }

    func testStartupPipelineForwardsAuthenticationChallenges() throws {
        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds-nio-tests")
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            database: "master",
            tls: .disable,
            clientHostName: "client"
        )
        let eventHandler = TDSEventsHandler(logger: logger)
        let recorder = UserEventRecorder()
        let channelHandler = TDSChannelHandler(
            configuration: configuration,
            logger: logger
        )
        let postprocessor = TDSFrontendMessagePostProcessor(packetLength: configuration.packetSize)

        try channel.pipeline.syncOperations.addHandler(eventHandler)
        try channel.pipeline.syncOperations.addHandler(recorder)
        try channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(eventHandler))
        try channel.pipeline.syncOperations.addHandler(postprocessor, position: .before(channelHandler))

        channel.pipeline.fireChannelActive()
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        var sspiPayload = ByteBufferAllocator().buffer(capacity: 8)
        sspiPayload.writeLengthPrefixedToken(0xED, bytes: [0xAA, 0xBB])
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: sspiPayload
            ))

        var fedAuthInfo = ByteBufferAllocator().buffer(capacity: 128)
        fedAuthInfo.writeFedAuthInfo(
            stsURL: "https://sts.example.test",
            spn: "MSSQLSvc/sql.example.test:1433"
        )
        var fedAuthPayload = ByteBufferAllocator().buffer(capacity: 128)
        fedAuthPayload.writeLongLengthPrefixedToken(0xEE, bytes: Array(fedAuthInfo.readableBytesView))
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: fedAuthPayload
            ))

        let challenges = recorder.events.compactMap { $0 as? TDSAuthenticationChallenge }
        XCTAssertEqual(challenges.count, 2)
        XCTAssertEqual(challenges.first, .sspi([0xAA, 0xBB]))
        guard case .federatedInfo(let info) = challenges.last else {
            return XCTFail("Expected federated auth info challenge")
        }
        XCTAssertEqual(info.options.map(\.id), [0x01, 0x02])
        XCTAssertEqual(info.stsURL, "https://sts.example.test")
        XCTAssertEqual(info.spn, "MSSQLSvc/sql.example.test:1433")
    }

    func testQueryResultIncludesOptionalMetadataTokens() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1 ORDER BY 1", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.optionalMetadataTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["id"])
        XCTAssertEqual(result.columns[0].metadata.baseTableName, "dbo")
        XCTAssertEqual(result.columns[0].metadata.tableNumber, 1)
        XCTAssertEqual(result.columns[0].metadata.baseColumnName, "baseId")
        XCTAssertFalse(result.columns[0].metadata.isExpression)
        XCTAssertFalse(result.columns[0].metadata.isKey)
        XCTAssertFalse(result.columns[0].metadata.isHidden)
        XCTAssertTrue(result.columns[0].metadata.isOrderBy)
        XCTAssertEqual(result.offsets, [.init(identifier: 0x0102, offset: 42)])
        XCTAssertEqual(result.resultSets[0].offsets, result.offsets)
        XCTAssertEqual(result.rows.count, 1)
        XCTAssertEqual(result.rows[0].cell(named: "id")?.columnMetadata.baseColumnName, "baseId")
        XCTAssertEqual(result.rows[0].values, [.int32(1)])
    }

    func testQueryResultIncludesDataClassificationMetadata() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT sensitive amount", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.dataClassificationTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["amount"])
        XCTAssertEqual(
            result.columns[0].metadata.sensitivityClassifications,
            [
                .init(
                    labelName: "Confidential",
                    labelID: "label-id",
                    informationTypeName: "Financial",
                    informationTypeID: "info-id",
                    rank: 10
                )
            ])
        XCTAssertEqual(result.rows[0].cell(named: "amount")?.columnMetadata.sensitivityClassifications.first?.rank, 10)
        XCTAssertEqual(result.rows.map(\.values), [[.int32(42)]])
    }

    func testQueryResultIncludesPLPMaxValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST('hello world' AS nvarchar(max))", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.plpMaxTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["text", "blob"])
        XCTAssertEqual(result.rows.count, 2)
        XCTAssertEqual(result.rows[0]["text"], .string("hello world"))
        XCTAssertEqual(result.rows[0]["blob"], .bytes([0xDE, 0xAD, 0xBE, 0xEF]))
        XCTAssertEqual(result.rows[1].values, [.null, .null])
    }

    func testQueryResultIncludesXMLValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST('<r/>' AS xml)", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.xmlTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["doc", "typedDoc"])
        XCTAssertNil(result.columns[0].metadata.xmlInfo)
        XCTAssertEqual(
            result.columns[1].metadata.xmlInfo,
            .init(
                databaseName: "master",
                owningSchema: "dbo",
                schemaCollection: "docSchema"
            ))
        XCTAssertEqual(
            result.rows[0].cell(named: "typedDoc")?.columnMetadata.xmlInfo,
            .init(
                databaseName: "master",
                owningSchema: "dbo",
                schemaCollection: "docSchema"
            ))
        XCTAssertEqual(result.rows.count, 2)
        XCTAssertEqual(result.rows[0]["doc"], .xml([0x3C, 0x72, 0x2F, 0x3E]))
        XCTAssertEqual(result.rows[0]["typedDoc"], .xml([0x01, 0x02, 0x03]))
        XCTAssertEqual(result.rows[1].values, [.null, .null])
    }

    func testQueryResultIncludesJSONValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT JSON_OBJECT('ok': true)", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.jsonTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.dataType), [.json])
        XCTAssertEqual(result.rows.map(\.values), [[.json(Array(#"{"ok":true}"#.utf8))], [.null]])
    }

    func testQueryResultIncludesNullTypeValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT NULL", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.nullTypeTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.dataType), [.null])
        XCTAssertEqual(result.rows.map(\.values), [[.null]])
    }

    func testQueryResultIncludesSQLVariantValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST(42 AS sql_variant)", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.sqlVariantTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.dataType), [.sqlVariant])
        XCTAssertEqual(result.rows.map(\.values), [[.int32(42)], [.string("variant")]])
    }

    func testQueryResultIncludesUDTValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT geography::Point(0, 0, 4326)", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.udtTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.dataType), [.udt])
        XCTAssertEqual(result.columns[0].metadata.length, UInt64(UInt16.max))
        XCTAssertEqual(result.columns[0].metadata.udtInfo?.databaseName, "master")
        XCTAssertEqual(result.columns[0].metadata.udtInfo?.schemaName, "sys")
        XCTAssertEqual(result.columns[0].metadata.udtInfo?.typeName, "geography")
        XCTAssertEqual(
            result.columns[0].metadata.udtInfo?.assemblyQualifiedName,
            "Microsoft.SqlServer.Types.SqlGeography"
        )
        XCTAssertEqual(result.rows.map(\.values), [[.bytes([0xE6, 0x10, 0x00, 0x01])], [.null]])
    }

    func testQueryResultIncludesLegacyCharAndBinaryValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT legacy character and binary values", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.legacyCharBinaryTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["varchar", "char", "varbinary", "binary"])
        XCTAssertEqual(result.rows.count, 2)
        XCTAssertEqual(
            result.rows[0].values, [.string("hello"), .string("abc"), .bytes([0xDE, 0xAD]), .bytes([0xBE, 0xEF])])
        XCTAssertEqual(result.rows[1].values, [.null, .string("xyz"), .null, .bytes([0x12, 0x34])])
        XCTAssertEqual(result.rows[0]["varbinary"], .bytes([0xDE, 0xAD]))
    }

    func testQueryResultIncludesLegacyLOBValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT legacy LOB values", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.legacyLOBTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["body", "unicodeBody", "picture"])
        XCTAssertEqual(result.rows.count, 2)
        XCTAssertEqual(result.rows[0].values, [.string("hello text"), .string("wide text"), .bytes([0xCA, 0xFE])])
        XCTAssertEqual(result.rows[1].values, [.null, .null, .null])
        XCTAssertEqual(result.rows[0]["unicodeBody"], .string("wide text"))
    }

    func testQueryResultIncludesDecimalValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST(123.45 AS decimal(10,2))", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.decimalTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["amount"])
        XCTAssertEqual(result.columns[0].metadata.length, 5)
        XCTAssertEqual(result.columns[0].metadata.precision, 10)
        XCTAssertEqual(result.columns[0].metadata.scale, 2)
        XCTAssertEqual(result.rows.map(\.values), [[.decimal("123.45")], [.decimal("-1.23")]])
        XCTAssertEqual(result.rows[0]["amount"], .decimal("123.45"))
    }

    func testQueryResultIncludesGUIDValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(
            TDSTask.sqlBatch("SELECT CAST('00112233-4455-6677-8899-aabbccddeeff' AS uniqueidentifier)", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.guidTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["id"])
        XCTAssertEqual(result.rows.map(\.values), [[.guid(Self.guid)], [.null]])
        XCTAssertEqual(result.rows[0]["id"], .guid(Self.guid))
    }

    func testQueryResultIncludesMultipleResultSets() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1; SELECT N'two'", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.multiResultSetTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["id"])
        XCTAssertEqual(result.rows.map(\.values), [[.int32(1)]])
        XCTAssertEqual(result.resultSets.count, 2)
        XCTAssertEqual(result.resultSets[0].columns.map(\.name), ["id"])
        XCTAssertEqual(result.resultSets[0].rows.map(\.values), [[.int32(1)]])
        XCTAssertEqual(result.resultSets[0].rowsAffected, 1)
        XCTAssertEqual(result.resultSets[1].columns.map(\.name), ["label"])
        XCTAssertEqual(result.resultSets[1].rows.map(\.values), [[.string("two")]])
        XCTAssertEqual(result.resultSets[1].rowsAffected, 1)
    }

    func testDoneInProcDoesNotCompleteActiveQuery() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        let completed = NIOLockedValueBox(false)
        queryPromise.futureResult.whenComplete { _ in
            completed.withLockedValue { $0 = true }
        }

        try channel.writeOutbound(TDSTask.rpc(.init(procedure: "dbo.two_results"), queryPromise))
        let rpc: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(rpc.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.doneInProcFirstResultSetPayload()
            ))

        XCTAssertFalse(completed.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertTrue(completed.withLockedValue { $0 })
        XCTAssertEqual(result.resultSets.count, 2)
        XCTAssertEqual(result.resultSets[0].rows.map(\.values), [[.int32(1), .string("one")]])
        XCTAssertEqual(result.resultSets[0].rowsAffected, 1)
        XCTAssertEqual(result.resultSets[1].rows.map(\.values), [[.int32(1), .string("one")]])
    }

    func testStartupPipelineCapturesRoutingEnvChange() throws {
        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds-nio-tests")
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            database: "master",
            tls: .disable,
            clientHostName: "client"
        )

        let eventHandler = TDSEventsHandler(logger: logger)
        let channelHandler = TDSChannelHandler(
            configuration: configuration,
            logger: logger
        )
        let postprocessor = TDSFrontendMessagePostProcessor(packetLength: configuration.packetSize)

        try channel.pipeline.syncOperations.addHandler(eventHandler)
        try channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(eventHandler))
        try channel.pipeline.syncOperations.addHandler(postprocessor, position: .before(channelHandler))

        channel.pipeline.fireChannelActive()
        _ = try channel.readOutbound(as: ByteBuffer.self)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        var payload = Self.routingEnvChangePayload()
        var loginAckAndDone = Self.loginAckAndDonePayload()
        payload.writeBuffer(&loginAckAndDone)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let context = try eventHandler.startupDoneFuture.wait()
        XCTAssertEqual(context.routing?.protocolByte, 0)
        XCTAssertEqual(context.routing?.port, 1444)
        XCTAssertEqual(context.routing?.server, "redirect.sql.example.test")
    }

    func testLoginRoutingSkipsInitialSQLBeforeRedirect() throws {
        var configuration = Self.configuration()
        configuration.options.initialSQL = "set ansi_nulls on"

        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds-nio-tests")
        let eventHandler = TDSEventsHandler(logger: logger)
        let channelHandler = TDSChannelHandler(configuration: configuration, logger: logger)
        let postprocessor = TDSFrontendMessagePostProcessor(packetLength: configuration.packetSize)

        try channel.pipeline.syncOperations.addHandler(eventHandler)
        try channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(eventHandler))
        try channel.pipeline.syncOperations.addHandler(postprocessor, position: .before(channelHandler))

        channel.pipeline.fireChannelActive()
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        var payload = Self.routingEnvChangePayload()
        var loginAckAndDone = Self.loginAckAndDonePayload()
        payload.writeBuffer(&loginAckAndDone)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let context = try eventHandler.startupDoneFuture.wait()
        XCTAssertEqual(context.routing?.server, "redirect.sql.example.test")
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
    }

    func testLoginSendsInitialSQLBeforeStartupDone() throws {
        var configuration = Self.configuration()
        configuration.options.initialSQL = "set ansi_nulls on"

        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds-nio-tests")
        let eventHandler = TDSEventsHandler(logger: logger)
        let channelHandler = TDSChannelHandler(configuration: configuration, logger: logger)
        let postprocessor = TDSFrontendMessagePostProcessor(packetLength: configuration.packetSize)
        let startupDone = NIOLockedValueBox(false)

        try channel.pipeline.syncOperations.addHandler(eventHandler)
        try channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(eventHandler))
        try channel.pipeline.syncOperations.addHandler(postprocessor, position: .before(channelHandler))
        eventHandler.startupDoneFuture.whenComplete { _ in
            startupDone.withLockedValue { $0 = true }
        }

        channel.pipeline.fireChannelActive()
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.loginAckAndDonePayload()
            ))

        var initialSQL = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(initialSQL.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        initialSQL.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        XCTAssertEqual(
            try XCTUnwrap(initialSQL.readUTF16(characterCount: initialSQL.readableBytes / 2)),
            "set ansi_nulls on"
        )
        XCTAssertFalse(startupDone.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))
        _ = try eventHandler.startupDoneFuture.wait()
        XCTAssertTrue(startupDone.withLockedValue { $0 })
    }

    func testLoginSendsInitialSessionSettingsBeforeStartupDone() throws {
        var configuration = Self.configuration()
        configuration.options.initialSessionSettings = .init(
            ansiNulls: true,
            textSize: 1024,
            isolationLevel: .snapshot
        )

        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds-nio-tests")
        let eventHandler = TDSEventsHandler(logger: logger)
        let channelHandler = TDSChannelHandler(configuration: configuration, logger: logger)
        let postprocessor = TDSFrontendMessagePostProcessor(packetLength: configuration.packetSize)
        let startupDone = NIOLockedValueBox(false)

        try channel.pipeline.syncOperations.addHandler(eventHandler)
        try channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(eventHandler))
        try channel.pipeline.syncOperations.addHandler(postprocessor, position: .before(channelHandler))
        eventHandler.startupDoneFuture.whenComplete { _ in
            startupDone.withLockedValue { $0 = true }
        }

        channel.pipeline.fireChannelActive()
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.loginAckAndDonePayload()
            ))

        var initialSQL = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(initialSQL.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        initialSQL.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        XCTAssertEqual(
            try XCTUnwrap(initialSQL.readUTF16(characterCount: initialSQL.readableBytes / 2)),
            """
            set ansi_nulls on
            set textsize 1024
            set transaction isolation level snapshot
            """
        )
        XCTAssertFalse(startupDone.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))
        _ = try eventHandler.startupDoneFuture.wait()
        XCTAssertTrue(startupDone.withLockedValue { $0 })
    }

    func testInitialSQLErrorInvokesHandlerAndFailsStartupOnDone() throws {
        let errors = NIOLockedValueBox<[TDSErrorMessage]>([])
        var configuration = Self.configuration()
        configuration.options.initialSQL = "set language invalid"
        configuration.options.errorMessageHandler = { message in
            errors.withLockedValue { $0.append(message) }
        }

        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds-nio-tests")
        let eventHandler = TDSEventsHandler(logger: logger)
        let channelHandler = TDSChannelHandler(configuration: configuration, logger: logger)
        let postprocessor = TDSFrontendMessagePostProcessor(packetLength: configuration.packetSize)

        try channel.pipeline.syncOperations.addHandler(eventHandler)
        try channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(eventHandler))
        try channel.pipeline.syncOperations.addHandler(postprocessor, position: .before(channelHandler))

        channel.pipeline.fireChannelActive()
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.loginAckAndDonePayload()
            ))
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))

        var payload = Self.errorPayload(message: "Invalid language", number: 50000)
        var done = Self.donePayload(status: .error)
        payload.writeBuffer(&done)
        XCTAssertThrowsError(
            try channel.writeInbound(
                Self.packet(
                    type: .preloginLoginOrTablularResponse,
                    payload: payload
                ))
        ) { error in
            guard let sqlError = error as? TDSSQLError else {
                return XCTFail("Expected TDSSQLError, got \(error)")
            }
            XCTAssertEqual(sqlError.serverInfo?.message, "Invalid language")
        }
        XCTAssertEqual(errors.withLockedValue { $0.map(\.message) }, ["Invalid language"])
    }
}
