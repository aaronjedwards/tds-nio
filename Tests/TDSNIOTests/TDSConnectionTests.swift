//===----------------------------------------------------------------------===//
//
// This source file is part of the TDSNIO open source project
//
// Copyright (c) 2026 Aaron Edwards and the TDSNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
// See CONTRIBUTORS.md for the list of TDSNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//
import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOSSL
import NIOTestUtils
import Testing

@testable import TDSNIO

extension TDSTests {
    @Test func channelQueryTaskStreamsOnlyFirstResultSet() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT 1; SELECT 2", streamPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

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
        expectEqual(rows.count, 1)
        expectEqual(rows[0]["id"], .int32(1))
        expectEqual(rows[0]["label"], .string("one"))
    }

    @Test func doneErrorStatusFailsActiveQueryWithoutErrorToken() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        expectThrowsError(try queryPromise.futureResult.wait()) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .server)
            expectEqual(sqlError?.query?.sql, "SELECT broken")
        }
    }

    @Test func doneInProcErrorStatusWithoutErrorTokenDoesNotFailQuery() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("EXEC proc_with_internal_status", queryPromise))
        _ = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))

        var payload = Self.doneInProcPayload(status: .error)
        var done = Self.donePayload()
        payload.writeBuffer(&done)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.rows, [])
        expectNil(result.rowsAffected)
    }

    @Test func errorTokenKeepsQueuedRequestUntilFinalDone() throws {
        let channel = try Self.loggedInChannel()

        let firstPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", firstPromise))
        _ = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.errorPayload(message: "Invalid object name")
            ))

        let firstCompleted = NIOLockedValueBox(false)
        firstPromise.futureResult.whenComplete { _ in
            firstCompleted.withLockedValue { $0 = true }
        }
        expectFalse(firstCompleted.withLockedValue { $0 })

        let secondPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", secondPromise))
        let secondCompleted = NIOLockedValueBox(false)
        secondPromise.futureResult.whenComplete { _ in
            secondCompleted.withLockedValue { $0 = true }
        }
        expectNil(try channel.readOutbound(as: ByteBuffer.self))
        expectFalse(secondCompleted.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        expectThrowsError(try firstPromise.futureResult.wait()) { error in
            expectEqual((error as? TDSSQLError)?.serverInfo?.message, "Invalid object name")
        }
        let sqlBatch = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
    }

    @Test func connectionQueuesRequestsAndSendsNextAfterDone() throws {
        let channel = try Self.loggedInChannel()

        let firstPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        let secondPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", firstPromise))
        let firstOutbound = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(firstOutbound.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 2", secondPromise))
        expectNil(try channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        let firstResult = try firstPromise.futureResult.wait()
        expectEqual(firstResult.rows.map(\.values), [[.int32(1), .string("one")]])
        let secondOutbound = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(secondOutbound.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        let secondResult = try secondPromise.futureResult.wait()
        expectEqual(secondResult.rows.map(\.values), [[.int32(1), .string("one")]])
    }

    @Test func resetConnectionEventAppliesToNextRequestOnly() throws {
        let channel = try Self.loggedInChannel()

        try channel.triggerUserOutboundEvent(TDSSQLEvent.resetConnectionOnNextRequest).wait()

        let firstPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", firstPromise))
        let firstOutbound = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(firstOutbound.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(
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
        let secondOutbound = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(secondOutbound.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(secondOutbound.getInteger(at: 1, as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
    }

    @Test func connectionDoesNotFireReadyBetweenQueuedRequests() throws {
        let recorder = UserEventRecorder()
        let channel = try Self.loggedInChannel(recordingEventsWith: recorder)
        recorder.events.removeAll()

        let firstPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        let secondPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", firstPromise))
        _ = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 2", secondPromise))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        _ = try firstPromise.futureResult.wait()
        _ = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(Self.readyForQueryEventCount(in: recorder.events), 0)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        _ = try secondPromise.futureResult.wait()
        expectEqual(Self.readyForQueryEventCount(in: recorder.events), 1)
    }

    @Test func infoTokenInvokesHandlerAndDoesNotFailQuery() throws {
        let infoMessages = NIOLockedValueBox<[TDSInfoMessage]>([])
        var configuration = Self.configuration()
        configuration.options.infoMessageHandler = { message in
            infoMessages.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("PRINT 'hello'; SELECT 1", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        var payload = Self.infoPayload(message: "hello from server", number: 0, severity: 0)
        var resultPayload = Self.selectOneTokenStreamPayload()
        payload.writeBuffer(&resultPayload)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.rows.map(\.values), [[.int32(1), .string("one")]])
        let messages = infoMessages.withLockedValue { $0 }
        expectEqual(messages.count, 1)
        expectEqual(messages[0].message, "hello from server")
        expectEqual(messages[0].severity, 0)
    }

    @Test func errorTokenInvokesHandlerAndFailsQuery() throws {
        let errorMessages = NIOLockedValueBox<[TDSErrorMessage]>([])
        var configuration = Self.configuration()
        configuration.options.errorMessageHandler = { message in
            errorMessages.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.errorPayload(message: "Invalid object name", number: 208, severity: 16)
            ))

        let queryCompleted = NIOLockedValueBox(false)
        queryPromise.futureResult.whenComplete { _ in
            queryCompleted.withLockedValue { $0 = true }
        }
        expectFalse(queryCompleted.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        expectThrowsError(try queryPromise.futureResult.wait()) { error in
            expectEqual((error as? TDSSQLError)?.serverInfo?.message, "Invalid object name")
        }
        let messages = errorMessages.withLockedValue { $0 }
        expectEqual(messages.count, 1)
        expectEqual(messages[0].message, "Invalid object name")
        expectEqual(messages[0].number, 208)
        expectEqual(messages[0].severity, 16)
    }

    @Test func multipleErrorTokensAreAggregatedUntilDone() throws {
        let errorMessages = NIOLockedValueBox<[TDSErrorMessage]>([])
        var configuration = Self.configuration()
        configuration.options.errorMessageHandler = { message in
            errorMessages.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", queryPromise))
        _ = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))

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
        expectFalse(queryCompleted.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        expectThrowsError(try queryPromise.futureResult.wait()) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.serverInfo?.message, "First failure")
            expectEqual(sqlError?.serverErrors.map(\.message), ["First failure", "Second failure"])
            expectEqual(sqlError?.serverErrors.map(\.number), [50001, 50002])
        }

        let messages = errorMessages.withLockedValue { $0 }
        expectEqual(messages.map(\.message), ["First failure", "Second failure"])
    }

    @Test func envChangeTokenInvokesHandlerAndDoesNotFailQuery() throws {
        let envChanges = NIOLockedValueBox<[TDSEnvChangeMessage]>([])
        var configuration = Self.configuration()
        configuration.options.envChangeHandler = { message in
            envChanges.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("USE tempdb; SELECT 1", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        var payload = Self.stringEnvChangePayload(type: 1, new: "tempdb", old: "master")
        var resultPayload = Self.selectOneTokenStreamPayload()
        payload.writeBuffer(&resultPayload)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.rows.map(\.values), [[.int32(1), .string("one")]])
        let changes = envChanges.withLockedValue { $0 }
        expectEqual(changes.count, 1)
        expectEqual(changes[0].type, 1)
        expectEqual(changes[0].value, .string(new: "tempdb", old: "master"))
    }

    @Test func resetConnectionEnvChangeFiresResetEventAndDoesNotFailQuery() throws {
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
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        var payload = Self.resetConnectionEnvChangePayload()
        var resultPayload = Self.selectOneTokenStreamPayload()
        payload.writeBuffer(&resultPayload)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: payload
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.rows.map(\.values), [[.int32(1), .string("one")]])
        expectEqual(Self.resetConnectionEventCount(in: recorder.events), 1)
        let changes = envChanges.withLockedValue { $0 }
        expectEqual(changes.count, 1)
        expectEqual(changes[0].type, 18)
        expectEqual(changes[0].value, .bytes(new: [], old: []))
    }

    @Test func sessionStateTokenInvokesHandlerAndDoesNotFailQuery() throws {
        let sessionStates = NIOLockedValueBox<[TDSSessionStateMessage]>([])
        let recorder = UserEventRecorder()
        var configuration = Self.configuration()
        configuration.options.sessionStateHandler = { message in
            sessionStates.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration, recordingEventsWith: recorder)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

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
        expectEqual(result.rows.map(\.values), [[.int32(1), .string("one")]])
        let messages = sessionStates.withLockedValue { $0 }
        expectEqual(messages.count, 1)
        expectEqual(messages[0].sequenceNumber, 7)
        expectEqual(messages[0].status, 0x01)
        expectTrue(messages[0].isRecoverable)
        expectEqual(
            messages[0].entries,
            [
                .init(stateID: 9, value: [0xAA, 0xBB]),
                .init(stateID: 3, value: [0xCC]),
            ])
        expectTrue(recorder.events.contains { $0 is TDSSessionStateMessage })
    }

    @Test func startupPipelineForwardsAuthenticationChallenges() throws {
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
        expectEqual(challenges.count, 2)
        expectEqual(challenges.first, .sspi([0xAA, 0xBB]))
        guard case .federatedInfo(let info) = challenges.last else {
            Issue.record("Expected federated auth info challenge")
            return
        }
        expectEqual(info.options.map(\.id), [0x01, 0x02])
        expectEqual(info.stsURL, "https://sts.example.test")
        expectEqual(info.spn, "MSSQLSvc/sql.example.test:1433")
    }

    @Test func queryResultIncludesOptionalMetadataTokens() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1 ORDER BY 1", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.optionalMetadataTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["id"])
        expectEqual(result.columns[0].metadata.baseTableName, "dbo")
        expectEqual(result.columns[0].metadata.tableNumber, 1)
        expectEqual(result.columns[0].metadata.baseColumnName, "baseId")
        expectFalse(result.columns[0].metadata.isExpression)
        expectFalse(result.columns[0].metadata.isKey)
        expectFalse(result.columns[0].metadata.isHidden)
        expectTrue(result.columns[0].metadata.isOrderBy)
        expectEqual(result.offsets, [.init(identifier: 0x0102, offset: 42)])
        expectEqual(result.resultSets[0].offsets, result.offsets)
        expectEqual(result.rows.count, 1)
        expectEqual(result.rows[0].cell(named: "id")?.columnMetadata.baseColumnName, "baseId")
        expectEqual(result.rows[0].values, [.int32(1)])
    }

    @Test func queryResultIncludesDataClassificationMetadata() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT sensitive amount", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.dataClassificationTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["amount"])
        expectEqual(
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
        expectEqual(result.rows[0].cell(named: "amount")?.columnMetadata.sensitivityClassifications.first?.rank, 10)
        expectEqual(result.rows.map(\.values), [[.int32(42)]])
    }

    @Test func queryResultIncludesPLPMaxValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST('hello world' AS nvarchar(max))", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.plpMaxTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["text", "blob"])
        expectEqual(result.rows.count, 2)
        expectEqual(result.rows[0]["text"], .string("hello world"))
        expectEqual(result.rows[0]["blob"], .bytes([0xDE, 0xAD, 0xBE, 0xEF]))
        expectEqual(result.rows[1].values, [.null, .null])
    }

    @Test func queryResultIncludesXMLValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST('<r/>' AS xml)", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.xmlTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["doc", "typedDoc"])
        expectNil(result.columns[0].metadata.xmlInfo)
        expectEqual(
            result.columns[1].metadata.xmlInfo,
            .init(
                databaseName: "master",
                owningSchema: "dbo",
                schemaCollection: "docSchema"
            ))
        expectEqual(
            result.rows[0].cell(named: "typedDoc")?.columnMetadata.xmlInfo,
            .init(
                databaseName: "master",
                owningSchema: "dbo",
                schemaCollection: "docSchema"
            ))
        expectEqual(result.rows.count, 2)
        expectEqual(result.rows[0]["doc"], .xml([0x3C, 0x72, 0x2F, 0x3E]))
        expectEqual(result.rows[0]["typedDoc"], .xml([0x01, 0x02, 0x03]))
        expectEqual(result.rows[1].values, [.null, .null])
    }

    @Test func queryResultIncludesJSONValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT JSON_OBJECT('ok': true)", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.jsonTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.dataType), [.json])
        expectEqual(result.rows.map(\.values), [[.json(Array(#"{"ok":true}"#.utf8))], [.null]])
    }

    @Test func queryResultIncludesNullTypeValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT NULL", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.nullTypeTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.dataType), [.null])
        expectEqual(result.rows.map(\.values), [[.null]])
    }

    @Test func queryResultIncludesSQLVariantValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST(42 AS sql_variant)", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.sqlVariantTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.dataType), [.sqlVariant])
        expectEqual(result.rows.map(\.values), [[.int32(42)], [.string("variant")]])
    }

    @Test func queryResultIncludesUDTValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT geography::Point(0, 0, 4326)", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.udtTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.dataType), [.udt])
        expectEqual(result.columns[0].metadata.length, UInt64(UInt16.max))
        expectEqual(result.columns[0].metadata.udtInfo?.databaseName, "master")
        expectEqual(result.columns[0].metadata.udtInfo?.schemaName, "sys")
        expectEqual(result.columns[0].metadata.udtInfo?.typeName, "geography")
        expectEqual(
            result.columns[0].metadata.udtInfo?.assemblyQualifiedName,
            "Microsoft.SqlServer.Types.SqlGeography"
        )
        expectEqual(result.rows.map(\.values), [[.bytes([0xE6, 0x10, 0x00, 0x01])], [.null]])
    }

    @Test func queryResultIncludesLegacyCharAndBinaryValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT legacy character and binary values", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.legacyCharBinaryTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["varchar", "char", "varbinary", "binary"])
        expectEqual(result.rows.count, 2)
        expectEqual(
            result.rows[0].values, [.string("hello"), .string("abc"), .bytes([0xDE, 0xAD]), .bytes([0xBE, 0xEF])])
        expectEqual(result.rows[1].values, [.null, .string("xyz"), .null, .bytes([0x12, 0x34])])
        expectEqual(result.rows[0]["varbinary"], .bytes([0xDE, 0xAD]))
    }

    @Test func queryResultIncludesLegacyLOBValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT legacy LOB values", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.legacyLOBTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["body", "unicodeBody", "picture"])
        expectEqual(result.rows.count, 2)
        expectEqual(result.rows[0].values, [.string("hello text"), .string("wide text"), .bytes([0xCA, 0xFE])])
        expectEqual(result.rows[1].values, [.null, .null, .null])
        expectEqual(result.rows[0]["unicodeBody"], .string("wide text"))
    }

    @Test func queryResultIncludesDecimalValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST(123.45 AS decimal(10,2))", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.decimalTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["amount"])
        expectEqual(result.columns[0].metadata.length, 5)
        expectEqual(result.columns[0].metadata.precision, 10)
        expectEqual(result.columns[0].metadata.scale, 2)
        expectEqual(result.rows.map(\.values), [[.decimal("123.45")], [.decimal("-1.23")]])
        expectEqual(result.rows[0]["amount"], .decimal("123.45"))
    }

    @Test func queryResultIncludesGUIDValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(
            TDSTask.sqlBatch("SELECT CAST('00112233-4455-6677-8899-aabbccddeeff' AS uniqueidentifier)", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.guidTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["id"])
        expectEqual(result.rows.map(\.values), [[.guid(Self.guid)], [.null]])
        expectEqual(result.rows[0]["id"], .guid(Self.guid))
    }

    @Test func queryResultIncludesMultipleResultSets() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1; SELECT N'two'", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.multiResultSetTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["id"])
        expectEqual(result.rows.map(\.values), [[.int32(1)]])
        expectEqual(result.resultSets.count, 2)
        expectEqual(result.resultSets[0].columns.map(\.name), ["id"])
        expectEqual(result.resultSets[0].rows.map(\.values), [[.int32(1)]])
        expectEqual(result.resultSets[0].rowsAffected, 1)
        expectEqual(result.resultSets[1].columns.map(\.name), ["label"])
        expectEqual(result.resultSets[1].rows.map(\.values), [[.string("two")]])
        expectEqual(result.resultSets[1].rowsAffected, 1)
    }

    @Test func doneInProcDoesNotCompleteActiveQuery() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        let completed = NIOLockedValueBox(false)
        queryPromise.futureResult.whenComplete { _ in
            completed.withLockedValue { $0 = true }
        }

        try channel.writeOutbound(TDSTask.rpc(.init(procedure: "dbo.two_results"), queryPromise))
        let rpc: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(rpc.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.doneInProcFirstResultSetPayload()
            ))

        expectFalse(completed.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectTrue(completed.withLockedValue { $0 })
        expectEqual(result.resultSets.count, 2)
        expectEqual(result.resultSets[0].rows.map(\.values), [[.int32(1), .string("one")]])
        expectEqual(result.resultSets[0].rowsAffected, 1)
        expectEqual(result.resultSets[1].rows.map(\.values), [[.int32(1), .string("one")]])
    }

    @Test func startupPipelineCapturesRoutingEnvChange() throws {
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
        expectEqual(context.routing?.protocolByte, 0)
        expectEqual(context.routing?.port, 1444)
        expectEqual(context.routing?.server, "redirect.sql.example.test")
    }

    @Test func loginRoutingSkipsInitialSQLBeforeRedirect() throws {
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
        expectEqual(context.routing?.server, "redirect.sql.example.test")
        expectNil(try channel.readOutbound(as: ByteBuffer.self))
    }

    @Test func loginSendsInitialSQLBeforeStartupDone() throws {
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

        var initialSQL = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(initialSQL.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        initialSQL.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        expectEqual(
            try requireUnwrap(initialSQL.readUTF16(characterCount: initialSQL.readableBytes / 2)),
            "set ansi_nulls on"
        )
        expectFalse(startupDone.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))
        _ = try eventHandler.startupDoneFuture.wait()
        expectTrue(startupDone.withLockedValue { $0 })
    }

    @Test func loginSendsInitialSessionSettingsBeforeStartupDone() throws {
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

        var initialSQL = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(initialSQL.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        initialSQL.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        expectEqual(
            try requireUnwrap(initialSQL.readUTF16(characterCount: initialSQL.readableBytes / 2)),
            """
            set ansi_nulls on
            set textsize 1024
            set transaction isolation level snapshot
            """
        )
        expectFalse(startupDone.withLockedValue { $0 })

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))
        _ = try eventHandler.startupDoneFuture.wait()
        expectTrue(startupDone.withLockedValue { $0 })
    }

    @Test func initialSQLErrorInvokesHandlerAndFailsStartupOnDone() throws {
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
        _ = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))

        var payload = Self.errorPayload(message: "Invalid language", number: 50000)
        var done = Self.donePayload(status: .error)
        payload.writeBuffer(&done)
        expectThrowsError(
            try channel.writeInbound(
                Self.packet(
                    type: .preloginLoginOrTablularResponse,
                    payload: payload
                ))
        ) { error in
            guard let sqlError = error as? TDSSQLError else {
                Issue.record("Expected TDSSQLError, got \(error)")
                return
            }
            expectEqual(sqlError.serverInfo?.message, "Invalid language")
        }
        expectEqual(errors.withLockedValue { $0.map(\.message) }, ["Invalid language"])
    }
}
