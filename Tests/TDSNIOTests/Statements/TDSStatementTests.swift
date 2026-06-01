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
    func testQueryDescriptionMatchesOracleStyleAPI() throws {
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(42)"

        XCTAssertEqual(
            query.description,
            #"SELECT * FROM dbo.items WHERE id = @p0 [TDSNIO.TDSRPC.Parameter(name: "@p0", value: TDSNIO.TDSData.int(42), isOutput: false)]"#
        )
        XCTAssertEqual(
            query.debugDescription,
            #"TDSQuery(sql: SELECT * FROM dbo.items WHERE id = @p0, binds: TDSBindings(parameters: [TDSNIO.TDSRPC.Parameter(name: "@p0", value: TDSNIO.TDSData.int(42), isOutput: false)]))"#
        )
    }

    func testTransactionManagerRequestEncodesBeginTransaction() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 64)
        )
        encoder.transactionManagerRequest(
            .begin(
                isolationLevel: .snapshot,
                name: Array("txn".utf8)
            ))

        var packet = encoder.flush()
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.transactionManagerRequest.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 22)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 18)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0x02)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt64.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 1)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 5)
        XCTAssertEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.snapshot.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readBytes(length: 3), Array("txn".utf8))
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testTransactionManagerRequestStringNamesEncodeAsUTF16LittleEndian() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 64)
        )
        encoder.transactionManagerRequest(
            .begin(
                isolationLevel: .snapshot,
                name: "txn"
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 5)
        XCTAssertEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.snapshot.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 6)
        XCTAssertEqual(packet.readBytes(length: 6), [0x74, 0x00, 0x78, 0x00, 0x6E, 0x00])
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testTransactionManagerRequestEncodesCommitWithChainedBegin() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 64)
        )
        encoder.transactionManagerRequest(
            .commit(
                name: Array("outer".utf8),
                beginAfterwards: (isolationLevel: .readCommitted, name: Array("next".utf8))
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 7)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readBytes(length: 5), Array("outer".utf8))
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x01)
        XCTAssertEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.readCommitted.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readBytes(length: 4), Array("next".utf8))
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testTransactionManagerRequestStringCommitEncodesChainedBeginAsUTF16LittleEndian() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 64)
        )
        encoder.transactionManagerRequest(
            .commit(
                name: "outer",
                beginAfterwards: (isolationLevel: .readCommitted, name: "next")
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 7)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 10)
        XCTAssertEqual(packet.readBytes(length: 10), [
            0x6F, 0x00, 0x75, 0x00, 0x74, 0x00, 0x65, 0x00, 0x72, 0x00,
        ])
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x01)
        XCTAssertEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.readCommitted.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readBytes(length: 8), [0x6E, 0x00, 0x65, 0x00, 0x78, 0x00, 0x74, 0x00])
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testTransactionManagerRequestBoundsByteLengthNames() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        let longName = Array(repeating: UInt8(0xA5), count: 300)
        encoder.transactionManagerRequest(
            .rollback(
                name: longName,
                beginAfterwards: (isolationLevel: .serializable, name: longName)
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 8)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), UInt8.max)
        XCTAssertEqual(packet.readBytes(length: Int(UInt8.max)), Array(repeating: UInt8(0xA5), count: Int(UInt8.max)))
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x01)
        XCTAssertEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.serializable.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), UInt8.max)
        XCTAssertEqual(packet.readBytes(length: Int(UInt8.max)), Array(repeating: UInt8(0xA5), count: Int(UInt8.max)))
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testTransactionManagerRequestBoundsUShortLengthPayloads() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 70_000)
        )
        let payload = Array(repeating: UInt8(0x7B), count: Int(UInt16.max) + 10)
        encoder.transactionManagerRequest(.propagateDTCTransaction(payload))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 1)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        XCTAssertEqual(packet.readBytes(length: Int(UInt16.max)), Array(repeating: UInt8(0x7B), count: Int(UInt16.max)))
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testTransactionManagerTaskEncodesChainedCommit() throws {
        let channel = try Self.loggedInChannel()

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(
            TDSTask.transactionManager(
                .commit(
                    name: Array("current".utf8),
                    beginAfterwards: (isolationLevel: .readCommitted, name: Array("next".utf8))
                ),
                promise
            ))

        var packet: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.transactionManagerRequest.rawValue)
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 7)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 7)
        XCTAssertEqual(packet.readBytes(length: 7), Array("current".utf8))
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 1)
        XCTAssertEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.readCommitted.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readBytes(length: 4), Array("next".utf8))
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testPingTaskSendsSelectOneAndCompletesOnDone() throws {
        let channel = try Self.loggedInChannel()

        let promise = channel.eventLoop.makePromise(of: Void.self)
        try channel.writeOutbound(TDSTask.ping(promise))

        var packet: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        XCTAssertEqual(try XCTUnwrap(packet.readUTF16(characterCount: packet.readableBytes / 2)), "SELECT 1")

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))

        XCTAssertNoThrow(try promise.futureResult.wait())
    }

    func testPingTaskFailsOnServerErrorAndKeepsQueueUntilDone() throws {
        let channel = try Self.loggedInChannel()

        let pingPromise = channel.eventLoop.makePromise(of: Void.self)
        try channel.writeOutbound(TDSTask.ping(pingPromise))
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.errorPayload(message: "Ping failed")
            ))

        XCTAssertThrowsError(try pingPromise.futureResult.wait()) { error in
            XCTAssertEqual((error as? TDSSQLError)?.serverInfo?.message, "Ping failed")
        }

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 2", queryPromise))
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        let packet = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
    }

    func testTransactionDescriptorEnvChangeIsSentOnLaterRequests() throws {
        let channel = try Self.loggedInChannel()
        let descriptor: [UInt8] = [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.transactionDescriptorEnvChangePayload(descriptor)
            ))

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", promise))

        let packet: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(packet.getBytes(at: TDSPacket.headerLength + 10, length: 8), descriptor)
        XCTAssertEqual(
            packet.getInteger(at: TDSPacket.headerLength + 18, endianness: .little, as: UInt32.self),
            1
        )
    }

    func testTransactionDescriptorEnvChangeClearsOnCommit() throws {
        let channel = try Self.loggedInChannel()
        let descriptor: [UInt8] = [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.transactionDescriptorEnvChangePayload(descriptor)
            ))
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.transactionDescriptorEnvChangePayload([], old: descriptor, type: 9)
            ))

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", promise))

        let packet: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(
            packet.getInteger(at: TDSPacket.headerLength + 10, endianness: .little, as: UInt64.self),
            0
        )
        XCTAssertEqual(
            packet.getInteger(at: TDSPacket.headerLength + 18, endianness: .little, as: UInt32.self),
            1
        )
    }

    func testTransactionDescriptorEnvChangeClearsOnRollback() throws {
        let channel = try Self.loggedInChannel()
        let descriptor: [UInt8] = [0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11]

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.transactionDescriptorEnvChangePayload(descriptor)
            ))
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.transactionDescriptorEnvChangePayload([], old: descriptor, type: 10)
            ))

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", promise))

        let packet: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(
            packet.getInteger(at: TDSPacket.headerLength + 10, endianness: .little, as: UInt64.self),
            0
        )
    }

    func testTxnEndedEnvChangeDoesNotReplaceTransactionDescriptor() throws {
        let envChanges = NIOLockedValueBox<[TDSEnvChangeMessage]>([])
        var configuration = Self.configuration()
        configuration.options.envChangeHandler = { message in
            envChanges.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)
        let descriptor: [UInt8] = [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
        let endedDescriptor: [UInt8] = [0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11]

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.transactionDescriptorEnvChangePayload(descriptor)
            ))
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.transactionDescriptorEnvChangePayload(endedDescriptor, old: descriptor, type: 17)
            ))

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", promise))

        let packet: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(packet.getBytes(at: TDSPacket.headerLength + 10, length: 8), descriptor)
        let changes = envChanges.withLockedValue { $0 }
        XCTAssertEqual(changes.map(\.type), [8, 17])
        XCTAssertEqual(changes[1].value, .bytes(new: endedDescriptor, old: descriptor))
    }

    func testAttentionCancelsInFlightRequestAfterServerDone() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("WAITFOR DELAY '00:00:30'", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        let cancelPromise = channel.eventLoop.makePromise(of: Void.self)
        let queryResult = NIOLockedValueBox<Result<TDSQueryResult, Error>?>(nil)
        queryPromise.futureResult.whenComplete { result in
            queryResult.withLockedValue { $0 = result }
        }
        let cancelResult = NIOLockedValueBox<Result<Void, Error>?>(nil)
        cancelPromise.futureResult.whenComplete { result in
            cancelResult.withLockedValue { $0 = result }
        }
        try channel.writeOutbound(TDSTask.attention(cancelPromise))
        let attention: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(attention.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.attentionSignal.rawValue)
        XCTAssertEqual(attention.getInteger(at: 2, endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .attention)
            ))

        let completedQuery = queryResult.withLockedValue { $0 }
        guard case .failure(let error) = completedQuery else {
            return XCTFail("Expected cancelled query promise, got \(String(describing: completedQuery))")
        }
        do {
            throw error
        } catch {
            guard let sqlError = error as? TDSSQLError else {
                return XCTFail("Expected TDSSQLError, got \(error)")
            }
            XCTAssertEqual(sqlError.code, .requestCancelled)
        }
        let completedCancel = cancelResult.withLockedValue { $0 }
        guard case .success = completedCancel else {
            return XCTFail("Expected successful cancellation promise, got \(String(describing: completedCancel))")
        }
    }

    func testAttentionDrainsNonAckTokensWithoutApplyingSideEffects() throws {
        let errors = NIOLockedValueBox<[TDSErrorMessage]>([])
        let infos = NIOLockedValueBox<[TDSInfoMessage]>([])
        let envChanges = NIOLockedValueBox<[TDSEnvChangeMessage]>([])
        var configuration = Self.configuration()
        configuration.options.errorMessageHandler = { message in
            errors.withLockedValue { $0.append(message) }
        }
        configuration.options.infoMessageHandler = { message in
            infos.withLockedValue { $0.append(message) }
        }
        configuration.options.envChangeHandler = { message in
            envChanges.withLockedValue { $0.append(message) }
        }
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("WAITFOR DELAY '00:00:30'", queryPromise))
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))

        let cancelPromise = channel.eventLoop.makePromise(of: Void.self)
        let queryResult = NIOLockedValueBox<Result<TDSQueryResult, Error>?>(nil)
        queryPromise.futureResult.whenComplete { result in
            queryResult.withLockedValue { $0 = result }
        }
        let cancelResult = NIOLockedValueBox<Result<Void, Error>?>(nil)
        cancelPromise.futureResult.whenComplete { result in
            cancelResult.withLockedValue { $0 = result }
        }
        try channel.writeOutbound(TDSTask.attention(cancelPromise))
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))

        var drainPayload = Self.errorPayload(message: "ignored during attention")
        var info = Self.infoPayload(message: "also ignored")
        drainPayload.writeBuffer(&info)
        var envChange = Self.stringEnvChangePayload(type: 1, new: "tempdb", old: "master")
        drainPayload.writeBuffer(&envChange)
        var nonAckDone = Self.donePayload()
        drainPayload.writeBuffer(&nonAckDone)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: drainPayload
            ))

        XCTAssertEqual(errors.withLockedValue { $0.count }, 0)
        XCTAssertEqual(infos.withLockedValue { $0.count }, 0)
        XCTAssertEqual(envChanges.withLockedValue { $0.count }, 0)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .attention)
            ))

        let completedQuery = queryResult.withLockedValue { $0 }
        guard case .failure(let error) = completedQuery else {
            return XCTFail("Expected cancelled query promise, got \(String(describing: completedQuery))")
        }
        XCTAssertEqual((error as? TDSSQLError)?.code, .requestCancelled)
        let completedCancel = cancelResult.withLockedValue { $0 }
        guard case .success = completedCancel else {
            return XCTFail("Expected successful cancellation promise, got \(String(describing: completedCancel))")
        }
    }
}
