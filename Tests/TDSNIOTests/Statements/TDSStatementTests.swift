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
        encoder.transactionManagerRequest(.begin(
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
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.snapshot.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readBytes(length: 3), Array("txn".utf8))
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testTransactionManagerRequestEncodesCommitWithChainedBegin() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 64)
        )
        encoder.transactionManagerRequest(.commit(
            name: Array("outer".utf8),
            beginAfterwards: (isolationLevel: .readCommitted, name: Array("next".utf8))
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 7)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readBytes(length: 5), Array("outer".utf8))
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x01)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.readCommitted.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readBytes(length: 4), Array("next".utf8))
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testTransactionManagerRequestBoundsByteLengthNames() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        let longName = Array(repeating: UInt8(0xA5), count: 300)
        encoder.transactionManagerRequest(.rollback(
            name: longName,
            beginAfterwards: (isolationLevel: .serializable, name: longName)
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 8)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), UInt8.max)
        XCTAssertEqual(packet.readBytes(length: Int(UInt8.max)), Array(repeating: UInt8(0xA5), count: Int(UInt8.max)))
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x01)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.serializable.rawValue)
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
        try channel.writeOutbound(TDSTask.transactionManager(
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
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.readCommitted.rawValue)
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

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.errorPayload(message: "Ping failed")
        ))

        XCTAssertThrowsError(try pingPromise.futureResult.wait()) { error in
            XCTAssertEqual((error as? TDSSQLError)?.serverInfo?.message, "Ping failed")
        }

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 2", queryPromise))
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.donePayload(status: .error)
        ))

        let packet = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
    }

    func testTransactionDescriptorEnvChangeIsSentOnLaterRequests() throws {
        let channel = try Self.loggedInChannel()
        let descriptor: [UInt8] = [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.transactionDescriptorEnvChangePayload(descriptor)
        ))
        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.transactionDescriptorEnvChangePayload(descriptor)
        ))
        try channel.writeInbound(Self.packet(
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

    func testAttentionCancelsInFlightRequestAfterServerDone() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("WAITFOR DELAY '00:00:30'", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        let cancelPromise = channel.eventLoop.makePromise(of: Void.self)
        try channel.writeOutbound(TDSTask.attention(cancelPromise))
        let attention: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(attention.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.attentionSignal.rawValue)
        XCTAssertEqual(attention.getInteger(at: 2, endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength))

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.donePayload(status: .attention)
        ))

        XCTAssertThrowsError(try queryPromise.futureResult.wait()) { error in
            guard let sqlError = error as? TDSSQLError else {
                return XCTFail("Expected TDSSQLError, got \(error)")
            }
            XCTAssertEqual(sqlError.code, .requestCancelled)
        }
        try cancelPromise.futureResult.wait()
    }
}
