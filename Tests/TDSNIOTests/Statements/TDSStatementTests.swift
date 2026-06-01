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
    @Test func queryDescriptionIncludesBoundParameters() throws {
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(42)"

        expectEqual(
            query.description,
            #"SELECT * FROM dbo.items WHERE id = @p0 [TDSNIO.TDSRPC.Parameter(name: "@p0", value: TDSNIO.TDSData.int(42), isOutput: false)]"#
        )
        expectEqual(
            query.debugDescription,
            #"TDSQuery(sql: SELECT * FROM dbo.items WHERE id = @p0, binds: TDSBindings(parameters: [TDSNIO.TDSRPC.Parameter(name: "@p0", value: TDSNIO.TDSData.int(42), isOutput: false)]))"#
        )
    }

    @Test func transactionManagerRequestEncodesBeginTransaction() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 64)
        )
        encoder.transactionManagerRequest(
            .begin(
                isolationLevel: .snapshot,
                name: Array("txn".utf8)
            ))

        var packet = encoder.flush()
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.transactionManagerRequest.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)

        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 22)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 18)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0x02)
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 1)

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 5)
        expectEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.snapshot.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readBytes(length: 3), Array("txn".utf8))
        expectEqual(packet.readableBytes, 0)
    }

    @Test func transactionManagerRequestStringNamesEncodeAsUTF16LittleEndian() throws {
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

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 5)
        expectEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.snapshot.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 6)
        expectEqual(packet.readBytes(length: 6), [0x74, 0x00, 0x78, 0x00, 0x6E, 0x00])
        expectEqual(packet.readableBytes, 0)
    }

    @Test func transactionManagerRequestEncodesCommitWithChainedBegin() throws {
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

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 7)
        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readBytes(length: 5), Array("outer".utf8))
        expectEqual(packet.readInteger(as: UInt8.self), 0x01)
        expectEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.readCommitted.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readBytes(length: 4), Array("next".utf8))
        expectEqual(packet.readableBytes, 0)
    }

    @Test func transactionManagerRequestStringCommitEncodesChainedBeginAsUTF16LittleEndian() throws {
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

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 7)
        expectEqual(packet.readInteger(as: UInt8.self), 10)
        expectEqual(
            packet.readBytes(length: 10),
            [
                0x6F, 0x00, 0x75, 0x00, 0x74, 0x00, 0x65, 0x00, 0x72, 0x00,
            ])
        expectEqual(packet.readInteger(as: UInt8.self), 0x01)
        expectEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.readCommitted.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readBytes(length: 8), [0x6E, 0x00, 0x65, 0x00, 0x78, 0x00, 0x74, 0x00])
        expectEqual(packet.readableBytes, 0)
    }

    @Test func transactionManagerRequestBoundsByteLengthNames() throws {
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

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 8)
        expectEqual(packet.readInteger(as: UInt8.self), UInt8.max)
        expectEqual(packet.readBytes(length: Int(UInt8.max)), Array(repeating: UInt8(0xA5), count: Int(UInt8.max)))
        expectEqual(packet.readInteger(as: UInt8.self), 0x01)
        expectEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.serializable.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), UInt8.max)
        expectEqual(packet.readBytes(length: Int(UInt8.max)), Array(repeating: UInt8(0xA5), count: Int(UInt8.max)))
        expectEqual(packet.readableBytes, 0)
    }

    @Test func transactionManagerRequestBoundsUShortLengthPayloads() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 70_000)
        )
        let payload = Array(repeating: UInt8(0x7B), count: Int(UInt16.max) + 10)
        encoder.transactionManagerRequest(.propagateDTCTransaction(payload))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22)

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        expectEqual(packet.readBytes(length: Int(UInt16.max)), Array(repeating: UInt8(0x7B), count: Int(UInt16.max)))
        expectEqual(packet.readableBytes, 0)
    }

    @Test func transactionManagerTaskEncodesChainedCommit() throws {
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

        var packet: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.transactionManagerRequest.rawValue)
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 7)
        expectEqual(packet.readInteger(as: UInt8.self), 7)
        expectEqual(packet.readBytes(length: 7), Array("current".utf8))
        expectEqual(packet.readInteger(as: UInt8.self), 1)
        expectEqual(
            packet.readInteger(as: UInt8.self), TDSTransactionManagerRequest.IsolationLevel.readCommitted.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readBytes(length: 4), Array("next".utf8))
        expectEqual(packet.readableBytes, 0)
    }

    @Test func pingTaskSendsSelectOneAndCompletesOnDone() throws {
        let channel = try Self.loggedInChannel()

        let promise = channel.eventLoop.makePromise(of: Void.self)
        try channel.writeOutbound(TDSTask.ping(promise))

        var packet: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        expectEqual(try requireUnwrap(packet.readUTF16(characterCount: packet.readableBytes / 2)), "SELECT 1")

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))

        expectNoThrow(try promise.futureResult.wait())
    }

    @Test func pingTaskFailsOnServerErrorAndKeepsQueueUntilDone() throws {
        let channel = try Self.loggedInChannel()

        let pingPromise = channel.eventLoop.makePromise(of: Void.self)
        try channel.writeOutbound(TDSTask.ping(pingPromise))
        _ = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.errorPayload(message: "Ping failed")
            ))

        expectThrowsError(try pingPromise.futureResult.wait()) { error in
            expectEqual((error as? TDSSQLError)?.serverInfo?.message, "Ping failed")
        }

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 2", queryPromise))
        expectNil(try channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .error)
            ))

        let packet = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
    }

    @Test func transactionDescriptorEnvChangeIsSentOnLaterRequests() throws {
        let channel = try Self.loggedInChannel()
        let descriptor: [UInt8] = [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.transactionDescriptorEnvChangePayload(descriptor)
            ))

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", promise))

        let packet: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(packet.getBytes(at: TDSPacket.headerLength + 10, length: 8), descriptor)
        expectEqual(
            packet.getInteger(at: TDSPacket.headerLength + 18, endianness: .little, as: UInt32.self),
            1
        )
    }

    @Test func transactionDescriptorEnvChangeClearsOnCommit() throws {
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

        let packet: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(
            packet.getInteger(at: TDSPacket.headerLength + 10, endianness: .little, as: UInt64.self),
            0
        )
        expectEqual(
            packet.getInteger(at: TDSPacket.headerLength + 18, endianness: .little, as: UInt32.self),
            1
        )
    }

    @Test func transactionDescriptorEnvChangeClearsOnRollback() throws {
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

        let packet: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(
            packet.getInteger(at: TDSPacket.headerLength + 10, endianness: .little, as: UInt64.self),
            0
        )
    }

    @Test func txnEndedEnvChangeDoesNotReplaceTransactionDescriptor() throws {
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

        let packet: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(packet.getBytes(at: TDSPacket.headerLength + 10, length: 8), descriptor)
        let changes = envChanges.withLockedValue { $0 }
        expectEqual(changes.map(\.type), [8, 17])
        expectEqual(changes[1].value, .bytes(new: endedDescriptor, old: descriptor))
    }

    @Test func attentionCancelsInFlightRequestAfterServerDone() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("WAITFOR DELAY '00:00:30'", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

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
        let attention: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(attention.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.attentionSignal.rawValue)
        expectEqual(attention.getInteger(at: 2, endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .attention)
            ))

        let completedQuery = queryResult.withLockedValue { $0 }
        guard case .failure(let error) = completedQuery else {
            Issue.record("Expected cancelled query promise, got \(String(describing: completedQuery))")
            return
        }
        do {
            throw error
        } catch {
            guard let sqlError = error as? TDSSQLError else {
                Issue.record("Expected TDSSQLError, got \(error)")
                return
            }
            expectEqual(sqlError.code, .requestCancelled)
        }
        let completedCancel = cancelResult.withLockedValue { $0 }
        guard case .success = completedCancel else {
            Issue.record("Expected successful cancellation promise, got \(String(describing: completedCancel))")
            return
        }
    }

    @Test func attentionDrainsNonAckTokensWithoutApplyingSideEffects() throws {
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
        _ = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))

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
        _ = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))

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

        expectEqual(errors.withLockedValue { $0.count }, 0)
        expectEqual(infos.withLockedValue { $0.count }, 0)
        expectEqual(envChanges.withLockedValue { $0.count }, 0)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .attention)
            ))

        let completedQuery = queryResult.withLockedValue { $0 }
        guard case .failure(let error) = completedQuery else {
            Issue.record("Expected cancelled query promise, got \(String(describing: completedQuery))")
            return
        }
        expectEqual((error as? TDSSQLError)?.code, .requestCancelled)
        let completedCancel = cancelResult.withLockedValue { $0 }
        guard case .success = completedCancel else {
            Issue.record("Expected successful cancellation promise, got \(String(describing: completedCancel))")
            return
        }
    }
}
