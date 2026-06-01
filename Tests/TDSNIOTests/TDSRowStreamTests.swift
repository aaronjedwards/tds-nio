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
    @Test func rowStreamConsumptionModes() throws {
        let channel = EmbeddedChannel()
        let columns = [
            TDSColumn(name: "id", dataType: .intN),
            TDSColumn(name: "label", dataType: .nVarChar),
        ]
        let rows = [
            TDSRow(columns: columns, values: [.int(1), .string("one")]),
            TDSRow(columns: columns, values: [.int(2), .string("two")]),
        ]

        let allRows = try TDSRowStream(rows: rows, eventLoop: channel.eventLoop).all().wait()
        expectEqual(allRows, rows)

        let seen = NIOLockedValueBox<[TDSData]>([])
        try TDSRowStream(rows: rows, eventLoop: channel.eventLoop).onRow { row in
            if let label = row["label"] {
                seen.withLockedValue {
                    $0.append(label)
                }
            }
        }.wait()
        expectEqual(seen.withLockedValue { $0 }, [.string("one"), .string("two")])
    }

    @Test func rowStreamAsyncSequenceCollectsRows() async throws {
        let channel = EmbeddedChannel()
        let columns = [
            TDSColumn(name: "id", dataType: .intN),
            TDSColumn(name: "label", dataType: .nVarChar),
        ]
        let rows = [
            TDSRow(columns: columns, values: [.int(1), .string("one")]),
            TDSRow(columns: columns, values: [.int(2), .string("two")]),
        ]

        let collected = try await TDSRowStream(rows: rows, eventLoop: channel.eventLoop)
            .asyncSequence()
            .collect()
        expectEqual(collected, rows)
    }

    @Test func channelRowStreamPromiseFailsWhenErrorArrivesBeforeMetadata() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT broken", streamPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.errorPayload(message: "Invalid object name")
            ))

        expectThrowsError(try streamPromise.futureResult.wait()) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .server)
            expectEqual(sqlError?.serverInfo?.number, 208)
            expectEqual(sqlError?.serverInfo?.state, 1)
            expectEqual(sqlError?.serverInfo?.severity, 16)
            expectEqual(sqlError?.serverInfo?.message, "Invalid object name")
            expectEqual(sqlError?.serverInfo?.lineNumber, 1)
            expectEqual(sqlError?.query?.sql, "SELECT broken")
        }
    }

    @Test func channelRowStreamFailsConsumerWhenErrorArrivesAfterMetadata() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT partially_broken", streamPromise))
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
                payload: Self.errorPayload(message: "Arithmetic overflow")
            ))

        expectThrowsError(try rowsFuture.wait()) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .server)
            expectEqual(sqlError?.serverInfo?.number, 208)
            expectEqual(sqlError?.serverInfo?.message, "Arithmetic overflow")
            expectEqual(sqlError?.query?.sql, "SELECT partially_broken")
        }
    }
}
