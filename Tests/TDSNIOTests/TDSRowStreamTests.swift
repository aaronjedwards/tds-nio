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
    func testRowStreamConsumptionModes() throws {
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
        XCTAssertEqual(allRows, rows)

        let seen = NIOLockedValueBox<[TDSData]>([])
        try TDSRowStream(rows: rows, eventLoop: channel.eventLoop).onRow { row in
            if let label = row["label"] {
                seen.withLockedValue {
                    $0.append(label)
                }
            }
        }.wait()
        XCTAssertEqual(seen.withLockedValue { $0 }, [.string("one"), .string("two")])
    }

    func testRowStreamAsyncSequenceCollectsRows() async throws {
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
        XCTAssertEqual(collected, rows)
    }

    func testChannelRowStreamPromiseFailsWhenErrorArrivesBeforeMetadata() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT broken", streamPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.errorPayload(message: "Invalid object name")
            ))

        XCTAssertThrowsError(try streamPromise.futureResult.wait()) { error in
            let sqlError = error as? TDSSQLError
            XCTAssertEqual(sqlError?.code, .server)
            XCTAssertEqual(sqlError?.serverInfo?.number, 208)
            XCTAssertEqual(sqlError?.serverInfo?.state, 1)
            XCTAssertEqual(sqlError?.serverInfo?.severity, 16)
            XCTAssertEqual(sqlError?.serverInfo?.message, "Invalid object name")
            XCTAssertEqual(sqlError?.serverInfo?.lineNumber, 1)
            XCTAssertEqual(sqlError?.query?.sql, "SELECT broken")
        }
    }

    func testChannelRowStreamFailsConsumerWhenErrorArrivesAfterMetadata() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT partially_broken", streamPromise))
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
                payload: Self.errorPayload(message: "Arithmetic overflow")
            ))

        XCTAssertThrowsError(try rowsFuture.wait()) { error in
            let sqlError = error as? TDSSQLError
            XCTAssertEqual(sqlError?.code, .server)
            XCTAssertEqual(sqlError?.serverInfo?.number, 208)
            XCTAssertEqual(sqlError?.serverInfo?.message, "Arithmetic overflow")
            XCTAssertEqual(sqlError?.query?.sql, "SELECT partially_broken")
        }
    }
}
