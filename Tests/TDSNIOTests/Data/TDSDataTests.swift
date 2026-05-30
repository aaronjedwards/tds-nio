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
    func testQueryInterpolationBuildsSpExecuteSQLRPC() throws {
        let id = 42
        let label = "forty-two"
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(id) AND label = \(label)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE id = @p0 AND label = @p1")
        XCTAssertEqual(query.binds.count, 2)
        XCTAssertEqual(query.binds.parameters.map(\.name), ["@p0", "@p1"])
        XCTAssertEqual(query.binds.parameters.map(\.value), [.int(42), .string("forty-two")])

        let rpc = query.rpcForExecution()
        XCTAssertEqual(rpc.procedure, "sp_executesql")
        XCTAssertEqual(rpc.parameters.map(\.name), ["@stmt", "@params", "@p0", "@p1"])
        XCTAssertEqual(rpc.parameters[0].value, .string(query.sql))
        XCTAssertEqual(rpc.parameters[1].value, .string("@p0 bigint, @p1 nvarchar(max)"))
    }

    func testQueryInterpolationBindsOptionalNilsAsTypedNulls() throws {
        let id: Int? = nil
        let label: String? = nil
        let flag: Bool? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(id) AND label = \(label) AND flag = \(flag)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE id = @p0 AND label = @p1 AND flag = @p2")
        XCTAssertEqual(
            query.binds.parameters.map(\.value),
            [
                .typedNull(.bigInt),
                .typedNull(.nvarchar()),
                .typedNull(.bit),
            ])
        XCTAssertEqual(query.binds.declarationList, "@p0 bigint, @p1 nvarchar(max), @p2 bit")
    }

    func testQueryInterpolationBindsIntegerWidths() throws {
        let tiny: UInt8 = 7
        let small: Int16 = -12
        let integer: Int32 = 123_456
        let query: TDSQuery = "SELECT \(tiny), \(small), \(integer)"

        XCTAssertEqual(query.sql, "SELECT @p0, @p1, @p2")
        XCTAssertEqual(
            query.binds.parameters.map(\.value),
            [
                .tinyInt(7),
                .smallInt(-12),
                .int32(123_456),
            ])
        XCTAssertEqual(query.binds.declarationList, "@p0 tinyint, @p1 smallint, @p2 int")
    }

    func testQueryInterpolationBindsUUIDAsUniqueIdentifier() throws {
        let uuid = try XCTUnwrap(UUID(uuidString: "00112233-4455-6677-8899-aabbccddeeff"))
        let nilUUID: UUID? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(uuid) OR parent_id = \(nilUUID)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE id = @p0 OR parent_id = @p1")
        XCTAssertEqual(
            query.binds.parameters.map(\.value),
            [
                .guid(TDSGUID(uuid)),
                .typedNull(.uniqueIdentifier),
            ])
        XCTAssertEqual(query.binds.declarationList, "@p0 uniqueidentifier, @p1 uniqueidentifier")
    }

    func testQueryInterpolationBindsDecimalValues() throws {
        let amount = try XCTUnwrap(Decimal(string: "123.45", locale: Locale(identifier: "en_US_POSIX")))
        let nilAmount: Decimal? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE amount = \(amount) OR discount = \(nilAmount)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE amount = @p0 OR discount = @p1")
        XCTAssertEqual(
            query.binds.parameters.map(\.value),
            [
                .decimal("123.45"),
                .typedNull(.decimal()),
            ])
        XCTAssertEqual(query.binds.declarationList, "@p0 decimal(5, 2), @p1 decimal(38, 0)")
    }

    func testQueryInterpolationBindsFoundationDataValues() throws {
        let payload = Data([0x01, 0x02, 0x03])
        let archivedPayload: Data? = nil
        let query: TDSQuery =
            "SELECT * FROM dbo.items WHERE payload = \(payload) OR archived_payload = \(archivedPayload)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE payload = @p0 OR archived_payload = @p1")
        XCTAssertEqual(
            query.binds.parameters.map(\.value),
            [
                .bytes([0x01, 0x02, 0x03]),
                .typedNull(.varbinary()),
            ])
        XCTAssertEqual(query.binds.declarationList, "@p0 varbinary(max), @p1 varbinary(max)")
    }

    func testQueryInterpolationBindsStructuredJSONValues() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let payload = try TDSJSON(JSONPayload(ok: true, count: 2), encoder: encoder)
        let missingPayload: TDSJSON<JSONPayload>? = nil
        let query: TDSQuery =
            "SELECT * FROM dbo.items WHERE payload = \(payload) OR missing_payload = \(missingPayload)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE payload = @p0 OR missing_payload = @p1")
        XCTAssertEqual(
            query.binds.parameters.map(\.value),
            [
                .json(Array(#"{"count":2,"ok":true}"#.utf8)),
                .typedNull(.json),
            ])
        XCTAssertEqual(query.binds.declarationList, "@p0 nvarchar(max), @p1 nvarchar(max)")
    }

    func testQueryInterpolationBindsCustomTDSBindableValues() throws {
        let accountID = AccountID(rawValue: 42)
        let parentID: AccountID? = nil
        let query: TDSQuery = "SELECT * FROM dbo.accounts WHERE id = \(accountID) OR parent_id = \(parentID)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.accounts WHERE id = @p0 OR parent_id = @p1")
        XCTAssertEqual(
            query.binds.parameters.map(\.value),
            [
                .int32(42),
                .typedNull(.int),
            ])
        XCTAssertEqual(query.binds.declarationList, "@p0 int, @p1 int")
    }

    func testQueryInterpolationBindsFoundationDateValues() throws {
        let createdAt = Self.utcDate(year: 2024, month: 2, day: 29, hour: 12, minute: 34, second: 56)
        let deletedAt: Date? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE created_at = \(createdAt) OR deleted_at = \(deletedAt)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE created_at = @p0 OR deleted_at = @p1")
        XCTAssertEqual(
            query.binds.parameters.map(\.value),
            [
                .datetime2(TDSDateTime(createdAt)),
                .typedNull(.datetime2()),
            ])
        XCTAssertEqual(query.binds.declarationList, "@p0 datetime2(7), @p1 datetime2(7)")
    }

    func testTemporalValuesBridgeToFoundationDate() throws {
        let instant = Self.utcDate(
            year: 2024,
            month: 2,
            day: 29,
            hour: 12,
            minute: 34,
            second: 56,
            nanosecond: 123_456_000
        )
        let dateTime = TDSDateTime(instant, scale: 6)
        let offsetDateTime = TDSDateTimeOffset(instant, offsetMinutes: -420, scale: 6)

        XCTAssertEqual(dateTime.date, TDSDate(year: 2024, month: 2, day: 29))
        XCTAssertEqual(dateTime.time, TDSTime(hour: 12, minute: 34, second: 56, nanosecond: 123_456_000, scale: 6))
        XCTAssertEqual(offsetDateTime.dateTime.date, TDSDate(year: 2024, month: 2, day: 29))
        XCTAssertEqual(offsetDateTime.dateTime.time.hour, 5)
        XCTAssertEqual(offsetDateTime.offsetMinutes, -420)
        XCTAssertEqual(
            try XCTUnwrap(dateTime.dateValue()).timeIntervalSince1970, instant.timeIntervalSince1970,
            accuracy: 0.000_001)
        XCTAssertEqual(
            try XCTUnwrap(offsetDateTime.dateValue()).timeIntervalSince1970, instant.timeIntervalSince1970,
            accuracy: 0.000_001)

        let decoded: Date = try Date.decode(from: .datetimeOffset(offsetDateTime))
        XCTAssertEqual(decoded.timeIntervalSince1970, instant.timeIntervalSince1970, accuracy: 0.000_001)
    }

    func testQueryInterpolationDeclaresTVPAsReadonly() throws {
        let tvp = TDSTableValuedParameter(
            schemaName: "dbo",
            typeName: "IntList",
            columns: [.init(dataType: .int(maxBytes: 4))],
            rows: [[.int(1)], [.int(2)]]
        )
        var query = TDSQuery(unsafeSQL: "SELECT * FROM @ids")
        _ = query.binds.append(.table(tvp), name: "@ids")

        XCTAssertEqual(query.binds.declarationList, "@ids dbo.IntList READONLY")
        let rpc = query.rpcForExecution()
        XCTAssertEqual(rpc.parameters[1].value, .string("@ids dbo.IntList READONLY"))
    }

    func testRowSequenceCollectsRowsFromResultSet() async throws {
        let columns = [
            TDSColumn(name: "id", dataType: .intN),
            TDSColumn(name: "label", dataType: .nVarChar),
        ]
        let rows = [
            TDSRow(columns: columns, values: [.int(1), .string("one")]),
            TDSRow(columns: columns, values: [.int(2), .string("two")]),
        ]
        let resultSet = TDSResultSet(columns: columns, rows: rows, rowsAffected: 2)

        var iterator = resultSet.rowSequence.makeAsyncIterator()
        let first = try await iterator.next()
        let collected = try await resultSet.rowSequence.collect()

        XCTAssertEqual(first?["label"], .string("one"))
        XCTAssertEqual(Array(first ?? TDSRow(columns: [], values: [])), [.int(1), .string("one")])
        XCTAssertEqual(collected, rows)
    }

    func testRowsDecodeIntoModelTypes() async throws {
        let columns = [
            TDSColumn(name: "id", dataType: .intN),
            TDSColumn(name: "label", dataType: .nVarChar),
            TDSColumn(name: "payload", dataType: .json),
        ]
        let rows = [
            TDSRow(
                columns: columns,
                values: [
                    .int32(1),
                    .string("one"),
                    .json(Array(#"{"ok":true}"#.utf8)),
                ]),
            TDSRow(
                columns: columns,
                values: [
                    .int32(2),
                    .string("two"),
                    .json(Array(#"{"ok":false,"count":3}"#.utf8)),
                ]),
        ]
        let result = TDSQueryResult(
            columns: columns,
            rows: rows,
            rowsAffected: nil,
            returnStatus: nil,
            outputParameters: [],
            resultSets: [.init(columns: columns, rows: rows, rowsAffected: nil)]
        )

        XCTAssertEqual(
            try result.decodeRows(as: ItemRow.self),
            [
                ItemRow(id: 1, label: "one", payload: .init(ok: true, count: nil)),
                ItemRow(id: 2, label: "two", payload: .init(ok: false, count: 3)),
            ])
        XCTAssertEqual(
            try result.resultSets[0].decodeRows(as: ItemRow.self),
            [
                ItemRow(id: 1, label: "one", payload: .init(ok: true, count: nil)),
                ItemRow(id: 2, label: "two", payload: .init(ok: false, count: 3)),
            ])
        let collected = try await result.rowSequence.collect(as: ItemRow.self)
        XCTAssertEqual(
            collected,
            [
                ItemRow(id: 1, label: "one", payload: .init(ok: true, count: nil)),
                ItemRow(id: 2, label: "two", payload: .init(ok: false, count: 3)),
            ])
    }

    func testRowModelDecodingPreservesColumnContext() throws {
        let row = TDSRow(
            columns: [
                .init(name: "id", dataType: .nVarChar),
                .init(name: "label", dataType: .nVarChar),
                .init(name: "payload", dataType: .json),
            ],
            values: [
                .string("not an integer"),
                .string("bad"),
                .json(Array(#"{"ok":true}"#.utf8)),
            ]
        )

        XCTAssertThrowsError(try row.decode(as: ItemRow.self)) { error in
            guard let decodingError = error as? TDSDecodingError else {
                return XCTFail("Expected TDSDecodingError, got \(error)")
            }
            XCTAssertEqual(decodingError.code, .typeMismatch(expected: "Int", actual: .string("not an integer")))
            XCTAssertEqual(decodingError.columnName, "id")
            XCTAssertEqual(decodingError.columnIndex, 0)
            XCTAssertEqual(decodingError.dataType, .nVarChar)
        }
    }

    func testRowCellsAndTypedDecoding() throws {
        let row = TDSRow(
            columns: [
                .init(name: "id", dataType: .intN),
                .init(name: "label", dataType: .nVarChar),
                .init(name: "payload", dataType: .json),
                .init(name: "guid", dataType: .guid),
                .init(name: "amount", dataType: .decimalN),
                .init(name: "maybe", dataType: .null),
            ],
            values: [
                .int(42),
                .string("forty-two"),
                .json(Array(#"{"ok":true}"#.utf8)),
                .guid(Self.guid),
                .decimal("123.45"),
                .null,
            ]
        )

        XCTAssertTrue(row.contains("label"))
        XCTAssertEqual(row.firstIndex(ofColumn: "payload"), 2)
        XCTAssertEqual(row.cell(named: "label")?.columnIndex, 1)
        XCTAssertEqual(row.cell(named: "label")?.dataType, .nVarChar)

        let id: Int = try row.decode(column: "id")
        let label: String = try row.decode(column: "label")
        let payload: [UInt8] = try row.decode(column: 2)
        let payloadData: Data = try row.decode(column: "payload")
        let jsonPayload: TDSJSONValue<JSONPayload> = try row.decode(column: "payload")
        let uuid: UUID = try row.decode(column: "guid")
        let amount: Decimal = try row.decode(column: "amount")
        let maybe: String? = try row.decode(column: "maybe")
        let typedNil = try Optional<String>.decode(from: .typedNull(.nvarchar()))
        let accountID = try AccountID.decode(from: .int32(42))

        XCTAssertEqual(id, 42)
        XCTAssertEqual(label, "forty-two")
        XCTAssertEqual(payload, Array(#"{"ok":true}"#.utf8))
        XCTAssertEqual(payloadData, Data(#"{"ok":true}"#.utf8))
        XCTAssertEqual(jsonPayload.value, JSONPayload(ok: true, count: nil))
        XCTAssertEqual(uuid.uuidString.lowercased(), Self.guid.stringValue)
        XCTAssertEqual(amount, Decimal(string: "123.45", locale: Locale(identifier: "en_US_POSIX")))
        XCTAssertEqual(accountID, AccountID(rawValue: 42))
        XCTAssertNil(maybe)
        XCTAssertNil(typedNil)
    }

    func testRowCanCreateOracleStyleRandomAccessView() throws {
        let row = TDSRow(
            columns: [
                .init(name: "id", dataType: .intN),
                .init(name: "label", dataType: .nVarChar),
                .init(name: "payload", dataType: .json),
            ],
            values: [
                .int32(42),
                .string("forty-two"),
                .json(Array(#"{"ok":true}"#.utf8)),
            ]
        )

        let randomAccess = row.makeRandomAccess()

        XCTAssertEqual(randomAccess.count, 3)
        XCTAssertTrue(randomAccess.contains("label"))
        XCTAssertFalse(randomAccess.contains("missing"))
        XCTAssertEqual(randomAccess[0].columnName, "id")
        XCTAssertEqual(randomAccess[1].value, .string("forty-two"))
        XCTAssertEqual(randomAccess["payload"].dataType, .json)

        let id: Int32 = try randomAccess.decode(column: "id")
        let label: String = try randomAccess.decode(column: 1)
        let payload: TDSJSONValue<JSONPayload> = try randomAccess.decode(column: "payload")

        XCTAssertEqual(id, 42)
        XCTAssertEqual(label, "forty-two")
        XCTAssertEqual(payload.value, JSONPayload(ok: true, count: nil))
    }

    func testWidthSpecificIntegerTypedDecoding() throws {
        let row = TDSRow(
            columns: [
                .init(name: "tiny", dataType: .intN),
                .init(name: "small", dataType: .intN),
                .init(name: "integer", dataType: .intN),
            ],
            values: [
                .tinyInt(255),
                .smallInt(-12),
                .int32(123_456),
            ]
        )

        let tiny: UInt8 = try row.decode(column: "tiny")
        let small: Int16 = try row.decode(column: "small")
        let integer: Int32 = try row.decode(column: "integer")

        XCTAssertEqual(tiny, 255)
        XCTAssertEqual(small, -12)
        XCTAssertEqual(integer, 123_456)
        XCTAssertThrowsError(try UInt8.decode(from: .int(256))) { error in
            XCTAssertEqual(
                (error as? TDSDecodingError)?.code,
                .valueOutOfRange(expected: "UInt8", actual: .int(256))
            )
        }
        XCTAssertThrowsError(try Int16.decode(from: .int32(Int32.max))) { error in
            XCTAssertEqual(
                (error as? TDSDecodingError)?.code,
                .valueOutOfRange(expected: "Int16", actual: .int32(Int32.max))
            )
        }
    }

    func testRowTypedDecodingAnnotatesErrorsWithColumnContext() throws {
        let row = TDSRow(
            columns: [.init(name: "label", dataType: .nVarChar)],
            values: [.string("not an integer")]
        )

        XCTAssertThrowsError(try row.decode(column: "label", as: Int.self)) { error in
            guard let decodingError = error as? TDSDecodingError else {
                return XCTFail("Expected TDSDecodingError, got \(error)")
            }
            XCTAssertEqual(decodingError.code, .typeMismatch(expected: "Int", actual: .string("not an integer")))
            XCTAssertEqual(decodingError.columnName, "label")
            XCTAssertEqual(decodingError.columnIndex, 0)
            XCTAssertEqual(decodingError.dataType, .nVarChar)
        }

        XCTAssertThrowsError(try row.decode(column: "missing", as: String.self)) { error in
            XCTAssertEqual((error as? TDSDecodingError)?.code, .missingColumn("missing"))
        }
    }

    func testChannelQueryTaskStreamsRowsAsTokensArrive() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT 1", streamPromise))
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
                payload: Self.selectOneRowPayload()
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

    func testRPCPacketEncodesTemporalParameterValueLengths() throws {
        let date = TDSDate(year: 2024, month: 1, day: 22)
        let time = TDSTime(hour: 10, minute: 46, second: 18, nanosecond: 713_000_000, scale: 3)
        let dateTime = TDSDateTime(date: date, time: time)
        let offset = TDSDateTimeOffset(dateTime: dateTime, offsetMinutes: 60)
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.temporal",
                parameters: [
                    .init(name: "@date", value: .date(date)),
                    .init(name: "@time", value: .time(time)),
                    .init(name: "@dt2", value: .datetime2(dateTime)),
                    .init(name: "@dto", value: .datetimeOffset(offset)),
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.temporal".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "@date")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.dateN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        packet.moveReaderIndex(forwardBy: 3)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "@time")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.timeN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        packet.moveReaderIndex(forwardBy: 4)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readUTF16(characterCount: 4), "@dt2")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.datetime2N.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 7)
        packet.moveReaderIndex(forwardBy: 7)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readUTF16(characterCount: 4), "@dto")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.datetimeOffsetN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 9)
        packet.moveReaderIndex(forwardBy: 9)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testBulkLoadPacketEncodesColumnMetadataRowsAndDone() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.bulkLoad(
            .init(
                columns: [
                    .init(name: "id", dataType: .int),
                    .init(name: "flag", dataType: .bit),
                    .init(name: "label", dataType: .nVarChar(maxBytes: 40)),
                    .init(name: "payload", dataType: .varBinary(maxBytes: 16)),
                ],
                rows: [
                    [.int(1), .bool(true), .string("one"), .bytes([0xAA, 0xBB])],
                    [.int(2), .null, .null, .null],
                ]
            ))

        var packet = encoder.flush()
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.bulkLoadData.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x81)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 4)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 2)
        XCTAssertEqual(packet.readUTF16(characterCount: 2), "id")

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.bitN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 1)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readUTF16(characterCount: 4), "flag")

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 40)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "label")

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigVarBin.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 16)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 7)
        XCTAssertEqual(packet.readUTF16(characterCount: 7), "payload")

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0xD1)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: Int64.self), 1)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 1)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 1)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 6)
        XCTAssertEqual(packet.readUTF16(characterCount: 3), "one")
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        XCTAssertEqual(packet.readBytes(length: 2), [0xAA, 0xBB])

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0xD1)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: Int64.self), 2)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0xFD)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt64.self), 2)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testBackendDecoderDecodesNBCRowNullBitmap() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.nbcRowTokenStreamPayload()
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)
        try channel.writeInbound(packet)

        var messages: [TDSBackendMessage] = []
        while let containers = try channel.readInbound(as: TinySequence<TDSBackendMessageDecoder.Container>.self) {
            for container in containers {
                messages.append(contentsOf: container.messages)
            }
        }

        XCTAssertEqual(messages.count, 3)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["id", "label"])
        guard case .row(let row) = messages[1] else {
            return XCTFail("Expected NBCROW")
        }
        XCTAssertEqual(row.values, [.int32(1), .null])
        guard case .done = messages[2] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesAltMetadataAndAltRow() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.altMetadataTokenStreamPayload()
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)
        try channel.writeInbound(packet)

        var messages: [TDSBackendMessage] = []
        while let containers = try channel.readInbound(as: TinySequence<TDSBackendMessageDecoder.Container>.self) {
            for container in containers {
                messages.append(contentsOf: container.messages)
            }
        }

        XCTAssertEqual(messages.count, 5)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["amount"])
        guard case .altMetadata(let altMetadata) = messages[1] else {
            return XCTFail("Expected ALTMETADATA")
        }
        XCTAssertEqual(altMetadata.count, 1)
        XCTAssertEqual(altMetadata.id, 7)
        XCTAssertEqual(altMetadata.byColumns, [1])
        XCTAssertEqual(altMetadata.columns[0].op, 0x4D)
        XCTAssertEqual(altMetadata.columns[0].operand, 1)
        XCTAssertEqual(altMetadata.columns[0].typeInfo.dataType, .int4)
        XCTAssertEqual(altMetadata.columns[0].name, "total")
        guard case .altRow(let altRow) = messages[2] else {
            return XCTFail("Expected ALTROW")
        }
        XCTAssertEqual(altRow.id, 7)
        XCTAssertEqual(altRow.values, [.int32(42)])
        guard case .row(let row) = messages[3] else {
            return XCTFail("Expected regular ROW after ALTROW")
        }
        XCTAssertEqual(row.values, [.int32(1)])
        guard case .done = messages[4] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesTemporalValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.temporalTokenStreamPayload()
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)
        try channel.writeInbound(packet)

        var messages: [TDSBackendMessage] = []
        while let containers = try channel.readInbound(as: TinySequence<TDSBackendMessageDecoder.Container>.self) {
            for container in containers {
                messages.append(contentsOf: container.messages)
            }
        }

        XCTAssertEqual(messages.count, 3)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["date", "time", "dt2", "dto"])
        guard case .row(let row) = messages[1] else {
            return XCTFail("Expected ROW")
        }
        XCTAssertEqual(row.values, Self.temporalValues)
        guard case .done = messages[2] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesLegacyTemporalAndMoneyValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.legacyTemporalMoneyTokenStreamPayload()
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)
        try channel.writeInbound(packet)

        var messages: [TDSBackendMessage] = []
        while let containers = try channel.readInbound(as: TinySequence<TDSBackendMessageDecoder.Container>.self) {
            for container in containers {
                messages.append(contentsOf: container.messages)
            }
        }

        XCTAssertEqual(messages.count, 3)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(
            metadata.columns.map(\.name), ["money", "smallmoney", "nullablemoney", "datetime", "smalldt", "nullabledt"])
        guard case .row(let row) = messages[1] else {
            return XCTFail("Expected ROW")
        }
        XCTAssertEqual(row.values, Self.legacyTemporalMoneyValues)
        guard case .done = messages[2] else {
            return XCTFail("Expected DONE")
        }
    }

    func testQueryResultIncludesAlternateRows() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT amount, SUM(amount)", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.altMetadataTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["amount"])
        XCTAssertEqual(result.rows.map(\.values), [[.int32(1)]])
        XCTAssertEqual(result.alternateResultSets.count, 1)
        XCTAssertEqual(result.alternateResultSets[0].id, 7)
        XCTAssertEqual(result.alternateResultSets[0].byColumns, [1])
        XCTAssertEqual(result.alternateResultSets[0].columns.map(\.name), ["total"])
        XCTAssertEqual(result.alternateResultSets[0].columns[0].metadata.alternateOperation, 0x4D)
        XCTAssertEqual(result.alternateResultSets[0].columns[0].metadata.alternateOperand, 1)
        XCTAssertEqual(result.alternateResultSets[0].rows.map(\.values), [[.int32(42)]])
        XCTAssertEqual(result.resultSets[0].alternateResultSets, result.alternateResultSets)
    }

    func testQueryResultIncludesNBCRowNullValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1, NULL", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.nbcRowTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["id", "label"])
        XCTAssertEqual(result.rows.count, 1)
        XCTAssertEqual(result.rows[0].values, [.int32(1), .null])
        XCTAssertEqual(result.rows[0]["label"], .null)
    }

    func testQueryResultIncludesTemporalValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT temporal values", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.temporalTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["date", "time", "dt2", "dto"])
        XCTAssertEqual(result.rows.count, 1)
        XCTAssertEqual(result.rows[0].values, Self.temporalValues)
        XCTAssertEqual(result.rows[0]["dt2"], Self.temporalValues[2])
    }

    func testQueryResultIncludesLegacyTemporalAndMoneyValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT legacy temporal and money values", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.legacyTemporalMoneyTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(
            result.columns.map(\.name), ["money", "smallmoney", "nullablemoney", "datetime", "smalldt", "nullabledt"])
        XCTAssertEqual(result.rows.count, 1)
        XCTAssertEqual(result.rows[0].values, Self.legacyTemporalMoneyValues)
        XCTAssertEqual(result.rows[0]["smallmoney"], .money("-12.3400"))
    }
}
