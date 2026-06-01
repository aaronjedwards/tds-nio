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
    @Test func queryInterpolationBuildsSpExecuteSQLRPC() throws {
        let id = 42
        let label = "forty-two"
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(id) AND label = \(label)"

        expectEqual(query.sql, "SELECT * FROM dbo.items WHERE id = @p0 AND label = @p1")
        expectEqual(query.binds.count, 2)
        expectEqual(query.binds.parameters.map(\.name), ["@p0", "@p1"])
        expectEqual(query.binds.parameters.map(\.value), [.int(42), .string("forty-two")])

        let rpc = query.rpcForExecution()
        expectEqual(rpc.procedure, "sp_executesql")
        expectEqual(rpc.parameters.map(\.name), ["@stmt", "@params", "@p0", "@p1"])
        expectEqual(rpc.parameters[0].value, .string(query.sql))
        expectEqual(rpc.parameters[1].value, .string("@p0 bigint, @p1 nvarchar(max)"))
    }

    @Test func queryInterpolationBindsOptionalNilsAsTypedNulls() throws {
        let id: Int? = nil
        let label: String? = nil
        let flag: Bool? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(id) AND label = \(label) AND flag = \(flag)"

        expectEqual(query.sql, "SELECT * FROM dbo.items WHERE id = @p0 AND label = @p1 AND flag = @p2")
        expectEqual(
            query.binds.parameters.map(\.value),
            [
                .typedNull(.bigInt),
                .typedNull(.nvarchar()),
                .typedNull(.bit),
            ])
        expectEqual(query.binds.declarationList, "@p0 bigint, @p1 nvarchar(max), @p2 bit")
    }

    @Test func queryInterpolationBindsIntegerWidths() throws {
        let tiny: UInt8 = 7
        let small: Int16 = -12
        let integer: Int32 = 123_456
        let query: TDSQuery = "SELECT \(tiny), \(small), \(integer)"

        expectEqual(query.sql, "SELECT @p0, @p1, @p2")
        expectEqual(
            query.binds.parameters.map(\.value),
            [
                .tinyInt(7),
                .smallInt(-12),
                .int32(123_456),
            ])
        expectEqual(query.binds.declarationList, "@p0 tinyint, @p1 smallint, @p2 int")
    }

    @Test func queryInterpolationBindsUUIDAsUniqueIdentifier() throws {
        let uuid = try requireUnwrap(UUID(uuidString: "00112233-4455-6677-8899-aabbccddeeff"))
        let nilUUID: UUID? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(uuid) OR parent_id = \(nilUUID)"

        expectEqual(query.sql, "SELECT * FROM dbo.items WHERE id = @p0 OR parent_id = @p1")
        expectEqual(
            query.binds.parameters.map(\.value),
            [
                .guid(TDSGUID(uuid)),
                .typedNull(.uniqueIdentifier),
            ])
        expectEqual(query.binds.declarationList, "@p0 uniqueidentifier, @p1 uniqueidentifier")
    }

    @Test func queryInterpolationBindsDecimalValues() throws {
        let amount = try requireUnwrap(Decimal(string: "123.45", locale: Locale(identifier: "en_US_POSIX")))
        let nilAmount: Decimal? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE amount = \(amount) OR discount = \(nilAmount)"

        expectEqual(query.sql, "SELECT * FROM dbo.items WHERE amount = @p0 OR discount = @p1")
        expectEqual(
            query.binds.parameters.map(\.value),
            [
                .decimal("123.45"),
                .typedNull(.decimal()),
            ])
        expectEqual(query.binds.declarationList, "@p0 decimal(5, 2), @p1 decimal(38, 0)")
    }

    @Test func queryInterpolationBindsFoundationDataValues() throws {
        let payload = Data([0x01, 0x02, 0x03])
        let archivedPayload: Data? = nil
        let query: TDSQuery =
            "SELECT * FROM dbo.items WHERE payload = \(payload) OR archived_payload = \(archivedPayload)"

        expectEqual(query.sql, "SELECT * FROM dbo.items WHERE payload = @p0 OR archived_payload = @p1")
        expectEqual(
            query.binds.parameters.map(\.value),
            [
                .bytes([0x01, 0x02, 0x03]),
                .typedNull(.varbinary()),
            ])
        expectEqual(query.binds.declarationList, "@p0 varbinary(max), @p1 varbinary(max)")
    }

    @Test func queryInterpolationBindsStructuredJSONValues() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let payload = try TDSJSON(JSONPayload(ok: true, count: 2), encoder: encoder)
        let missingPayload: TDSJSON<JSONPayload>? = nil
        let query: TDSQuery =
            "SELECT * FROM dbo.items WHERE payload = \(payload) OR missing_payload = \(missingPayload)"

        expectEqual(query.sql, "SELECT * FROM dbo.items WHERE payload = @p0 OR missing_payload = @p1")
        expectEqual(
            query.binds.parameters.map(\.value),
            [
                .json(Array(#"{"count":2,"ok":true}"#.utf8)),
                .typedNull(.json),
            ])
        expectEqual(query.binds.declarationList, "@p0 nvarchar(max), @p1 nvarchar(max)")
    }

    @Test func queryInterpolationBindsCustomTDSBindableValues() throws {
        let accountID = AccountID(rawValue: 42)
        let parentID: AccountID? = nil
        let query: TDSQuery = "SELECT * FROM dbo.accounts WHERE id = \(accountID) OR parent_id = \(parentID)"

        expectEqual(query.sql, "SELECT * FROM dbo.accounts WHERE id = @p0 OR parent_id = @p1")
        expectEqual(
            query.binds.parameters.map(\.value),
            [
                .int32(42),
                .typedNull(.int),
            ])
        expectEqual(query.binds.declarationList, "@p0 int, @p1 int")
    }

    @Test func queryInterpolationBindsFoundationDateValues() throws {
        let createdAt = Self.utcDate(year: 2024, month: 2, day: 29, hour: 12, minute: 34, second: 56)
        let deletedAt: Date? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE created_at = \(createdAt) OR deleted_at = \(deletedAt)"

        expectEqual(query.sql, "SELECT * FROM dbo.items WHERE created_at = @p0 OR deleted_at = @p1")
        expectEqual(
            query.binds.parameters.map(\.value),
            [
                .datetime2(TDSDateTime(createdAt)),
                .typedNull(.datetime2()),
            ])
        expectEqual(query.binds.declarationList, "@p0 datetime2(7), @p1 datetime2(7)")
    }

    @Test func temporalValuesBridgeToFoundationDate() throws {
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

        expectEqual(dateTime.date, TDSDate(year: 2024, month: 2, day: 29))
        expectEqual(dateTime.time, TDSTime(hour: 12, minute: 34, second: 56, nanosecond: 123_456_000, scale: 6))
        expectEqual(offsetDateTime.dateTime.date, TDSDate(year: 2024, month: 2, day: 29))
        expectEqual(offsetDateTime.dateTime.time.hour, 5)
        expectEqual(offsetDateTime.offsetMinutes, -420)
        expectEqual(
            try requireUnwrap(dateTime.dateValue()).timeIntervalSince1970, instant.timeIntervalSince1970,
            accuracy: 0.000_001)
        expectEqual(
            try requireUnwrap(offsetDateTime.dateValue()).timeIntervalSince1970, instant.timeIntervalSince1970,
            accuracy: 0.000_001)

        let decoded: Date = try Date.decode(from: .datetimeOffset(offsetDateTime))
        expectEqual(decoded.timeIntervalSince1970, instant.timeIntervalSince1970, accuracy: 0.000_001)
    }

    @Test func queryInterpolationDeclaresTVPAsReadonly() throws {
        let tvp = TDSTableValuedParameter(
            schemaName: "dbo",
            typeName: "IntList",
            columns: [.init(dataType: .int(maxBytes: 4))],
            rows: [[.int(1)], [.int(2)]]
        )
        var query = TDSQuery(unsafeSQL: "SELECT * FROM @ids")
        _ = query.binds.append(.table(tvp), name: "@ids")

        expectEqual(query.binds.declarationList, "@ids dbo.IntList READONLY")
        let rpc = query.rpcForExecution()
        expectEqual(rpc.parameters[1].value, .string("@ids dbo.IntList READONLY"))
    }

    @Test func queryInterpolationDeclaresTVPWithoutDefaultSchema() throws {
        let tvp = TDSTableValuedParameter(
            typeName: "IntList",
            columns: [.init(dataType: .int(maxBytes: 4))],
            rows: [[.int(1)], [.int(2)]]
        )
        var query = TDSQuery(unsafeSQL: "SELECT * FROM @ids")
        _ = query.binds.append(.table(tvp), name: "@ids")

        expectEqual(query.binds.declarationList, "@ids IntList READONLY")
        let rpc = query.rpcForExecution()
        expectEqual(rpc.parameters[1].value, .string("@ids IntList READONLY"))
    }

    @Test func rowSequenceCollectsRowsFromResultSet() async throws {
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

        expectEqual(first?["label"], .string("one"))
        expectEqual(Array(first ?? TDSRow(columns: [], values: [])), [.int(1), .string("one")])
        expectEqual(collected, rows)
    }

    @Test func rowsDecodeIntoModelTypes() async throws {
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

        expectEqual(
            try result.decodeRows(as: ItemRow.self),
            [
                ItemRow(id: 1, label: "one", payload: .init(ok: true, count: nil)),
                ItemRow(id: 2, label: "two", payload: .init(ok: false, count: 3)),
            ])
        expectEqual(
            try result.resultSets[0].decodeRows(as: ItemRow.self),
            [
                ItemRow(id: 1, label: "one", payload: .init(ok: true, count: nil)),
                ItemRow(id: 2, label: "two", payload: .init(ok: false, count: 3)),
            ])
        let collected = try await result.rowSequence.collect(as: ItemRow.self)
        expectEqual(
            collected,
            [
                ItemRow(id: 1, label: "one", payload: .init(ok: true, count: nil)),
                ItemRow(id: 2, label: "two", payload: .init(ok: false, count: 3)),
            ])
    }

    @Test func rowModelDecodingPreservesColumnContext() throws {
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

        expectThrowsError(try row.decode(as: ItemRow.self)) { error in
            guard let decodingError = error as? TDSDecodingError else {
                Issue.record("Expected TDSDecodingError, got \(error)"); return
            }
            expectEqual(decodingError.code, .typeMismatch(expected: "Int", actual: .string("not an integer")))
            expectEqual(decodingError.columnName, "id")
            expectEqual(decodingError.columnIndex, 0)
            expectEqual(decodingError.dataType, .nVarChar)
        }
    }

    @Test func rowCellsAndTypedDecoding() throws {
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

        expectTrue(row.contains("label"))
        expectEqual(row.firstIndex(ofColumn: "payload"), 2)
        expectEqual(row.cell(named: "label")?.columnIndex, 1)
        expectEqual(row.cell(named: "label")?.dataType, .nVarChar)

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

        expectEqual(id, 42)
        expectEqual(label, "forty-two")
        expectEqual(payload, Array(#"{"ok":true}"#.utf8))
        expectEqual(payloadData, Data(#"{"ok":true}"#.utf8))
        expectEqual(jsonPayload.value, JSONPayload(ok: true, count: nil))
        expectEqual(uuid.uuidString.lowercased(), Self.guid.stringValue)
        expectEqual(amount, Decimal(string: "123.45", locale: Locale(identifier: "en_US_POSIX")))
        expectEqual(accountID, AccountID(rawValue: 42))
        expectNil(maybe)
        expectNil(typedNil)
    }

    @Test func rowCanCreateOracleStyleRandomAccessView() throws {
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

        expectEqual(randomAccess.count, 3)
        expectTrue(randomAccess.contains("label"))
        expectFalse(randomAccess.contains("missing"))
        expectEqual(randomAccess[0].columnName, "id")
        expectEqual(randomAccess[1].value, .string("forty-two"))
        expectEqual(randomAccess["payload"].dataType, .json)

        let id: Int32 = try randomAccess.decode(column: "id")
        let label: String = try randomAccess.decode(column: 1)
        let payload: TDSJSONValue<JSONPayload> = try randomAccess.decode(column: "payload")

        expectEqual(id, 42)
        expectEqual(label, "forty-two")
        expectEqual(payload.value, JSONPayload(ok: true, count: nil))
    }

    @Test func widthSpecificIntegerTypedDecoding() throws {
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

        expectEqual(tiny, 255)
        expectEqual(small, -12)
        expectEqual(integer, 123_456)
        expectThrowsError(try UInt8.decode(from: .int(256))) { error in
            expectEqual(
                (error as? TDSDecodingError)?.code,
                .valueOutOfRange(expected: "UInt8", actual: .int(256))
            )
        }
        expectThrowsError(try Int16.decode(from: .int32(Int32.max))) { error in
            expectEqual(
                (error as? TDSDecodingError)?.code,
                .valueOutOfRange(expected: "Int16", actual: .int32(Int32.max))
            )
        }
    }

    @Test func rowTypedDecodingAnnotatesErrorsWithColumnContext() throws {
        let row = TDSRow(
            columns: [.init(name: "label", dataType: .nVarChar)],
            values: [.string("not an integer")]
        )

        expectThrowsError(try row.decode(column: "label", as: Int.self)) { error in
            guard let decodingError = error as? TDSDecodingError else {
                Issue.record("Expected TDSDecodingError, got \(error)"); return
            }
            expectEqual(decodingError.code, .typeMismatch(expected: "Int", actual: .string("not an integer")))
            expectEqual(decodingError.columnName, "label")
            expectEqual(decodingError.columnIndex, 0)
            expectEqual(decodingError.dataType, .nVarChar)
        }

        expectThrowsError(try row.decode(column: "missing", as: String.self)) { error in
            expectEqual((error as? TDSDecodingError)?.code, .missingColumn("missing"))
        }
    }

    @Test func channelQueryTaskStreamsRowsAsTokensArrive() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT 1", streamPromise))
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
                payload: Self.selectOneRowPayload()
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

    @Test func rpcPacketEncodesTemporalParameterValueLengths() throws {
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

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@date")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.dateN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 3)
        packet.moveReaderIndex(forwardBy: 3)

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@time")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.timeN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        packet.moveReaderIndex(forwardBy: 4)

        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readUTF16(characterCount: 4), "@dt2")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.datetime2N.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readInteger(as: UInt8.self), 7)
        packet.moveReaderIndex(forwardBy: 7)

        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readUTF16(characterCount: 4), "@dto")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.datetimeOffsetN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readInteger(as: UInt8.self), 9)
        packet.moveReaderIndex(forwardBy: 9)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func bulkLoadPacketEncodesColumnMetadataRowsAndDone() throws {
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
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.bulkLoadData.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)

        expectEqual(packet.readInteger(as: UInt8.self), 0x81)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 4)

        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(as: UInt8.self), 2)
        expectEqual(packet.readUTF16(characterCount: 2), "id")

        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.bitN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 1)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readUTF16(characterCount: 4), "flag")

        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 40)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "label")

        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigVarBin.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 16)
        expectEqual(packet.readInteger(as: UInt8.self), 7)
        expectEqual(packet.readUTF16(characterCount: 7), "payload")

        expectEqual(packet.readInteger(as: UInt8.self), 0xD1)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(endianness: .little, as: Int64.self), 1)
        expectEqual(packet.readInteger(as: UInt8.self), 1)
        expectEqual(packet.readInteger(as: UInt8.self), 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 6)
        expectEqual(packet.readUTF16(characterCount: 3), "one")
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        expectEqual(packet.readBytes(length: 2), [0xAA, 0xBB])

        expectEqual(packet.readInteger(as: UInt8.self), 0xD1)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(endianness: .little, as: Int64.self), 2)
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        expectEqual(packet.readInteger(as: UInt8.self), 0xFD)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), 2)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func backendDecoderDecodesNBCRowNullBitmap() throws {
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

        expectEqual(messages.count, 3)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA"); return
        }
        expectEqual(metadata.columns.map(\.name), ["id", "label"])
        guard case .row(let row) = messages[1] else {
            Issue.record("Expected NBCROW"); return
        }
        expectEqual(row.values, [.int32(1), .null])
        guard case .done = messages[2] else {
            Issue.record("Expected DONE"); return
        }
    }

    @Test func backendDecoderDecodesAltMetadataAndAltRow() throws {
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

        expectEqual(messages.count, 5)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA"); return
        }
        expectEqual(metadata.columns.map(\.name), ["amount"])
        guard case .altMetadata(let altMetadata) = messages[1] else {
            Issue.record("Expected ALTMETADATA"); return
        }
        expectEqual(altMetadata.count, 1)
        expectEqual(altMetadata.id, 7)
        expectEqual(altMetadata.byColumns, [1])
        expectEqual(altMetadata.columns[0].op, 0x4D)
        expectEqual(altMetadata.columns[0].operand, 1)
        expectEqual(altMetadata.columns[0].typeInfo.dataType, .int4)
        expectEqual(altMetadata.columns[0].name, "total")
        guard case .altRow(let altRow) = messages[2] else {
            Issue.record("Expected ALTROW"); return
        }
        expectEqual(altRow.id, 7)
        expectEqual(altRow.values, [.int32(42)])
        guard case .row(let row) = messages[3] else {
            Issue.record("Expected regular ROW after ALTROW"); return
        }
        expectEqual(row.values, [.int32(1)])
        guard case .done = messages[4] else {
            Issue.record("Expected DONE"); return
        }
    }

    @Test func backendDecoderDecodesTemporalValues() throws {
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

        expectEqual(messages.count, 3)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA"); return
        }
        expectEqual(metadata.columns.map(\.name), ["date", "time", "dt2", "dto"])
        guard case .row(let row) = messages[1] else {
            Issue.record("Expected ROW"); return
        }
        expectEqual(row.values, Self.temporalValues)
        guard case .done = messages[2] else {
            Issue.record("Expected DONE"); return
        }
    }

    @Test func backendDecoderDecodesLegacyTemporalAndMoneyValues() throws {
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

        expectEqual(messages.count, 3)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA"); return
        }
        expectEqual(
            metadata.columns.map(\.name), ["money", "smallmoney", "nullablemoney", "datetime", "smalldt", "nullabledt"])
        guard case .row(let row) = messages[1] else {
            Issue.record("Expected ROW"); return
        }
        expectEqual(row.values, Self.legacyTemporalMoneyValues)
        guard case .done = messages[2] else {
            Issue.record("Expected DONE"); return
        }
    }

    @Test func queryResultIncludesAlternateRows() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT amount, SUM(amount)", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.altMetadataTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["amount"])
        expectEqual(result.rows.map(\.values), [[.int32(1)]])
        expectEqual(result.alternateResultSets.count, 1)
        expectEqual(result.alternateResultSets[0].id, 7)
        expectEqual(result.alternateResultSets[0].byColumns, [1])
        expectEqual(result.alternateResultSets[0].columns.map(\.name), ["total"])
        expectEqual(result.alternateResultSets[0].columns[0].metadata.alternateOperation, 0x4D)
        expectEqual(result.alternateResultSets[0].columns[0].metadata.alternateOperand, 1)
        expectEqual(result.alternateResultSets[0].rows.map(\.values), [[.int32(42)]])
        expectEqual(result.resultSets[0].alternateResultSets, result.alternateResultSets)
    }

    @Test func queryResultIncludesNBCRowNullValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1, NULL", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.nbcRowTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["id", "label"])
        expectEqual(result.rows.count, 1)
        expectEqual(result.rows[0].values, [.int32(1), .null])
        expectEqual(result.rows[0]["label"], .null)
    }

    @Test func queryResultIncludesTemporalValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT temporal values", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.temporalTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["date", "time", "dt2", "dto"])
        expectEqual(result.rows.count, 1)
        expectEqual(result.rows[0].values, Self.temporalValues)
        expectEqual(result.rows[0]["dt2"], Self.temporalValues[2])
    }

    @Test func queryResultIncludesLegacyTemporalAndMoneyValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT legacy temporal and money values", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.legacyTemporalMoneyTokenStreamPayload()
            ))

        let result = try queryPromise.futureResult.wait()
        expectEqual(
            result.columns.map(\.name), ["money", "smallmoney", "nullablemoney", "datetime", "smalldt", "nullabledt"])
        expectEqual(result.rows.count, 1)
        expectEqual(result.rows[0].values, Self.legacyTemporalMoneyValues)
        expectEqual(result.rows[0]["smallmoney"], .money("-12.3400"))
    }
}
