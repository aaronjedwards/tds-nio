import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOSSL
import NIOTestUtils
import XCTest

@testable import TDSNIO

private final class UserEventRecorder: ChannelInboundHandler {
    typealias InboundIn = Never

    var events: [Any] = []

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        self.events.append(event)
        context.fireUserInboundEventTriggered(event)
    }
}

private struct AccountID: TDSCodable, Hashable {
    static var tdsSQLType: TDSSQLType { .int }

    var rawValue: Int32

    var tdsData: TDSData { .int32(self.rawValue) }

    static func decode(from value: TDSData) throws -> AccountID {
        AccountID(rawValue: try Int32.decode(from: value))
    }
}

private struct JSONPayload: Codable, Sendable, Equatable {
    var ok: Bool
    var count: Int?
}

private struct ItemRow: TDSRowDecodable, Equatable {
    var id: Int
    var label: String
    var payload: JSONPayload

    init(id: Int, label: String, payload: JSONPayload) {
        self.id = id
        self.label = label
        self.payload = payload
    }

    init(row: TDSRow) throws {
        self.id = try row.decode(column: "id")
        self.label = try row.decode(column: "label")
        self.payload = try row.decode(column: "payload", as: TDSJSONValue<JSONPayload>.self).value
    }
}

final class TDSTests: XCTestCase {
    private static let temporalValues: [TDSData] = [
        .date(.init(year: 2024, month: 2, day: 29)),
        .time(.init(hour: 12, minute: 34, second: 56, nanosecond: 123_456_700, scale: 7)),
        .datetime2(.init(
            date: .init(year: 2024, month: 2, day: 29),
            time: .init(hour: 1, minute: 2, second: 3, nanosecond: 456_000_000, scale: 3)
        )),
        .datetimeOffset(.init(
            dateTime: .init(
                date: .init(year: 2024, month: 2, day: 29),
                time: .init(hour: 23, minute: 59, second: 59, nanosecond: 0, scale: 0)
            ),
            offsetMinutes: -420
        )),
    ]

    private static let legacyTemporalMoneyValues: [TDSData] = [
        .money("123.4567"),
        .money("-12.3400"),
        .null,
        .datetime(.init(
            date: .init(year: 2024, month: 2, day: 29),
            time: .init(hour: 1, minute: 2, second: 3, nanosecond: 0, scale: 3)
        )),
        .datetime(.init(
            date: .init(year: 2024, month: 2, day: 29),
            time: .init(hour: 12, minute: 34, second: 0, nanosecond: 0, scale: 0)
        )),
        .null,
    ]

    private static let guid = TDSGUID("00112233-4455-6677-8899-aabbccddeeff")

    private static func utcDate(
        year: Int,
        month: Int,
        day: Int,
        hour: Int = 0,
        minute: Int = 0,
        second: Int = 0,
        nanosecond: Int = 0
    ) -> Date {
        var calendar = Calendar(identifier: .gregorian)
        calendar.locale = Locale(identifier: "en_US_POSIX")
        calendar.timeZone = TimeZone(secondsFromGMT: 0)!
        return calendar.date(from: DateComponents(
            timeZone: calendar.timeZone,
            year: year,
            month: month,
            day: day,
            hour: hour,
            minute: minute,
            second: second,
            nanosecond: nanosecond
        ))!
    }

    func testPreloginPacketIsEncodedWithTDSHeader() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )

        encoder.prelogin(encryption: .encryptOn)
        var packet = encoder.flush()

        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.prelogin.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)

        var options: [(UInt8, UInt16, UInt16)] = []
        while let token = packet.readInteger(as: UInt8.self), token != 0xFF {
            let offset = try XCTUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            let length = try XCTUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            options.append((token, offset, length))
        }

        XCTAssertEqual(options.map(\.0), [0x00, 0x01, 0x02, 0x03, 0x04])
        XCTAssertEqual(options.map(\.1), [0x001A, 0x0020, 0x0021, 0x0022, 0x0026])
        XCTAssertEqual(options.map(\.2), [6, 1, 1, 4, 1])
        XCTAssertEqual(packet.getBytes(at: TDSPacket.headerLength + Int(options[0].1), length: 6), [
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00,
        ])
        XCTAssertEqual(packet.getInteger(at: TDSPacket.headerLength + Int(options[1].1), as: UInt8.self), 0x01)
        XCTAssertEqual(packet.getInteger(at: TDSPacket.headerLength + Int(options[2].1), as: UInt8.self), 0x00)
        XCTAssertEqual(packet.getInteger(at: TDSPacket.headerLength + Int(options[4].1), as: UInt8.self), 0x00)
    }

    func testPreloginPacketOffsetsAreDynamicWhenEncryptionIsOmitted() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )

        encoder.prelogin(encryption: nil)
        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength)

        var options: [(UInt8, UInt16, UInt16)] = []
        while let token = packet.readInteger(as: UInt8.self), token != 0xFF {
            let offset = try XCTUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            let length = try XCTUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            options.append((token, offset, length))
        }

        XCTAssertEqual(options.map(\.0), [0x00, 0x02, 0x03, 0x04])
        XCTAssertEqual(options.map(\.1), [0x0015, 0x001B, 0x001C, 0x0020])
        XCTAssertEqual(options.map(\.2), [6, 1, 4, 1])
        XCTAssertEqual(packet.getBytes(at: TDSPacket.headerLength + Int(options[0].1), length: 6), [
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00,
        ])
    }

    func testLoginPacketEncodesTDS74FeatureExtensions() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            database: "master",
            tls: .disable,
            clientHostName: "client"
        )

        encoder.login(configuration: configuration)
        let packet = encoder.flush()
        let loginStart = TDSPacket.headerLength

        XCTAssertEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.tds7Login.rawValue)
        XCTAssertEqual(packet.getInteger(at: loginStart, endianness: .little, as: UInt32.self), UInt32(packet.writerIndex - loginStart))
        XCTAssertEqual(packet.getInteger(at: loginStart + 4, endianness: .little, as: UInt32.self), TDSProtocolVersion.v7_4.wireValue)
        XCTAssertEqual(packet.getInteger(at: loginStart + 8, endianness: .little, as: UInt32.self), UInt32(configuration.packetSize))
        XCTAssertEqual(packet.getInteger(at: loginStart + 26, as: UInt8.self), 0x00)
        XCTAssertEqual(packet.getInteger(at: loginStart + 27, as: UInt8.self), 0x10)

        let extensionEntry = loginStart + 36 + 5 * 4
        let extensionOffset = try XCTUnwrap(packet.getInteger(at: extensionEntry, endianness: .little, as: UInt16.self))
        let extensionLength = try XCTUnwrap(packet.getInteger(at: extensionEntry + 2, endianness: .little, as: UInt16.self))
        XCTAssertEqual(extensionLength, 4)

        let featureExtOffset = try XCTUnwrap(packet.getInteger(
            at: loginStart + Int(extensionOffset),
            endianness: .little,
            as: UInt32.self
        ))
        var featureExt = try XCTUnwrap(packet.getSlice(
            at: loginStart + Int(featureExtOffset),
            length: packet.writerIndex - (loginStart + Int(featureExtOffset))
        ))

        XCTAssertEqual(featureExt.readInteger(as: UInt8.self), 0x09)
        XCTAssertEqual(featureExt.readInteger(endianness: .little, as: UInt32.self), 1)
        XCTAssertEqual(featureExt.readInteger(as: UInt8.self), 0x02)
        XCTAssertEqual(featureExt.readInteger(as: UInt8.self), 0x0D)
        XCTAssertEqual(featureExt.readInteger(endianness: .little, as: UInt32.self), 1)
        XCTAssertEqual(featureExt.readInteger(as: UInt8.self), 0x01)
        XCTAssertEqual(featureExt.readInteger(as: UInt8.self), 0xFF)
        XCTAssertEqual(featureExt.readableBytes, 0)
    }

    func testLoginPacketUsesConfiguredClampedPacketSize() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        var configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            tls: .disable,
            packetSize: 40_000
        )
        XCTAssertEqual(configuration.packetSize, TDSPacket.maximumNegotiatedPacketLength)
        configuration.packetSize = 8

        encoder.login(configuration: configuration)
        let packet = encoder.flush()
        let loginStart = TDSPacket.headerLength

        XCTAssertEqual(configuration.packetSize, TDSPacket.minimumPacketLength)
        XCTAssertEqual(
            packet.getInteger(at: loginStart + 8, endianness: .little, as: UInt32.self),
            UInt32(TDSPacket.minimumPacketLength)
        )
    }

    func testLoginPacketEncodesReadOnlyApplicationIntent() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            tls: .disable,
            applicationIntent: .readOnly
        )

        encoder.login(configuration: configuration)
        let packet = encoder.flush()
        let loginStart = TDSPacket.headerLength

        XCTAssertEqual(packet.getInteger(at: loginStart + 26, as: UInt8.self), 0x20)
        XCTAssertEqual(packet.getInteger(at: loginStart + 27, as: UInt8.self), 0x10)
    }

    func testLoginPacketEncodesSSPIAuthenticationMode() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        let initialToken: [UInt8] = [0x60, 0x82, 0x01, 0x02]
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "ignored",
            password: "ignored",
            tls: .disable,
            authentication: .sspi(initialToken: initialToken)
        )

        encoder.login(configuration: configuration)
        var packet = encoder.flush()
        let loginStart = TDSPacket.headerLength
        let sspiEntry = loginStart + 36 + 9 * 4 + 6
        let sspiOffset = try XCTUnwrap(packet.getInteger(
            at: sspiEntry,
            endianness: .little,
            as: UInt16.self
        ))
        let sspiLength = try XCTUnwrap(packet.getInteger(
            at: sspiEntry + 2,
            endianness: .little,
            as: UInt16.self
        ))
        let sspiLongLength = try XCTUnwrap(packet.getInteger(
            at: sspiEntry + 12,
            endianness: .little,
            as: UInt32.self
        ))

        XCTAssertEqual(packet.getInteger(at: loginStart + 25, as: UInt8.self), 0x83)
        XCTAssertEqual(try Self.loginStringField(index: 1, in: &packet), "")
        XCTAssertEqual(try Self.loginStringField(index: 2, in: &packet), "")
        XCTAssertEqual(sspiLength, UInt16(initialToken.count))
        XCTAssertEqual(sspiLongLength, UInt32(initialToken.count))
        XCTAssertEqual(packet.getBytes(at: loginStart + Int(sspiOffset), length: initialToken.count), initialToken)
    }

    func testLoginPacketEncodesInitialLanguage() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            database: "master",
            language: "us_english",
            tls: .disable
        )

        encoder.login(configuration: configuration)
        var packet = encoder.flush()

        XCTAssertEqual(try Self.loginStringField(index: 7, in: &packet), "us_english")
        XCTAssertEqual(try Self.loginStringField(index: 8, in: &packet), "master")
    }

    func testLoginPacketBoundsOversizedStringFields() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        let language = String(repeating: "x", count: Int(UInt16.max) + 5)
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            language: language,
            tls: .disable
        )

        encoder.login(configuration: configuration)
        let packet = encoder.flush()
        let loginStart = TDSPacket.headerLength
        let languageEntry = loginStart + 36 + 7 * 4
        let languageOffset = try XCTUnwrap(packet.getInteger(
            at: languageEntry,
            endianness: .little,
            as: UInt16.self
        ))
        let languageLength = try XCTUnwrap(packet.getInteger(
            at: languageEntry + 2,
            endianness: .little,
            as: UInt16.self
        ))

        let expectedLength = UInt16((Int(UInt16.max) - Int(languageOffset)) / 2)
        XCTAssertEqual(languageLength, expectedLength)
        XCTAssertEqual(
            packet.getBytes(at: loginStart + Int(languageOffset), length: 2),
            [0x78, 0x00]
        )
        XCTAssertEqual(
            packet.getBytes(at: loginStart + Int(languageOffset) + (Int(languageLength) - 1) * 2, length: 2),
            [0x78, 0x00]
        )
        XCTAssertEqual(
            packet.getInteger(at: 2, endianness: .big, as: UInt16.self),
            UInt16.max
        )
    }

    func testLoginPacketNormalizesClientIDToSixBytes() throws {
        var shortConfiguration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            tls: .disable,
            clientID: [0xAA, 0xBB]
        )
        XCTAssertEqual(shortConfiguration.clientID, [0xAA, 0xBB, 0, 0, 0, 0])

        shortConfiguration.clientID = [1, 2, 3, 4, 5, 6, 7, 8]
        XCTAssertEqual(shortConfiguration.clientID, [1, 2, 3, 4, 5, 6])

        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        encoder.login(configuration: shortConfiguration)
        let packet = encoder.flush()

        XCTAssertEqual(
            packet.getBytes(at: TDSPacket.headerLength + 36 + 9 * 4, length: 6),
            [1, 2, 3, 4, 5, 6]
        )
    }

    func testLoginPacketObfuscatesPasswordField() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            database: "master",
            tls: .disable,
            clientHostName: "client"
        )

        encoder.login(configuration: configuration)
        let packet = encoder.flush()
        let loginStart = TDSPacket.headerLength
        let passwordEntry = loginStart + 36 + 2 * 4
        let passwordOffset = try XCTUnwrap(packet.getInteger(
            at: passwordEntry,
            endianness: .little,
            as: UInt16.self
        ))
        let passwordLength = try XCTUnwrap(packet.getInteger(
            at: passwordEntry + 2,
            endianness: .little,
            as: UInt16.self
        ))

        XCTAssertEqual(passwordLength, UInt16(configuration.password.utf16.count))
        let encodedPassword = try XCTUnwrap(packet.getBytes(
            at: loginStart + Int(passwordOffset),
            length: Int(passwordLength) * 2
        ))
        XCTAssertEqual(encodedPassword, Self.loginPasswordBytes(configuration.password))
        XCTAssertNotEqual(encodedPassword, Array(configuration.password.utf16).flatMap {
            [UInt8($0 & 0x00FF), UInt8($0 >> 8)]
        })
    }

    func testCapabilitiesTrackLoginAckAndFeatureExtAck() throws {
        var capabilities = Capabilities(requestedProtocolVersion: .v7_4)
        let loginAck = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: TDSProtocolVersion.v7_4.wireValue,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )

        capabilities.adjustForLoginAck(loginAck)
        capabilities.adjustForFeatureExtAck(.init(options: [
            .init(featureID: Capabilities.FeatureID.dataClassification.rawValue, data: [0x02]),
            .init(featureID: Capabilities.FeatureID.jsonSupport.rawValue, data: [0x01]),
            .init(featureID: Capabilities.FeatureID.utf8Support.rawValue, data: [0x00]),
            .init(featureID: 0xFE, data: [0xAA]),
        ]))

        XCTAssertEqual(capabilities.requestedProtocolVersion.description, "7.4")
        XCTAssertEqual(capabilities.negotiatedProtocolVersion?.description, "7.4")
        XCTAssertTrue(capabilities.wasAcknowledged(.dataClassification))
        XCTAssertEqual(capabilities.dataClassificationVersion, 2)
        XCTAssertTrue(capabilities.supportsJSON)
        XCTAssertFalse(capabilities.supportsUTF8)
        XCTAssertEqual(capabilities.acknowledgedFeatureExtensions[0xFE], [0xAA])
    }

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

    func testSQLBatchPacketEncodesAllHeadersAndUnicodeText() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 128)
        )

        encoder.sqlBatch("SELECT 1")

        var packet = encoder.flush()
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 22)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 18)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0x02)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt64.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 1)
        XCTAssertEqual(packet.readUTF16(characterCount: 8), "SELECT 1")
        XCTAssertEqual(packet.readableBytes, 0)
    }

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

    func testQueryInterpolationBindsOptionalNilsAsTypedNulls() throws {
        let id: Int? = nil
        let label: String? = nil
        let flag: Bool? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(id) AND label = \(label) AND flag = \(flag)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE id = @p0 AND label = @p1 AND flag = @p2")
        XCTAssertEqual(query.binds.parameters.map(\.value), [
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
        XCTAssertEqual(query.binds.parameters.map(\.value), [
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
        XCTAssertEqual(query.binds.parameters.map(\.value), [
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
        XCTAssertEqual(query.binds.parameters.map(\.value), [
            .decimal("123.45"),
            .typedNull(.decimal()),
        ])
        XCTAssertEqual(query.binds.declarationList, "@p0 decimal(5, 2), @p1 decimal(38, 0)")
    }

    func testQueryInterpolationBindsFoundationDataValues() throws {
        let payload = Data([0x01, 0x02, 0x03])
        let archivedPayload: Data? = nil
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE payload = \(payload) OR archived_payload = \(archivedPayload)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE payload = @p0 OR archived_payload = @p1")
        XCTAssertEqual(query.binds.parameters.map(\.value), [
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
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE payload = \(payload) OR missing_payload = \(missingPayload)"

        XCTAssertEqual(query.sql, "SELECT * FROM dbo.items WHERE payload = @p0 OR missing_payload = @p1")
        XCTAssertEqual(query.binds.parameters.map(\.value), [
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
        XCTAssertEqual(query.binds.parameters.map(\.value), [
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
        XCTAssertEqual(query.binds.parameters.map(\.value), [
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
        XCTAssertEqual(try XCTUnwrap(dateTime.dateValue()).timeIntervalSince1970, instant.timeIntervalSince1970, accuracy: 0.000_001)
        XCTAssertEqual(try XCTUnwrap(offsetDateTime.dateValue()).timeIntervalSince1970, instant.timeIntervalSince1970, accuracy: 0.000_001)

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
            TDSRow(columns: columns, values: [
                .int32(1),
                .string("one"),
                .json(Array(#"{"ok":true}"#.utf8)),
            ]),
            TDSRow(columns: columns, values: [
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

        XCTAssertEqual(try result.decodeRows(as: ItemRow.self), [
            ItemRow(id: 1, label: "one", payload: .init(ok: true, count: nil)),
            ItemRow(id: 2, label: "two", payload: .init(ok: false, count: 3)),
        ])
        XCTAssertEqual(try result.resultSets[0].decodeRows(as: ItemRow.self), [
            ItemRow(id: 1, label: "one", payload: .init(ok: true, count: nil)),
            ItemRow(id: 2, label: "two", payload: .init(ok: false, count: 3)),
        ])
        let collected = try await result.rowSequence.collect(as: ItemRow.self)
        XCTAssertEqual(collected, [
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

    func testChannelQueryTaskStreamsRowsAsTokensArrive() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT 1", streamPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneMetadataPayload()
        ))

        let stream = try streamPromise.futureResult.wait()
        let rowsFuture = stream.all()

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneRowPayload()
        ))
        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.donePayload()
        ))

        let rows = try rowsFuture.wait()
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0]["id"], .int32(1))
        XCTAssertEqual(rows[0]["label"], .string("one"))
    }

    func testChannelQueryTaskStreamsOnlyFirstResultSet() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT 1; SELECT 2", streamPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneMetadataPayload()
        ))

        let stream = try streamPromise.futureResult.wait()
        let rowsFuture = stream.all()

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneRowPayload(id: 1, label: "one")
        ))
        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.donePayload(status: .more)
        ))

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneMetadataPayload()
        ))
        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneRowPayload(id: 2, label: "two")
        ))
        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.donePayload()
        ))

        let rows = try rowsFuture.wait()
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0]["id"], .int32(1))
        XCTAssertEqual(rows[0]["label"], .string("one"))
    }

    func testChannelRowStreamPromiseFailsWhenErrorArrivesBeforeMetadata() throws {
        let channel = try Self.loggedInChannel()

        let streamPromise = channel.eventLoop.makePromise(of: TDSRowStream.self)
        try channel.writeOutbound(TDSTask.sqlBatchRows("SELECT broken", streamPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneMetadataPayload()
        ))

        let stream = try streamPromise.futureResult.wait()
        let rowsFuture = stream.all()

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneRowPayload(id: 1, label: "one")
        ))
        try channel.writeInbound(Self.packet(
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

    func testDoneErrorStatusFailsActiveQueryWithoutErrorToken() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.donePayload(status: .error)
        ))

        XCTAssertThrowsError(try queryPromise.futureResult.wait()) { error in
            let sqlError = error as? TDSSQLError
            XCTAssertEqual(sqlError?.code, .server)
            XCTAssertEqual(sqlError?.query?.sql, "SELECT broken")
        }
    }

    func testErrorTokenKeepsQueuedRequestUntilFinalDone() throws {
        let channel = try Self.loggedInChannel()

        let firstPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT broken", firstPromise))
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.errorPayload(message: "Invalid object name")
        ))

        XCTAssertThrowsError(try firstPromise.futureResult.wait()) { error in
            XCTAssertEqual((error as? TDSSQLError)?.serverInfo?.message, "Invalid object name")
        }

        let secondPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", secondPromise))
        let secondCompleted = NIOLockedValueBox(false)
        secondPromise.futureResult.whenComplete { _ in
            secondCompleted.withLockedValue { $0 = true }
        }
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
        XCTAssertFalse(secondCompleted.withLockedValue { $0 })

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.donePayload(status: .error)
        ))

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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneTokenStreamPayload()
        ))

        let firstResult = try firstPromise.futureResult.wait()
        XCTAssertEqual(firstResult.rows.map(\.values), [[.int32(1), .string("one")]])
        let secondOutbound = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(secondOutbound.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneTokenStreamPayload()
        ))

        _ = try firstPromise.futureResult.wait()
        _ = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(Self.readyForQueryEventCount(in: recorder.events), 0)

        try channel.writeInbound(Self.packet(
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
        try channel.writeInbound(Self.packet(
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
        try channel.writeInbound(Self.packet(
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
        try channel.writeInbound(Self.packet(
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
        XCTAssertEqual(messages[0].entries, [
            .init(stateID: 9, value: [0xAA, 0xBB]),
            .init(stateID: 3, value: [0xCC]),
        ])
        XCTAssertTrue(recorder.events.contains { $0 is TDSSessionStateMessage })
    }

    func testBoundQueryRPCPacketEncodesSpExecuteSQL() throws {
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(42) AND label = \("forty-two")"
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 1_024)
        )

        encoder.rpc(query.rpcForExecution())

        var packet = encoder.flush()
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)
        packet.moveReaderIndex(forwardBy: 22)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 13)
        XCTAssertEqual(packet.readUTF16(characterCount: 13), "sp_executesql")
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "@stmt")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 108)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 108)
        XCTAssertEqual(
            packet.readUTF16(characterCount: 54),
            "SELECT * FROM dbo.items WHERE id = @p0 AND label = @p1"
        )

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 7)
        XCTAssertEqual(packet.readUTF16(characterCount: 7), "@params")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 58)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 58)
        XCTAssertEqual(packet.readUTF16(characterCount: 29), "@p0 bigint, @p1 nvarchar(max)")

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readUTF16(characterCount: 3), "@p0")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: Int64.self), 42)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readUTF16(characterCount: 3), "@p1")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 18)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 18)
        XCTAssertEqual(packet.readUTF16(characterCount: 9), "forty-two")
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testRPCPacketEncodesEmptyStringParameterWithValidNVarCharMetadata() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 128)
        )
        encoder.rpc(.init(
            procedure: "dbo.emptyText",
            parameters: [
                .init(name: "@text", value: .string("")),
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.emptyText".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "@text")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testRPCPacketEncodesTemporalParameterValueLengths() throws {
        let date = TDSDate(year: 2024, month: 1, day: 22)
        let time = TDSTime(hour: 10, minute: 46, second: 18, nanosecond: 713_000_000, scale: 3)
        let dateTime = TDSDateTime(date: date, time: time)
        let offset = TDSDateTimeOffset(dateTime: dateTime, offsetMinutes: 60)
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
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

    func testTypedNullBindingsDeclareAndEncodeSQLTypes() throws {
        var bindings = TDSBindings()
        bindings.append(.typedNull(.tinyInt), name: "@tiny")
        bindings.append(.typedNull(.smallInt), name: "@small")
        bindings.append(.typedNull(.int), name: "@integer")
        bindings.append(.typedNull(.bigInt), name: "@id")
        bindings.append(.typedNull(.nvarchar(maxBytes: 41)), name: "@label")
        bindings.append(.typedNull(.decimal(precision: 9, scale: 4)), name: "@amount")
        XCTAssertEqual(
            bindings.declarationList,
            "@tiny tinyint, @small smallint, @integer int, @id bigint, @label nvarchar(20), @amount decimal(9, 4)"
        )

        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
            procedure: "dbo.nulls",
            parameters: bindings.parameters
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.nulls".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "@tiny")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 1)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 6)
        XCTAssertEqual(packet.readUTF16(characterCount: 6), "@small")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 2)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readUTF16(characterCount: 8), "@integer")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readUTF16(characterCount: 3), "@id")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 6)
        XCTAssertEqual(packet.readUTF16(characterCount: 6), "@label")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 40)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 7)
        XCTAssertEqual(packet.readUTF16(characterCount: 7), "@amount")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.decimalN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 17)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 9)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testTypedNullBindingsDeclareAndEncodeCharacterAndBinarySQLTypes() throws {
        var bindings = TDSBindings()
        bindings.append(.typedNull(.char(maxBytes: 3)), name: "@char")
        bindings.append(.typedNull(.varchar(maxBytes: 12)), name: "@varchar")
        bindings.append(.typedNull(.varchar()), name: "@varcharMax")
        bindings.append(.typedNull(.nchar(maxBytes: 6)), name: "@nchar")
        bindings.append(.typedNull(.binary(maxBytes: 4)), name: "@binary")
        XCTAssertEqual(
            bindings.declarationList,
            "@char char(3), @varchar varchar(12), @varcharMax varchar(max), @nchar nchar(3), @binary binary(4)"
        )

        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
            procedure: "dbo.nulls",
            parameters: bindings.parameters
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.nulls".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "@char")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 3)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readUTF16(characterCount: 8), "@varchar")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 12)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 11)
        XCTAssertEqual(packet.readUTF16(characterCount: 11), "@varcharMax")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt64.self), UInt64.max)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 6)
        XCTAssertEqual(packet.readUTF16(characterCount: 6), "@nchar")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 6)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 7)
        XCTAssertEqual(packet.readUTF16(characterCount: 7), "@binary")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigBinary.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 4)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testRPCPacketEncodesWidthSpecificIntegerParameters() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
            procedure: "dbo.widths",
            parameters: [
                .init(name: "@tiny", value: .tinyInt(255)),
                .init(name: "@small", value: .smallInt(-123)),
                .init(name: "@integer", value: .int32(123_456)),
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.widths".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "@tiny")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 1)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 1)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 255)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 6)
        XCTAssertEqual(packet.readUTF16(characterCount: 6), "@small")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 2)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 2)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: Int16.self), -123)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readUTF16(characterCount: 8), "@integer")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: Int32.self), 123_456)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testRPCPacketEncodesProcedureAndParameters() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
            procedure: "dbo.echo",
            parameters: [
                .init(name: "@id", value: .int(42)),
                .init(name: "@label", value: .string("forty-two")),
            ]
        ))

        var packet = encoder.flush()
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 22)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 18)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0x02)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt64.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 1)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 8)
        XCTAssertEqual(packet.readUTF16(characterCount: 8), "dbo.echo")
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readUTF16(characterCount: 3), "@id")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: Int64.self), 42)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 6)
        XCTAssertEqual(packet.readUTF16(characterCount: 6), "@label")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 18)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 18)
        XCTAssertEqual(packet.readUTF16(characterCount: 9), "forty-two")
    }

    func testRPCPacketEncodesOutputParameterStatus() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
            procedure: "dbo.answer",
            parameters: [
                .init(name: "@answer", value: .int(0), isOutput: true),
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.answer".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 7)
        XCTAssertEqual(packet.readUTF16(characterCount: 7), "@answer")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x01)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 8)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: Int64.self), 0)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testRPCPacketEncodesDecimalParameter() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
            procedure: "dbo.money",
            parameters: [
                .init(name: "@amount", value: .decimal("123.45")),
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.money".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 7)
        XCTAssertEqual(packet.readUTF16(characterCount: 7), "@amount")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.decimalN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 17)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 2)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 17)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 1)
        XCTAssertEqual(packet.readBytes(length: 2), [0x39, 0x30])
        XCTAssertEqual(packet.readBytes(length: 14), Array(repeating: 0, count: 14))
    }

    func testRPCPacketEncodesGUIDParameter() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
            procedure: "dbo.guid",
            parameters: [
                .init(name: "@id", value: .guid(Self.guid)),
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.guid".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readUTF16(characterCount: 3), "@id")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.guid.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 16)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 16)
        XCTAssertEqual(packet.readBytes(length: 16), [
            0x33, 0x22, 0x11, 0x00,
            0x55, 0x44,
            0x77, 0x66,
            0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
        ])
    }

    func testRPCPacketEncodesXMLParameter() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
            procedure: "dbo.xml",
            parameters: [
                .init(name: "@doc", value: .xml([0x3C, 0x72, 0x2F, 0x3E])),
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.xml".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readUTF16(characterCount: 4), "@doc")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.xml.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt64.self), 4)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 4)
        XCTAssertEqual(packet.readBytes(length: 4), [0x3C, 0x72, 0x2F, 0x3E])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
    }

    func testRPCPacketEncodesJSONParameter() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(.init(
            procedure: "dbo.json",
            parameters: [
                .init(name: "@doc", value: .json(Array(#"{"ok":true}"#.utf8))),
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.json".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readUTF16(characterCount: 4), "@doc")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.json.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt64.self), 11)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 11)
        XCTAssertEqual(packet.readBytes(length: 11), Array(#"{"ok":true}"#.utf8))
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
    }

    func testRPCPacketEncodesLongStringParameterAsPLP() throws {
        let value = String(repeating: "x", count: 5_000)
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 12_000)
        )
        encoder.rpc(.init(
            procedure: "dbo.longText",
            parameters: [
                .init(name: "@text", value: .string(value)),
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.longText".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "@text")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt64.self), 10_000)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 10_000)
        let bytes = try XCTUnwrap(packet.readBytes(length: 10_000))
        XCTAssertEqual(bytes.count, 10_000)
        XCTAssertEqual(bytes.prefix(4), [0x78, 0x00, 0x78, 0x00])
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
    }

    func testRPCPacketEncodesLongBytesParameterAsPLP() throws {
        let value = Array(repeating: UInt8(0xA5), count: 9_001)
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 10_000)
        )
        encoder.rpc(.init(
            procedure: "dbo.longBytes",
            parameters: [
                .init(name: "@data", value: .bytes(value)),
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.longBytes".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 5)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "@data")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigVarBin.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt64.self), 9_001)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 9_001)
        let bytes = try XCTUnwrap(packet.readBytes(length: 9_001))
        XCTAssertEqual(bytes.count, 9_001)
        XCTAssertEqual(bytes.first, 0xA5)
        XCTAssertEqual(bytes.last, 0xA5)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
    }

    func testRPCPacketEncodesTableValuedParameter() throws {
        let tvp = TDSTableValuedParameter(
            schemaName: "dbo",
            typeName: "IntStringList",
            columns: [
                .init(dataType: .int(maxBytes: 4)),
                .init(dataType: .nVarChar(maxBytes: 40)),
            ],
            rows: [
                [.int(7), .string("seven")],
                [.int(8), .null],
            ]
        )
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        encoder.rpc(.init(
            procedure: "dbo.use_tvp",
            parameters: [.init(name: "@items", value: .table(tvp))]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.use_tvp".utf16.count * 2 + 2)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 6)
        XCTAssertEqual(packet.readUTF16(characterCount: 6), "@items")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0xF3)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 3)
        XCTAssertEqual(packet.readUTF16(characterCount: 3), "dbo")
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 13)
        XCTAssertEqual(packet.readUTF16(characterCount: 13), "IntStringList")

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 40)
        XCTAssertEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x00)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x01)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: Int32.self), 7)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 10)
        XCTAssertEqual(packet.readUTF16(characterCount: 5), "seven")

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x01)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 4)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: Int32.self), 8)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x00)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testRPCPacketBoundsTableValuedParameterVariableValuesToColumnMax() throws {
        let tvp = TDSTableValuedParameter(
            schemaName: "dbo",
            typeName: "BoundedList",
            columns: [
                .init(dataType: .nVarChar(maxBytes: 4)),
                .init(dataType: .varBinary(maxBytes: 3)),
            ],
            rows: [
                [.string("abcdef"), .bytes([1, 2, 3, 4, 5])],
            ]
        )
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        encoder.rpc(.init(
            procedure: "dbo.use_tvp",
            parameters: [.init(name: "@items", value: .table(tvp))]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.use_tvp".utf16.count * 2 + 2)
        packet.moveReaderIndex(forwardBy: 1 + "@items".utf16.count * 2 + 1 + 1)
        packet.moveReaderIndex(forwardBy: 1 + 1 + "dbo".utf16.count * 2 + 1 + "BoundedList".utf16.count * 2)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        for _ in 0..<2 {
            packet.moveReaderIndex(forwardBy: 4 + 2)
            let type = try XCTUnwrap(packet.readInteger(as: UInt8.self))
            if type == TDSDataType.nVarChar.rawValue {
                packet.moveReaderIndex(forwardBy: 2 + 5)
            } else {
                packet.moveReaderIndex(forwardBy: 2)
            }
            packet.moveReaderIndex(forwardBy: 1)
        }
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x00)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x01)

        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 4)
        XCTAssertEqual(packet.readUTF16(characterCount: 2), "ab")
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 3)
        XCTAssertEqual(packet.readBytes(length: 3), [1, 2, 3])
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x00)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testAttentionPacketIsEncodedWithEmptyPayload() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 32)
        )
        encoder.attention()

        var packet = encoder.flush()
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.attentionSignal.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength))
        XCTAssertEqual(packet.readableBytes, 4)
    }

    func testSSPIPacketEncodesRawAuthenticationBytes() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 32)
        )
        encoder.sspi([0x4E, 0x54, 0x4C, 0x4D])

        var packet = encoder.flush()
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sspi.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength + 4))
        packet.moveReaderIndex(forwardBy: 4)
        XCTAssertEqual(packet.readBytes(length: 4), [0x4E, 0x54, 0x4C, 0x4D])
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testFederatedAuthenticationPacketEncodesTokenAndNonce() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 64)
        )
        let nonce = Array(UInt8(0)..<UInt8(32))
        encoder.federatedAuthenticationToken(token: [0xAA, 0xBB, 0xCC], nonce: nonce)

        var packet = encoder.flush()
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.federatedAuthenticationToken.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength + 43))
        packet.moveReaderIndex(forwardBy: 4)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 39)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt32.self), 3)
        XCTAssertEqual(packet.readBytes(length: 3), [0xAA, 0xBB, 0xCC])
        XCTAssertEqual(packet.readBytes(length: 32), nonce)
        XCTAssertEqual(packet.readableBytes, 0)
    }

    func testAuthenticationTokenOutboundEventWritesContinuationPacket() throws {
        let channel = try Self.loggedInChannel()

        try channel.pipeline.triggerUserOutboundEvent(
            TDSAuthenticationToken.sspi([0x01, 0x02, 0x03])
        ).wait()

        var packet = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sspi.rawValue)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength + 3))
        packet.moveReaderIndex(forwardBy: 4)
        XCTAssertEqual(packet.readBytes(length: 3), [0x01, 0x02, 0x03])
    }

    func testFederatedAuthenticationOutboundEventRejectsInvalidNonceLength() throws {
        let channel = try Self.loggedInChannel()

        XCTAssertThrowsError(try channel.pipeline.triggerUserOutboundEvent(
            TDSAuthenticationToken.federated(token: [0xAA], nonce: [0x01])
        ).wait()) { error in
            let sqlError = error as? TDSSQLError
            XCTAssertEqual(sqlError?.code, .connectionError)
        }
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
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

    func testTransactionManagerTaskUsesPacketTypeAndCompletesOnDone() throws {
        let channel = try Self.loggedInChannel()

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.transactionManager(.commit(), promise))

        var packet: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.transactionManagerRequest.rawValue)
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 7)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(packet.readableBytes, 0)

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.donePayload()
        ))
        let result = try promise.futureResult.wait()
        XCTAssertEqual(result.rows.count, 0)
        XCTAssertEqual(result.resultSets.count, 0)
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

    func testBulkLoadPacketEncodesColumnMetadataRowsAndDone() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.bulkLoad(.init(
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

    func testBulkLoadPacketBoundsVariableValuesToColumnMax() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.bulkLoad(.init(
            columns: [
                .init(name: "label", dataType: .nVarChar(maxBytes: 4)),
                .init(name: "payload", dataType: .varBinary(maxBytes: 3)),
            ],
            rows: [
                [.string("abcdef"), .bytes([1, 2, 3, 4, 5])],
            ]
        ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength)
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0x81)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        for _ in 0..<2 {
            packet.moveReaderIndex(forwardBy: 4 + 2)
            let type = try XCTUnwrap(packet.readInteger(as: UInt8.self))
            if type == TDSDataType.nVarChar.rawValue {
                packet.moveReaderIndex(forwardBy: 2 + 5)
            } else {
                packet.moveReaderIndex(forwardBy: 2)
            }
            let nameLength = Int(try XCTUnwrap(packet.readInteger(as: UInt8.self)))
            packet.moveReaderIndex(forwardBy: nameLength * 2)
        }

        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0xD1)
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 4)
        XCTAssertEqual(packet.readUTF16(characterCount: 2), "ab")
        XCTAssertEqual(packet.readInteger(endianness: .little, as: UInt16.self), 3)
        XCTAssertEqual(packet.readBytes(length: 3), [1, 2, 3])
        XCTAssertEqual(packet.readInteger(as: UInt8.self), 0xFD)
    }

    func testBulkLoadTaskUsesPacketTypeAndCompletesOnDone() throws {
        let channel = try Self.loggedInChannel()

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.bulkLoad(.init(
            columns: [.init(name: "id", dataType: .int)],
            rows: [[.int(1)]]
        ), promise))

        let packet: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.bulkLoadData.rawValue)

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.donePayload(status: .count, rowCount: 1)
        ))
        let result = try promise.futureResult.wait()
        XCTAssertEqual(result.rowsAffected, 1)
    }

    func testPostProcessorSplitsLargePacketsWithEOMOnlyOnFinalPacket() throws {
        let channel = EmbeddedChannel(handler: TDSFrontendMessagePostProcessor())
        let payloadLength = TDSPacket.maximumPacketDataLength * 2 + 17
        var packet = ByteBufferAllocator().buffer(capacity: TDSPacket.headerLength + payloadLength)
        packet.moveWriterIndex(forwardBy: TDSPacket.headerLength)
        packet.writeRepeatingByte(0xA5, count: payloadLength)
        packet.prepareSend(
            packetType: .sqlBatch,
            statusFlags: [.eom, .resetConnection],
            payloadLength: UInt16(payloadLength)
        )

        try channel.writeOutbound(packet)

        var first = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(first.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(first.readInteger(as: UInt8.self), TDSPacket.StatusFlag.resetConnection.rawValue)
        XCTAssertEqual(first.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.maximumPacketLength))
        first.moveReaderIndex(forwardBy: 2)
        XCTAssertEqual(first.readInteger(as: UInt8.self), 0)
        first.moveReaderIndex(forwardBy: 1)
        XCTAssertEqual(first.readableBytes, TDSPacket.maximumPacketDataLength)

        var second = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(second.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(second.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(second.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.maximumPacketLength))
        second.moveReaderIndex(forwardBy: 2)
        XCTAssertEqual(second.readInteger(as: UInt8.self), 1)
        second.moveReaderIndex(forwardBy: 1)
        XCTAssertEqual(second.readableBytes, TDSPacket.maximumPacketDataLength)

        var third = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(third.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(third.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(
            third.readInteger(endianness: .big, as: UInt16.self),
            UInt16(TDSPacket.headerLength + 17)
        )
        third.moveReaderIndex(forwardBy: 2)
        XCTAssertEqual(third.readInteger(as: UInt8.self), 2)
        third.moveReaderIndex(forwardBy: 1)
        XCTAssertEqual(third.readableBytes, 17)
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
    }

    func testPacketSizeEnvChangeUpdatesOutboundPacketSplitting() throws {
        let channel = try Self.loggedInChannel()

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.stringEnvChangePayload(type: 4, new: "512", old: "\(TDSPacket.maximumPacketLength)")
        ))

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT '\(String(repeating: "x", count: 700))'", queryPromise))

        var first = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(first.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(first.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(first.readInteger(endianness: .big, as: UInt16.self), 512)

        var second = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(second.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(second.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(second.readInteger(endianness: .big, as: UInt16.self), 512)

        var final = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(final.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(final.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertLessThan(try XCTUnwrap(final.readInteger(endianness: .big, as: UInt16.self)), 512)
    }

    func testPacketSizeEnvChangeClampsInvalidSmallValues() throws {
        let channel = try Self.loggedInChannel()

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.stringEnvChangePayload(type: 4, new: "8", old: "\(TDSPacket.maximumPacketLength)")
        ))

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT '\(String(repeating: "x", count: 600))'", queryPromise))

        var packets: [ByteBuffer] = []
        while let packet = try channel.readOutbound(as: ByteBuffer.self) {
            packets.append(packet)
        }

        XCTAssertGreaterThan(packets.count, 1)
        for index in packets.indices {
            var packet = packets[index]
            XCTAssertEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
            let status = try XCTUnwrap(packet.readInteger(as: UInt8.self))
            let packetLength = try XCTUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            XCTAssertLessThanOrEqual(packetLength, UInt16(TDSPacket.minimumPacketLength))
            packet.moveReaderIndex(forwardBy: 4)
            XCTAssertGreaterThan(packet.readableBytes, 0)
            XCTAssertEqual(
                status & TDSPacket.StatusFlag.eom.rawValue,
                index == packets.indices.last ? TDSPacket.StatusFlag.eom.rawValue : 0
            )
        }
    }

    func testConfiguredPacketSizeControlsInitialOutboundSplitting() throws {
        var configuration = Self.configuration()
        configuration.packetSize = 512
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT '\(String(repeating: "x", count: 600))'", queryPromise))

        var first = try XCTUnwrap(channel.readOutbound(as: ByteBuffer.self))
        XCTAssertEqual(first.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        XCTAssertEqual(first.readInteger(as: UInt8.self), 0)
        XCTAssertEqual(first.readInteger(endianness: .big, as: UInt16.self), 512)
    }

    func testBackendDecoderDecodesPreloginResponse() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.preloginResponsePayload(encryption: .encryptOff)
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)
        try channel.writeInbound(packet)

        let containers: TinySequence<TDSBackendMessageDecoder.Container> = try XCTUnwrap(
            channel.readInbound()
        )
        let container = try XCTUnwrap(containers.first)
        let message = try XCTUnwrap(container.messages.first)

        guard case .prelogin(let response) = message else {
            return XCTFail("Expected prelogin response, got \(message)")
        }
        XCTAssertEqual(response.encryption, .encryptOff)
        XCTAssertEqual(response.version?.major, 15)
    }

    func testBackendDecoderReassemblesSplitPacketsBeforeDecoding() throws {
        var payload = Self.selectOneTokenStreamPayload()
        let firstPayload = payload.readSlice(length: 18)!
        let secondPayload = payload

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            statusFlags: [],
            payload: firstPayload
        ))
        XCTAssertNil(try channel.readInbound(as: TinySequence<TDSBackendMessageDecoder.Container>.self))

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: secondPayload
        ))

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
            return XCTFail("Expected ROW")
        }
        XCTAssertEqual(row.values, [.int32(1), .string("one")])
        guard case .done = messages[2] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderFramesOptionalMetadataTokens() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.optionalMetadataTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 11)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["id"])
        guard case .tabName(let tabName) = messages[1] else {
            return XCTFail("Expected TABNAME")
        }
        XCTAssertEqual(tabName.tableNames, ["dbo"])
        guard case .colInfo(let colInfo) = messages[2] else {
            return XCTFail("Expected COLINFO")
        }
        XCTAssertEqual(colInfo.columns.count, 1)
        XCTAssertEqual(colInfo.columns[0].columnNumber, 1)
        XCTAssertEqual(colInfo.columns[0].tableNumber, 1)
        XCTAssertTrue(colInfo.columns[0].status.contains(.differentName))
        XCTAssertEqual(colInfo.columns[0].baseColumnName, "baseId")
        guard case .order(let order) = messages[3] else {
            return XCTFail("Expected ORDER")
        }
        XCTAssertEqual(order.columnNumbers, [1])
        guard case .offset(let offset) = messages[4] else {
            return XCTFail("Expected OFFSET")
        }
        XCTAssertEqual(offset.identifier, 0x0102)
        XCTAssertEqual(offset.offset, 42)
        guard case .featureExtAck(let featureExtAck) = messages[5] else {
            return XCTFail("Expected FEATUREEXTACK")
        }
        XCTAssertEqual(featureExtAck.options.count, 2)
        XCTAssertEqual(featureExtAck.options[0].featureID, 0x0A)
        XCTAssertEqual(featureExtAck.options[0].data, [0x01])
        XCTAssertEqual(featureExtAck.options[1].featureID, 0x0D)
        XCTAssertEqual(featureExtAck.options[1].data, [0x01, 0x02])
        guard case .sspi(let sspi) = messages[6] else {
            return XCTFail("Expected SSPI")
        }
        XCTAssertEqual(sspi, [0x01, 0x00])
        guard case .sessionState(let sessionState) = messages[7] else {
            return XCTFail("Expected SESSIONSTATE")
        }
        XCTAssertEqual(sessionState.sequenceNumber, 1)
        XCTAssertTrue(sessionState.status.contains(.recoverable))
        XCTAssertEqual(sessionState.entries.count, 1)
        XCTAssertEqual(sessionState.entries[0].stateID, 9)
        XCTAssertEqual(sessionState.entries[0].value, [0xFF, 0xFF, 0xFF, 0xFF])
        guard case .fedAuthInfo(let fedAuthInfo) = messages[8] else {
            return XCTFail("Expected FEDAUTHINFO")
        }
        XCTAssertEqual(fedAuthInfo.options.map(\.id), [0x01, 0x02])
        XCTAssertEqual(fedAuthInfo.stsURL, "https://sts.example.test")
        XCTAssertEqual(fedAuthInfo.spn, "MSSQLSvc/sql.example.test:1433")
        guard case .row(let row) = messages[9] else {
            return XCTFail("Expected ROW after optional metadata")
        }
        XCTAssertEqual(row.values, [.int32(1)])
        guard case .done = messages[10] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesRoutingEnvChange() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.routingEnvChangePayload()
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)
        try channel.writeInbound(packet)

        let containers: TinySequence<TDSBackendMessageDecoder.Container> = try XCTUnwrap(
            channel.readInbound()
        )
        let message = try XCTUnwrap(containers.first?.messages.first)

        guard
            case .envChange(let envChange) = message,
            case .routing(let routing) = envChange.value
        else {
            return XCTFail("Expected routing ENVCHANGE, got \(message)")
        }
        XCTAssertEqual(envChange.type, 20)
        XCTAssertEqual(routing.protocolByte, 0)
        XCTAssertEqual(routing.port, 1444)
        XCTAssertEqual(routing.server, "redirect.sql.example.test")
    }

    func testBackendDecoderDecodesReturnStatusAndReturnValue() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.returnStatusReturnValueAndDonePayload()
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
        guard case .returnStatus(let status) = messages[0] else {
            return XCTFail("Expected RETURNSTATUS")
        }
        XCTAssertEqual(status, 7)

        guard case .returnValue(let value) = messages[1] else {
            return XCTFail("Expected RETURNVALUE")
        }
        XCTAssertEqual(value.ordinal, 1)
        XCTAssertEqual(value.name, "@answer")
        XCTAssertEqual(value.status, 1)
        XCTAssertEqual(value.typeInfo.dataType, .intN)
        XCTAssertEqual(value.typeInfo.length, 4)
        XCTAssertEqual(value.value, .int32(42))

        guard case .done = messages[2] else {
            return XCTFail("Expected DONE")
        }
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

    func testBackendDecoderDecodesDataClassificationToken() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.dataClassificationTokenStreamPayload()
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
        guard case .featureExtAck(let featureExtAck) = messages[0] else {
            return XCTFail("Expected FEATUREEXTACK")
        }
        XCTAssertEqual(featureExtAck.options.first?.featureID, 0x09)
        XCTAssertEqual(featureExtAck.options.first?.data, [0x02, 0x01])
        guard case .colMetadata(let metadata) = messages[1] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["amount"])
        guard case .dataClassification(let dataClassification) = messages[2] else {
            return XCTFail("Expected DATACLASSIFICATION")
        }
        XCTAssertEqual(dataClassification.labels, [
            .init(name: "Confidential", id: "label-id")
        ])
        XCTAssertEqual(dataClassification.informationTypes, [
            .init(name: "Financial", id: "info-id")
        ])
        XCTAssertEqual(dataClassification.columns.count, 1)
        XCTAssertEqual(dataClassification.columns[0].properties.count, 1)
        XCTAssertEqual(dataClassification.columns[0].properties[0].labelIndex, 0)
        XCTAssertEqual(dataClassification.columns[0].properties[0].informationTypeIndex, 0)
        XCTAssertEqual(dataClassification.columns[0].properties[0].rank, 10)
        guard case .row(let row) = messages[3] else {
            return XCTFail("Expected ROW after DATACLASSIFICATION")
        }
        XCTAssertEqual(row.values, [.int32(42)])
        guard case .done = messages[4] else {
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

    func testBackendDecoderDecodesPLPMaxValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.plpMaxTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["text", "blob"])
        guard case .row(let firstRow) = messages[1] else {
            return XCTFail("Expected first ROW")
        }
        XCTAssertEqual(firstRow.values, [.string("hello world"), .bytes([0xDE, 0xAD, 0xBE, 0xEF])])
        guard case .row(let secondRow) = messages[2] else {
            return XCTFail("Expected second ROW")
        }
        XCTAssertEqual(secondRow.values, [.null, .null])
        guard case .done = messages[3] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesXMLValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.xmlTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["doc", "typedDoc"])
        XCTAssertNil(metadata.columns[0].typeInfo.xmlInfo)
        XCTAssertEqual(metadata.columns[1].typeInfo.xmlInfo, .init(
            databaseName: "master",
            owningSchema: "dbo",
            schemaCollection: "docSchema"
        ))
        guard case .row(let firstRow) = messages[1] else {
            return XCTFail("Expected first ROW")
        }
        XCTAssertEqual(firstRow.values, [.xml([0x3C, 0x72, 0x2F, 0x3E]), .xml([0x01, 0x02, 0x03])])
        guard case .row(let secondRow) = messages[2] else {
            return XCTFail("Expected second ROW")
        }
        XCTAssertEqual(secondRow.values, [.null, .null])
        guard case .done = messages[3] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesJSONValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.jsonTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["doc"])
        XCTAssertEqual(metadata.columns[0].typeInfo.dataType, .json)
        guard case .row(let firstRow) = messages[1] else {
            return XCTFail("Expected first ROW")
        }
        XCTAssertEqual(firstRow.values, [.json(Array(#"{"ok":true}"#.utf8))])
        guard case .row(let secondRow) = messages[2] else {
            return XCTFail("Expected second ROW")
        }
        XCTAssertEqual(secondRow.values, [.null])
        guard case .done = messages[3] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesNullTypeValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.nullTypeTokenStreamPayload()
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
        XCTAssertEqual(metadata.columns.map(\.name), ["nothing"])
        XCTAssertEqual(metadata.columns[0].typeInfo.dataType, .null)
        guard case .row(let row) = messages[1] else {
            return XCTFail("Expected ROW")
        }
        XCTAssertEqual(row.values, [.null])
        guard case .done = messages[2] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesSQLVariantValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.sqlVariantTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["variant"])
        XCTAssertEqual(metadata.columns[0].typeInfo.dataType, .sqlVariant)
        guard case .row(let firstRow) = messages[1] else {
            return XCTFail("Expected first ROW")
        }
        XCTAssertEqual(firstRow.values, [.int32(42)])
        guard case .row(let secondRow) = messages[2] else {
            return XCTFail("Expected second ROW")
        }
        XCTAssertEqual(secondRow.values, [.string("variant")])
        guard case .done = messages[3] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesUDTValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.udtTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["location"])
        XCTAssertEqual(metadata.columns[0].typeInfo.dataType, .udt)
        XCTAssertEqual(metadata.columns[0].typeInfo.length, UInt64(UInt16.max))
        XCTAssertEqual(metadata.columns[0].typeInfo.udtInfo?.databaseName, "master")
        XCTAssertEqual(metadata.columns[0].typeInfo.udtInfo?.schemaName, "sys")
        XCTAssertEqual(metadata.columns[0].typeInfo.udtInfo?.typeName, "geography")
        XCTAssertEqual(metadata.columns[0].typeInfo.udtInfo?.assemblyQualifiedName, "Microsoft.SqlServer.Types.SqlGeography")
        guard case .row(let firstRow) = messages[1] else {
            return XCTFail("Expected first ROW")
        }
        XCTAssertEqual(firstRow.values, [.bytes([0xE6, 0x10, 0x00, 0x01])])
        guard case .row(let secondRow) = messages[2] else {
            return XCTFail("Expected second ROW")
        }
        XCTAssertEqual(secondRow.values, [.null])
        guard case .done = messages[3] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesLegacyCharAndBinaryValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.legacyCharBinaryTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["varchar", "char", "varbinary", "binary"])
        guard case .row(let firstRow) = messages[1] else {
            return XCTFail("Expected first ROW")
        }
        XCTAssertEqual(firstRow.values, [.string("hello"), .string("abc"), .bytes([0xDE, 0xAD]), .bytes([0xBE, 0xEF])])
        guard case .row(let secondRow) = messages[2] else {
            return XCTFail("Expected second ROW")
        }
        XCTAssertEqual(secondRow.values, [.null, .string("xyz"), .null, .bytes([0x12, 0x34])])
        guard case .done = messages[3] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesLegacyLOBValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.legacyLOBTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["body", "unicodeBody", "picture"])
        guard case .row(let firstRow) = messages[1] else {
            return XCTFail("Expected first ROW")
        }
        XCTAssertEqual(firstRow.values, [.string("hello text"), .string("wide text"), .bytes([0xCA, 0xFE])])
        guard case .row(let secondRow) = messages[2] else {
            return XCTFail("Expected second ROW")
        }
        XCTAssertEqual(secondRow.values, [.null, .null, .null])
        guard case .done = messages[3] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesDecimalValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.decimalTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["amount"])
        guard case .row(let firstRow) = messages[1] else {
            return XCTFail("Expected first ROW")
        }
        XCTAssertEqual(firstRow.values, [.decimal("123.45")])
        guard case .row(let secondRow) = messages[2] else {
            return XCTFail("Expected second ROW")
        }
        XCTAssertEqual(secondRow.values, [.decimal("-1.23")])
        guard case .done = messages[3] else {
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
        XCTAssertEqual(metadata.columns.map(\.name), ["money", "smallmoney", "nullablemoney", "datetime", "smalldt", "nullabledt"])
        guard case .row(let row) = messages[1] else {
            return XCTFail("Expected ROW")
        }
        XCTAssertEqual(row.values, Self.legacyTemporalMoneyValues)
        guard case .done = messages[2] else {
            return XCTFail("Expected DONE")
        }
    }

    func testBackendDecoderDecodesGUIDValues() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.guidTokenStreamPayload()
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

        XCTAssertEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            return XCTFail("Expected COLMETADATA")
        }
        XCTAssertEqual(metadata.columns.map(\.name), ["id"])
        guard case .row(let firstRow) = messages[1] else {
            return XCTFail("Expected first ROW")
        }
        XCTAssertEqual(firstRow.values, [.guid(Self.guid)])
        guard case .row(let secondRow) = messages[2] else {
            return XCTFail("Expected second ROW")
        }
        XCTAssertEqual(secondRow.values, [.null])
        guard case .done = messages[3] else {
            return XCTFail("Expected DONE")
        }
    }

    func testStartupPipelineSendsPreloginLoginAndFiresStartupDone() throws {
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

        let prelogin: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(prelogin.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.prelogin.rawValue)
        var preloginOptions = prelogin
        preloginOptions.moveReaderIndex(forwardBy: TDSPacket.headerLength)
        var encryptionOffset: UInt16?
        while let token = preloginOptions.readInteger(as: UInt8.self), token != 0xFF {
            let offset = try XCTUnwrap(preloginOptions.readInteger(endianness: .big, as: UInt16.self))
            let _: UInt16 = try XCTUnwrap(preloginOptions.readInteger(endianness: .big, as: UInt16.self))
            if token == 0x01 {
                encryptionOffset = offset
            }
        }
        let offset = try XCTUnwrap(encryptionOffset)
        XCTAssertEqual(
            prelogin.getInteger(at: TDSPacket.headerLength + Int(offset), as: UInt8.self),
            TDSFrontendMessageEncoder.PreloginEncryption.encryptNotSup.rawValue
        )

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.preloginResponsePayload(encryption: .encryptOff)
        ))

        let login: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(login.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.tds7Login.rawValue)
        XCTAssertEqual(login.getInteger(at: 2, endianness: .big, as: UInt16.self), UInt16(login.writerIndex))

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.loginAckAndDonePayload()
        ))

        let context = try eventHandler.startupDoneFuture.wait()
        XCTAssertEqual(context.version, .v7_4)
        XCTAssertEqual(context.sessionID, 0)
        XCTAssertEqual(context.serialNumber, 0)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneTokenStreamPayload()
        ))
        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["id", "label"])
        XCTAssertEqual(result.rows.count, 1)
        XCTAssertEqual(result.rows[0].values, [.int32(1), .string("one")])
        XCTAssertEqual(result.rows[0]["label"], .string("one"))

        let rpcPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.rpc(
            .init(procedure: "dbo.echo", parameters: [.init(name: "@id", value: .int(1))]),
            rpcPromise
        ))
        let rpc: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(rpc.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.selectOneTokenStreamPayload()
        ))
        let rpcResult = try rpcPromise.futureResult.wait()
        XCTAssertEqual(rpcResult.rows[0]["id"], .int32(1))
    }

    func testStartupPipelineFailsStartupFutureOnLoginError() throws {
        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds-nio-tests")
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "wrong_password",
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
        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.preloginResponsePayload(encryption: .encryptOff)
        ))
        _ = try channel.readOutbound(as: ByteBuffer.self)
        XCTAssertThrowsError(try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.errorPayload(message: "Login failed for user 'sa'.", number: 18456)
        ))) { error in
            let sqlError = error as? TDSSQLError
            XCTAssertEqual(sqlError?.code, .server)
            XCTAssertEqual(sqlError?.serverInfo?.number, 18456)
        }

        XCTAssertThrowsError(try eventHandler.startupDoneFuture.wait()) { error in
            let sqlError = error as? TDSSQLError
            XCTAssertEqual(sqlError?.code, .server)
            XCTAssertEqual(sqlError?.serverInfo?.number, 18456)
        }
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
        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.preloginResponsePayload(encryption: .encryptOff)
        ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        var sspiPayload = ByteBufferAllocator().buffer(capacity: 8)
        sspiPayload.writeLengthPrefixedToken(0xED, bytes: [0xAA, 0xBB])
        try channel.writeInbound(Self.packet(
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
        try channel.writeInbound(Self.packet(
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

    func testRPCResultIncludesReturnStatusAndOutputParameters() throws {
        let channel = try Self.loggedInChannel()

        let rpcPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.rpc(
            .init(
                procedure: "dbo.answer",
                parameters: [.init(name: "@answer", value: .int(0))]
            ),
            rpcPromise
        ))
        let rpc: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(rpc.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.returnStatusReturnValueAndDonePayload()
        ))

        let result = try rpcPromise.futureResult.wait()
        XCTAssertEqual(result.returnStatus, 7)
        XCTAssertEqual(result.outputParameters.count, 1)
        XCTAssertEqual(result.outputParameters[0].ordinal, 1)
        XCTAssertEqual(result.outputParameters[0].name, "@answer")
        XCTAssertEqual(result.outputParameters[0].dataType, .intN)
        XCTAssertEqual(result.outputParameters[0].metadata.length, 4)
        XCTAssertEqual(result.outputParameters[0].value, .int32(42))
        XCTAssertEqual(result.outputParameter(at: 1)?.name, "@answer")
        XCTAssertEqual(result.outputParameter(named: "answer")?.value, .int32(42))

        let answer: Int32 = try result.decodeOutputParameter(named: "@answer")
        XCTAssertEqual(answer, 42)
        XCTAssertThrowsError(try result.decodeOutputParameter(named: "missing", as: Int.self)) { error in
            XCTAssertEqual((error as? TDSDecodingError)?.code, .missingOutputParameter("missing"))
        }
    }

    func testQueryResultIncludesOptionalMetadataTokens() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1 ORDER BY 1", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.dataClassificationTokenStreamPayload()
        ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["amount"])
        XCTAssertEqual(result.columns[0].metadata.sensitivityClassifications, [
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

    func testQueryResultIncludesAlternateRows() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT amount, SUM(amount)", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.nbcRowTokenStreamPayload()
        ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["id", "label"])
        XCTAssertEqual(result.rows.count, 1)
        XCTAssertEqual(result.rows[0].values, [.int32(1), .null])
        XCTAssertEqual(result.rows[0]["label"], .null)
    }

    func testQueryResultIncludesPLPMaxValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST('hello world' AS nvarchar(max))", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.xmlTokenStreamPayload()
        ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["doc", "typedDoc"])
        XCTAssertNil(result.columns[0].metadata.xmlInfo)
        XCTAssertEqual(result.columns[1].metadata.xmlInfo, .init(
            databaseName: "master",
            owningSchema: "dbo",
            schemaCollection: "docSchema"
        ))
        XCTAssertEqual(result.rows[0].cell(named: "typedDoc")?.columnMetadata.xmlInfo, .init(
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

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.legacyCharBinaryTokenStreamPayload()
        ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["varchar", "char", "varbinary", "binary"])
        XCTAssertEqual(result.rows.count, 2)
        XCTAssertEqual(result.rows[0].values, [.string("hello"), .string("abc"), .bytes([0xDE, 0xAD]), .bytes([0xBE, 0xEF])])
        XCTAssertEqual(result.rows[1].values, [.null, .string("xyz"), .null, .bytes([0x12, 0x34])])
        XCTAssertEqual(result.rows[0]["varbinary"], .bytes([0xDE, 0xAD]))
    }

    func testQueryResultIncludesLegacyLOBValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT legacy LOB values", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
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

    func testQueryResultIncludesTemporalValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT temporal values", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.legacyTemporalMoneyTokenStreamPayload()
        ))

        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["money", "smallmoney", "nullablemoney", "datetime", "smalldt", "nullabledt"])
        XCTAssertEqual(result.rows.count, 1)
        XCTAssertEqual(result.rows[0].values, Self.legacyTemporalMoneyValues)
        XCTAssertEqual(result.rows[0]["smallmoney"], .money("-12.3400"))
    }

    func testQueryResultIncludesGUIDValues() throws {
        let channel = try Self.loggedInChannel()

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT CAST('00112233-4455-6677-8899-aabbccddeeff' AS uniqueidentifier)", queryPromise))
        let sqlBatch: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.doneInProcFirstResultSetPayload()
        ))

        XCTAssertFalse(completed.withLockedValue { $0 })

        try channel.writeInbound(Self.packet(
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

        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.preloginResponsePayload(encryption: .encryptOff)
        ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        var payload = Self.routingEnvChangePayload()
        var loginAckAndDone = Self.loginAckAndDonePayload()
        payload.writeBuffer(&loginAckAndDone)
        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: payload
        ))

        let context = try eventHandler.startupDoneFuture.wait()
        XCTAssertEqual(context.routing?.protocolByte, 0)
        XCTAssertEqual(context.routing?.port, 1444)
        XCTAssertEqual(context.routing?.server, "redirect.sql.example.test")
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

    func testPreloginEncryptionIsDerivedFromTLSMode() throws {
        let sslContext = try NIOSSLContext(configuration: .makeClientConfiguration())

        XCTAssertEqual(TDSConnection.Configuration.TLS.disable.preloginEncryption, .encryptNotSup)
        XCTAssertEqual(TDSConnection.Configuration.TLS.prefer(sslContext).preloginEncryption, .encryptOn)
        XCTAssertEqual(TDSConnection.Configuration.TLS.require(sslContext).preloginEncryption, .encryptReq)

        XCTAssertFalse(TDSConnection.Configuration.TLS.disable.isCompatible(with: .encryptReq))
        XCTAssertFalse(TDSConnection.Configuration.TLS.require(sslContext).isCompatible(with: .encryptNotSup))
        XCTAssertTrue(TDSConnection.Configuration.TLS.prefer(sslContext).isCompatible(with: .encryptOff))
    }

    func testConfigurationCanApplyRoutingRedirect() throws {
        var configuration = TDSConnection.Configuration(
            host: "original.sql.example.test",
            port: 1433,
            username: "sa",
            password: "Secret123!",
            database: "master"
        )
        configuration.options.routingRedirectLimit = 2

        let redirected = try configuration.redirected(to: .init(
            protocolByte: 0,
            port: 1444,
            server: "redirect.sql.example.test"
        ))

        XCTAssertEqual(redirected.host, "redirect.sql.example.test")
        XCTAssertEqual(redirected.port, 1444)
        XCTAssertEqual(redirected.username, configuration.username)
        XCTAssertEqual(redirected.password, configuration.password)
        XCTAssertEqual(redirected.database, configuration.database)
        XCTAssertEqual(redirected.options.routingRedirectLimit, 2)
        XCTAssertEqual(redirected.serverNameForTLS, "redirect.sql.example.test")
    }

    func testConfigurationParsesSQLServerConnectionString() throws {
        let configuration = try TDSConnection.Configuration(connectionString: """
            Server=tcp:sql.example.test,1444;\
            Database=appdb;\
            User ID=sa;\
            Password={Secret;123!};\
            Application Name=App;\
            Language=us_english;\
            Packet Size=32768;\
            Encrypt=no;\
            Application Intent=ReadOnly
            """)

        XCTAssertEqual(configuration.host, "sql.example.test")
        XCTAssertEqual(configuration.port, 1444)
        XCTAssertEqual(configuration.database, "appdb")
        XCTAssertEqual(configuration.username, "sa")
        XCTAssertEqual(configuration.password, "Secret;123!")
        XCTAssertEqual(configuration.applicationName, "App")
        XCTAssertEqual(configuration.language, "us_english")
        XCTAssertEqual(configuration.packetSize, TDSPacket.maximumNegotiatedPacketLength)
        XCTAssertFalse(configuration.tls.isSupported)
        XCTAssertEqual(configuration.applicationIntent, .readOnly)
        XCTAssertEqual(configuration.authentication, .sqlServer)
    }

    func testConfigurationParsesIntegratedSecurityConnectionString() throws {
        let configuration = try TDSConnection.Configuration(connectionString: """
            Data Source=sql.example.test;\
            Initial Catalog=appdb;\
            Integrated Security=SSPI
            """)

        XCTAssertEqual(configuration.host, "sql.example.test")
        XCTAssertEqual(configuration.port, 1433)
        XCTAssertEqual(configuration.database, "appdb")
        XCTAssertEqual(configuration.username, "")
        XCTAssertEqual(configuration.password, "")
        XCTAssertEqual(configuration.authentication, .sspi(initialToken: []))
    }

    func testConfigurationRejectsInvalidConnectionStringValues() throws {
        XCTAssertThrowsError(try TDSConnection.Configuration(connectionString: "Database=appdb")) { error in
            XCTAssertEqual(error as? TDSConnectionStringError, .missingServer)
        }
        XCTAssertThrowsError(try TDSConnection.Configuration(connectionString: "Server=sql.example.test;User ID=sa")) { error in
            XCTAssertEqual(error as? TDSConnectionStringError, .missingPassword)
        }
        XCTAssertThrowsError(try TDSConnection.Configuration(connectionString: "Server=sql.example.test,nope;Integrated Security=true")) { error in
            XCTAssertEqual(error as? TDSConnectionStringError, .invalidPort("nope"))
        }
    }

    func testClientInitializesConnectionFactoryAndKeepAliveBehavior() throws {
        let configuration = TDSConnection.Configuration(
            host: "pooled.sql.example.test",
            username: "sa",
            password: "Secret123!",
            database: "master",
            packetSize: 2048
        )
        var options = TDSClient.Options()
        options.minimumConnections = 1
        options.maximumConnections = 4
        options.connectionIdleTimeout = .seconds(5)
        options.keepAliveBehavior = .init(frequency: .seconds(3))

        let client = TDSClient(configuration: configuration, options: options)

        XCTAssertEqual(client.factory.configuration.host, "pooled.sql.example.test")
        XCTAssertEqual(client.factory.configuration.database, "master")
        XCTAssertEqual(client.factory.configuration.packetSize, 2048)
        XCTAssertEqual(TDSKeepAliveBehavior(options.keepAliveBehavior).keepAliveFrequency, .seconds(3))
        XCTAssertNil(TDSKeepAliveBehavior(nil).keepAliveFrequency)
    }

    func testPreloginTLSHandlerWrapsAndUnwrapsTLSBytes() throws {
        let channel = EmbeddedChannel(handler: TDSPreloginTLSHandler())

        var outboundTLS = ByteBufferAllocator().buffer(capacity: 8)
        outboundTLS.writeBytes([0x16, 0x03, 0x03, 0x00, 0x2A])
        try channel.writeOutbound(outboundTLS)

        var wrapped: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(wrapped.readInteger(as: UInt8.self), TDSPacket.MessageType.prelogin.rawValue)
        XCTAssertEqual(wrapped.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        XCTAssertEqual(wrapped.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength + 5))
        wrapped.moveReaderIndex(forwardBy: 4)
        XCTAssertEqual(wrapped.readBytes(length: 5), [0x16, 0x03, 0x03, 0x00, 0x2A])

        var inboundTLS = ByteBufferAllocator().buffer(capacity: 8)
        inboundTLS.writeBytes([0x16, 0x03, 0x03, 0x00, 0x11])
        try channel.writeInbound(Self.packet(type: .prelogin, payload: inboundTLS))

        var unwrapped: ByteBuffer = try XCTUnwrap(channel.readInbound())
        XCTAssertEqual(unwrapped.readBytes(length: 5), [0x16, 0x03, 0x03, 0x00, 0x11])
    }

    func testStateMachineNegotiatesTLSBeforeLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOn,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard case .startTLS(let removeAfterLogin) = state.preloginReceived(
            response,
            clientEncryption: .encryptOn
        ) else {
            return XCTFail("Expected TLS negotiation to start")
        }
        XCTAssertFalse(removeAfterLogin)

        guard case .sendLoginRequest = state.tlsEstablished() else {
            return XCTFail("Expected LOGIN7 after TLS handshake")
        }
    }

    func testStateMachineMarksClientOnlyTLSForRemovalAfterLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard case .startTLS(let removeAfterLogin) = state.preloginReceived(
            response,
            clientEncryption: .encryptOn
        ) else {
            return XCTFail("Expected login-only TLS negotiation to start")
        }
        XCTAssertTrue(removeAfterLogin)
        guard case .sendLoginRequest = state.tlsEstablished() else {
            return XCTFail("Expected LOGIN7 after TLS handshake")
        }

        let ack = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: 0x7400_0004,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )
        guard case .wait = state.loginAckReceived(ack) else {
            return XCTFail("Expected LOGINACK to be stored until DONE")
        }

        let done = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)
        guard case .authenticated(_, let removeTLS) = state.doneReceived(done) else {
            return XCTFail("Expected authentication completion")
        }
        XCTAssertTrue(removeTLS)
    }

    private static func packet(
        type: TDSPacket.MessageType,
        statusFlags: [TDSPacket.StatusFlag] = [.eom],
        payload: ByteBuffer
    ) -> ByteBuffer {
        var payload = payload
        var packet = ByteBufferAllocator().buffer(capacity: payload.readableBytes + TDSPacket.headerLength)
        packet.moveWriterIndex(forwardBy: TDSPacket.headerLength)
        packet.writeBuffer(&payload)
        packet.prepareSend(
            packetType: type,
            statusFlags: statusFlags,
            payloadLength: UInt16(packet.readableBytes - TDSPacket.headerLength)
        )
        return packet
    }

    private static func preloginResponsePayload(
        encryption: TDSFrontendMessageEncoder.PreloginEncryption
    ) -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        buffer.writeBytes([
            0x00, 0x00, 0x0B, 0x00, 0x06,
            0x01, 0x00, 0x11, 0x00, 0x01,
            0xFF,
            0x0F, 0x00, 0x10, 0x6A, 0x00, 0x00,
            encryption.rawValue,
        ])
        return buffer
    }

    private static func loginAckAndDonePayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 64)
        payload.writeInteger(0xAD as UInt8)

        var loginAck = ByteBufferAllocator().buffer(capacity: 32)
        loginAck.writeInteger(0x01 as UInt8)
        loginAck.writeInteger(0x7400_0004 as UInt32, endianness: .big)
        loginAck.writeInteger(3 as UInt8)
        loginAck.writeUTF16("SQL")
        loginAck.writeBytes([16, 0, 0x10, 0x6A])

        payload.writeInteger(UInt16(loginAck.readableBytes), endianness: .little)
        payload.writeBuffer(&loginAck)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func routingEnvChangePayload() -> ByteBuffer {
        let server = "redirect.sql.example.test"
        var routingData = ByteBufferAllocator().buffer(capacity: 64)
        routingData.writeInteger(0 as UInt8)
        routingData.writeInteger(1444 as UInt16, endianness: .little)
        routingData.writeInteger(UInt16(server.utf16.count), endianness: .little)
        routingData.writeUTF16(server)

        var token = ByteBufferAllocator().buffer(capacity: 80)
        token.writeInteger(20 as UInt8)
        token.writeInteger(UInt16(routingData.readableBytes), endianness: .little)
        token.writeBuffer(&routingData)
        token.writeInteger(0 as UInt16, endianness: .little)

        var payload = ByteBufferAllocator().buffer(capacity: 96)
        payload.writeInteger(0xE3 as UInt8)
        payload.writeInteger(UInt16(token.readableBytes), endianness: .little)
        payload.writeBuffer(&token)
        return payload
    }

    private static func stringEnvChangePayload(
        type: UInt8,
        new: String,
        old: String
    ) -> ByteBuffer {
        var envChange = ByteBufferAllocator().buffer(capacity: 64)
        envChange.writeInteger(type)
        envChange.writeBVarchar(new)
        envChange.writeBVarchar(old)

        var payload = ByteBufferAllocator().buffer(capacity: 80)
        payload.writeLengthPrefixedToken(0xE3, bytes: Array(envChange.readableBytesView))
        return payload
    }

    private static func returnStatusReturnValueAndDonePayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 64)
        payload.writeInteger(0x79 as UInt8)
        payload.writeInteger(7 as Int32, endianness: .little)

        payload.writeInteger(0xAC as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeInteger(7 as UInt8)
        payload.writeUTF16("@answer")
        payload.writeInteger(1 as UInt8)
        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.intN.rawValue)
        payload.writeInteger(4 as UInt8)
        payload.writeInteger(4 as UInt8)
        payload.writeInteger(42 as Int32, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func errorPayload(
        message: String,
        number: Int32 = 208,
        severity: UInt8 = 16
    ) -> ByteBuffer {
        var tokenData = ByteBufferAllocator().buffer(capacity: 128)
        tokenData.writeInteger(number, endianness: .little)
        tokenData.writeInteger(1 as UInt8)
        tokenData.writeInteger(severity)
        tokenData.writeInteger(UInt16(message.utf16.count), endianness: .little)
        tokenData.writeUTF16(message)
        tokenData.writeInteger(0 as UInt8)
        tokenData.writeInteger(0 as UInt8)
        tokenData.writeInteger(1 as UInt32, endianness: .little)

        var payload = ByteBufferAllocator().buffer(capacity: tokenData.readableBytes + 3)
        payload.writeInteger(0xAA as UInt8)
        payload.writeInteger(UInt16(tokenData.readableBytes), endianness: .little)
        payload.writeBuffer(&tokenData)
        return payload
    }

    private static func infoPayload(
        message: String,
        number: Int32 = 0,
        severity: UInt8 = 0
    ) -> ByteBuffer {
        var payload = Self.errorPayload(message: message, number: number, severity: severity)
        payload.setInteger(0xAB as UInt8, at: payload.readerIndex)
        return payload
    }

    private static func donePayload(
        status: TDSBackendMessage.Done.Status = [],
        rowCount: UInt64 = 0
    ) -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 16)
        payload.writeInteger(0xFD as UInt8)
        payload.writeInteger(status.rawValue, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(rowCount, endianness: .little)
        return payload
    }

    private static func doneInProcPayload(
        status: TDSBackendMessage.Done.Status = [],
        rowCount: UInt64 = 0
    ) -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 16)
        payload.writeInteger(0xFF as UInt8)
        payload.writeInteger(status.rawValue, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(rowCount, endianness: .little)
        return payload
    }

    private static func transactionDescriptorEnvChangePayload(
        _ descriptor: [UInt8],
        old: [UInt8] = [],
        type: UInt8 = 8
    ) -> ByteBuffer {
        var envChange = ByteBufferAllocator().buffer(capacity: 16)
        envChange.writeInteger(type)
        envChange.writeBVarbyte(descriptor)
        envChange.writeBVarbyte(old)

        var payload = ByteBufferAllocator().buffer(capacity: 32)
        payload.writeLengthPrefixedToken(0xE3, bytes: Array(envChange.readableBytesView))
        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func selectOneTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 128)
        var metadata = Self.selectOneMetadataPayload()
        var row = Self.selectOneRowPayload()
        payload.writeBuffer(&metadata)
        payload.writeBuffer(&row)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func doneInProcFirstResultSetPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 128)
        var metadata = Self.selectOneMetadataPayload()
        var row = Self.selectOneRowPayload()
        var doneInProc = Self.doneInProcPayload(status: .count, rowCount: 1)
        payload.writeBuffer(&metadata)
        payload.writeBuffer(&row)
        payload.writeBuffer(&doneInProc)
        return payload
    }

    private static func selectOneMetadataPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 96)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(2 as UInt16, endianness: .little)

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.int4.rawValue)
        payload.writeInteger(2 as UInt8)
        payload.writeUTF16("id")

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.nVarChar.rawValue)
        payload.writeInteger(100 as UInt16, endianness: .little)
        payload.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
        payload.writeInteger(5 as UInt8)
        payload.writeUTF16("label")
        return payload
    }

    private static func selectOneRowPayload(
        id: Int32 = 1,
        label: String = "one"
    ) -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 32)
        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(id, endianness: .little)
        payload.writeInteger(UInt16(label.utf16.count * 2), endianness: .little)
        payload.writeUTF16(label)
        return payload
    }

    private static func optionalMetadataTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 96)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeColumnMetadata(type: .int4, name: "id")

        var tableName = ByteBufferAllocator().buffer(capacity: 16)
        tableName.writeInteger(1 as UInt8)
        tableName.writeInteger(3 as UInt16, endianness: .little)
        tableName.writeUTF16("dbo")
        payload.writeLengthPrefixedToken(0xA4, bytes: Array(tableName.readableBytesView))

        var colInfo = ByteBufferAllocator().buffer(capacity: 24)
        colInfo.writeInteger(1 as UInt8)
        colInfo.writeInteger(1 as UInt8)
        colInfo.writeInteger(0x20 as UInt8)
        colInfo.writeInteger(6 as UInt8)
        colInfo.writeUTF16("baseId")
        payload.writeLengthPrefixedToken(0xA5, bytes: Array(colInfo.readableBytesView))

        var order = ByteBufferAllocator().buffer(capacity: 4)
        order.writeInteger(1 as UInt16, endianness: .little)
        payload.writeLengthPrefixedToken(0xA9, bytes: Array(order.readableBytesView))
        payload.writeInteger(0x78 as UInt8)
        payload.writeInteger(0x0102 as UInt16, endianness: .little)
        payload.writeInteger(42 as UInt16, endianness: .little)
        payload.writeInteger(0xAE as UInt8)
        payload.writeInteger(0x0A as UInt8)
        payload.writeInteger(1 as UInt32, endianness: .little)
        payload.writeInteger(0x01 as UInt8)
        payload.writeInteger(0x0D as UInt8)
        payload.writeInteger(2 as UInt32, endianness: .little)
        payload.writeBytes([0x01, 0x02])
        payload.writeInteger(0xFF as UInt8)
        payload.writeLengthPrefixedToken(0xED, bytes: [0x01, 0x00])
        payload.writeLongLengthPrefixedToken(
            0xE4,
            bytes: [0x01, 0x00, 0x00, 0x00, 0x01, 0x09, 0x04, 0xFF, 0xFF, 0xFF, 0xFF]
        )
        var fedAuthInfo = ByteBufferAllocator().buffer(capacity: 128)
        fedAuthInfo.writeFedAuthInfo(
            stsURL: "https://sts.example.test",
            spn: "MSSQLSvc/sql.example.test:1433"
        )
        payload.writeLongLengthPrefixedToken(0xEE, bytes: Array(fedAuthInfo.readableBytesView))

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(1 as Int32, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func sessionStatePayload(
        sequenceNumber: UInt32,
        status: UInt8,
        entries: [(stateID: UInt8, value: [UInt8])]
    ) -> ByteBuffer {
        var data = ByteBufferAllocator().buffer(capacity: 32)
        data.writeInteger(sequenceNumber, endianness: .little)
        data.writeInteger(status)
        for entry in entries {
            data.writeInteger(entry.stateID)
            if entry.value.count < Int(UInt8.max) {
                data.writeInteger(UInt8(entry.value.count))
            } else {
                data.writeInteger(UInt8.max)
                data.writeInteger(UInt32(entry.value.count), endianness: .little)
            }
            data.writeBytes(entry.value)
        }

        var payload = ByteBufferAllocator().buffer(capacity: data.readableBytes + 5)
        payload.writeLongLengthPrefixedToken(0xE4, bytes: Array(data.readableBytesView))
        return payload
    }

    private static func multiResultSetTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 192)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeColumnMetadata(type: .int4, name: "id")

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(1 as Int32, endianness: .little)

        var firstDone = Self.donePayload(status: [.more, .count], rowCount: 1)
        payload.writeBuffer(&firstDone)

        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.nVarChar.rawValue)
        payload.writeInteger(100 as UInt16, endianness: .little)
        payload.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
        payload.writeInteger(5 as UInt8)
        payload.writeUTF16("label")

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(6 as UInt16, endianness: .little)
        payload.writeUTF16("two")

        var finalDone = Self.donePayload(status: .count, rowCount: 1)
        payload.writeBuffer(&finalDone)
        return payload
    }

    private static func nbcRowTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 128)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(2 as UInt16, endianness: .little)

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.int4.rawValue)
        payload.writeInteger(2 as UInt8)
        payload.writeUTF16("id")

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.nVarChar.rawValue)
        payload.writeInteger(100 as UInt16, endianness: .little)
        payload.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
        payload.writeInteger(5 as UInt8)
        payload.writeUTF16("label")

        payload.writeInteger(0xD2 as UInt8)
        payload.writeInteger(0b0000_0010 as UInt8)
        payload.writeInteger(1 as Int32, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func dataClassificationTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 192)
        payload.writeInteger(0xAE as UInt8)
        payload.writeInteger(0x09 as UInt8)
        payload.writeInteger(2 as UInt32, endianness: .little)
        payload.writeBytes([0x02, 0x01])
        payload.writeInteger(0xFF as UInt8)

        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeColumnMetadata(type: .int4, name: "amount")

        payload.writeInteger(0xA3 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeBVarchar("Confidential")
        payload.writeBVarchar("label-id")
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeBVarchar("Financial")
        payload.writeBVarchar("info-id")
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(10 as Int32, endianness: .little)

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(42 as Int32, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func altMetadataTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 128)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeColumnMetadata(type: .int4, name: "amount")

        payload.writeInteger(0x88 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeInteger(7 as UInt16, endianness: .little)
        payload.writeInteger(1 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeInteger(0x4D as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.int4.rawValue)
        payload.writeBVarchar("total")

        payload.writeInteger(0xD3 as UInt8)
        payload.writeInteger(7 as UInt16, endianness: .little)
        payload.writeInteger(42 as Int32, endianness: .little)

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(1 as Int32, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func legacyCharBinaryTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 160)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(4 as UInt16, endianness: .little)

        payload.writeLegacyColumnMetadata(type: .legacyVarChar, length: 20, name: "varchar")
        payload.writeLegacyColumnMetadata(type: .legacyChar, length: 3, name: "char")
        payload.writeLegacyColumnMetadata(type: .legacyVarBin, length: 8, name: "varbinary")
        payload.writeLegacyColumnMetadata(type: .legacyBinary, length: 2, name: "binary")

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(5 as UInt8)
        payload.writeBytes(Array("hello".utf8))
        payload.writeInteger(3 as UInt8)
        payload.writeBytes(Array("abc".utf8))
        payload.writeInteger(2 as UInt8)
        payload.writeBytes([0xDE, 0xAD])
        payload.writeInteger(2 as UInt8)
        payload.writeBytes([0xBE, 0xEF])

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(UInt8.max)
        payload.writeInteger(3 as UInt8)
        payload.writeBytes(Array("xyz".utf8))
        payload.writeInteger(UInt8.max)
        payload.writeInteger(2 as UInt8)
        payload.writeBytes([0x12, 0x34])

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func plpMaxTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 192)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(2 as UInt16, endianness: .little)

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.nVarChar.rawValue)
        payload.writeInteger(UInt16.max, endianness: .little)
        payload.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
        payload.writeInteger(4 as UInt8)
        payload.writeUTF16("text")

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.bigVarBin.rawValue)
        payload.writeInteger(UInt16.max, endianness: .little)
        payload.writeInteger(4 as UInt8)
        payload.writeUTF16("blob")

        payload.writeInteger(0xD1 as UInt8)
        var text = ByteBufferAllocator().buffer(capacity: 32)
        text.writeUTF16("hello world")
        payload.writePLPBytes(Array(text.readableBytesView), chunkSizes: [10, 12])
        payload.writePLPBytes([0xDE, 0xAD, 0xBE, 0xEF], chunkSizes: [2, 2])

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(UInt64.max, endianness: .little)
        payload.writeInteger(UInt64.max, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func xmlTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 192)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(2 as UInt16, endianness: .little)

        payload.writeXMLColumnMetadata(name: "doc", schema: nil)
        payload.writeXMLColumnMetadata(
            name: "typedDoc",
            schema: (database: "master", owner: "dbo", collection: "docSchema")
        )

        payload.writeInteger(0xD1 as UInt8)
        payload.writePLPBytes([0x3C, 0x72, 0x2F, 0x3E], chunkSizes: [2, 2])
        payload.writePLPBytes([0x01, 0x02, 0x03], chunkSizes: [3])

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(UInt64.max, endianness: .little)
        payload.writeInteger(UInt64.max, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func jsonTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 96)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.json.rawValue)
        payload.writeInteger(3 as UInt8)
        payload.writeUTF16("doc")

        payload.writeInteger(0xD1 as UInt8)
        payload.writePLPBytes(Array(#"{"ok":true}"#.utf8), chunkSizes: [5, 6])

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(UInt64.max, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func nullTypeTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 64)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.null.rawValue)
        payload.writeInteger(7 as UInt8)
        payload.writeUTF16("nothing")

        payload.writeInteger(0xD1 as UInt8)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func sqlVariantTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 128)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.sqlVariant.rawValue)
        payload.writeInteger(8009 as UInt32, endianness: .little)
        payload.writeInteger(7 as UInt8)
        payload.writeUTF16("variant")

        payload.writeInteger(0xD1 as UInt8)
        payload.writeSQLVariant(type: .int4, properties: []) { value in
            value.writeInteger(42 as Int32, endianness: .little)
        }

        payload.writeInteger(0xD1 as UInt8)
        payload.writeSQLVariant(
            type: .nVarChar,
            properties: [0x09, 0x04, 0xD0, 0x00, 0x34, 0x20, 0x00]
        ) { value in
            value.writeUTF16("variant")
        }

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func udtTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 192)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)

        payload.writeUDTColumnMetadata(
            name: "location",
            database: "master",
            schema: "sys",
            typeName: "geography",
            assemblyQualifiedName: "Microsoft.SqlServer.Types.SqlGeography"
        )

        payload.writeInteger(0xD1 as UInt8)
        payload.writePLPBytes([0xE6, 0x10, 0x00, 0x01], chunkSizes: [2, 2])

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(UInt64.max, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func legacyLOBTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 192)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(3 as UInt16, endianness: .little)

        payload.writeLegacyLOBColumnMetadata(type: .text, name: "body")
        payload.writeLegacyLOBColumnMetadata(type: .nText, name: "unicodeBody")
        payload.writeLegacyLOBColumnMetadata(type: .image, name: "picture")

        payload.writeInteger(0xD1 as UInt8)
        payload.writeLegacyLOBBytes(Array("hello text".utf8))
        var wideText = ByteBufferAllocator().buffer(capacity: 32)
        wideText.writeUTF16("wide text")
        payload.writeLegacyLOBBytes(Array(wideText.readableBytesView))
        payload.writeLegacyLOBBytes([0xCA, 0xFE])

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(0 as UInt8)
        payload.writeInteger(0 as UInt8)
        payload.writeInteger(0 as UInt8)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func decimalTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 96)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.decimalN.rawValue)
        payload.writeInteger(5 as UInt8)
        payload.writeInteger(10 as UInt8)
        payload.writeInteger(2 as UInt8)
        payload.writeInteger(6 as UInt8)
        payload.writeUTF16("amount")

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(5 as UInt8)
        payload.writeInteger(1 as UInt8)
        payload.writeBytes([0x39, 0x30, 0x00, 0x00])

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(5 as UInt8)
        payload.writeInteger(0 as UInt8)
        payload.writeBytes([0x7B, 0x00, 0x00, 0x00])

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func temporalTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 160)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(4 as UInt16, endianness: .little)

        payload.writeColumnMetadata(type: .dateN, name: "date")

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.timeN.rawValue)
        payload.writeInteger(7 as UInt8)
        payload.writeInteger(4 as UInt8)
        payload.writeUTF16("time")

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.datetime2N.rawValue)
        payload.writeInteger(3 as UInt8)
        payload.writeInteger(3 as UInt8)
        payload.writeUTF16("dt2")

        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.datetimeOffsetN.rawValue)
        payload.writeInteger(0 as UInt8)
        payload.writeInteger(3 as UInt8)
        payload.writeUTF16("dto")

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(3 as UInt8)
        payload.writeDate(year: 2024, month: 2, day: 29)
        payload.writeInteger(5 as UInt8)
        payload.writeTime(hour: 12, minute: 34, second: 56, fractionalUnits: 1_234_567, scale: 7)
        payload.writeInteger(7 as UInt8)
        payload.writeTime(hour: 1, minute: 2, second: 3, fractionalUnits: 456, scale: 3)
        payload.writeDate(year: 2024, month: 2, day: 29)
        payload.writeInteger(8 as UInt8)
        payload.writeTime(hour: 6, minute: 59, second: 59, fractionalUnits: 0, scale: 0)
        payload.writeDate(year: 2024, month: 3, day: 1)
        payload.writeInteger(-420 as Int16, endianness: .little)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func legacyTemporalMoneyTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 192)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(6 as UInt16, endianness: .little)

        payload.writeColumnMetadata(type: .money, name: "money")
        payload.writeColumnMetadata(type: .money4, name: "smallmoney")
        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.moneyN.rawValue)
        payload.writeInteger(8 as UInt8)
        payload.writeInteger(13 as UInt8)
        payload.writeUTF16("nullablemoney")
        payload.writeColumnMetadata(type: .datetime, name: "datetime")
        payload.writeColumnMetadata(type: .datetime4, name: "smalldt")
        payload.writeInteger(0 as UInt32, endianness: .little)
        payload.writeInteger(0 as UInt16, endianness: .little)
        payload.writeInteger(TDSDataType.datetimeN.rawValue)
        payload.writeInteger(8 as UInt8)
        payload.writeInteger(10 as UInt8)
        payload.writeUTF16("nullabledt")

        payload.writeInteger(0xD1 as UInt8)
        payload.writeMoney(scaledValue: 1_234_567, byteCount: 8)
        payload.writeMoney(scaledValue: -123_400, byteCount: 4)
        payload.writeInteger(0 as UInt8)
        payload.writeLegacyDateTime(year: 2024, month: 2, day: 29, hour: 1, minute: 2, second: 3)
        payload.writeSmallDateTime(year: 2024, month: 2, day: 29, hour: 12, minute: 34)
        payload.writeInteger(0 as UInt8)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func guidTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 80)
        payload.writeInteger(0x81 as UInt8)
        payload.writeInteger(1 as UInt16, endianness: .little)
        payload.writeColumnMetadata(type: .guid, name: "id")

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(16 as UInt8)
        payload.writeGUID(Self.guid)

        payload.writeInteger(0xD1 as UInt8)
        payload.writeInteger(0 as UInt8)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    private static func configuration() -> TDSConnection.Configuration {
        TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            database: "master",
            tls: .disable,
            clientHostName: "client"
        )
    }

    private static func loggedInChannel(
        configuration: TDSConnection.Configuration? = nil,
        recordingEventsWith recorder: UserEventRecorder? = nil
    ) throws -> EmbeddedChannel {
        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds-nio-tests")
        let configuration = configuration ?? Self.configuration()

        let eventHandler = TDSEventsHandler(logger: logger)
        let channelHandler = TDSChannelHandler(
            configuration: configuration,
            logger: logger
        )
        let postprocessor = TDSFrontendMessagePostProcessor(packetLength: configuration.packetSize)

        try channel.pipeline.syncOperations.addHandler(eventHandler)
        if let recorder {
            try channel.pipeline.syncOperations.addHandler(recorder, position: .before(eventHandler))
            try channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(recorder))
        } else {
            try channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(eventHandler))
        }
        try channel.pipeline.syncOperations.addHandler(postprocessor, position: .before(channelHandler))

        channel.pipeline.fireChannelActive()
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.preloginResponsePayload(encryption: .encryptOff)
        ))
        _ = try channel.readOutbound(as: ByteBuffer.self)
        try channel.writeInbound(Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.loginAckAndDonePayload()
        ))
        _ = try eventHandler.startupDoneFuture.wait()
        return channel
    }

    private static func readyForQueryEventCount(in events: [Any]) -> Int {
        events.filter {
            if case TDSSQLEvent.readyForQuery = $0 {
                return true
            }
            return false
        }.count
    }

    private static func loginStringField(index: Int, in packet: inout ByteBuffer) throws -> String {
        let loginStart = TDSPacket.headerLength
        let entry = loginStart + 36 + index * 4
        let offset = try XCTUnwrap(packet.getInteger(
            at: entry,
            endianness: .little,
            as: UInt16.self
        ))
        let length = try XCTUnwrap(packet.getInteger(
            at: entry + 2,
            endianness: .little,
            as: UInt16.self
        ))
        var field = try XCTUnwrap(packet.getSlice(
            at: loginStart + Int(offset),
            length: Int(length) * 2
        ))
        return try XCTUnwrap(field.readUTF16(characterCount: Int(length)))
    }

    private static func loginPasswordBytes(_ password: String) -> [UInt8] {
        password.utf16.flatMap { codeUnit -> [UInt8] in
            let swapped = ((codeUnit << 4) & 0xF0F0) | ((codeUnit >> 4) & 0x0F0F)
            let encoded = swapped ^ 0xA5A5
            return [UInt8(encoded & 0x00FF), UInt8(encoded >> 8)]
        }
    }
}

private extension ByteBuffer {
    mutating func writeUTF16(_ string: String) {
        for codeUnit in string.utf16 {
            self.writeInteger(codeUnit, endianness: .little)
        }
    }

    mutating func readUTF16(characterCount: Int) -> String? {
        guard let bytes = self.readBytes(length: characterCount * 2) else {
            return nil
        }
        return String(bytes: bytes, encoding: .utf16LittleEndian)
    }

    mutating func writeBVarchar(_ string: String) {
        self.writeInteger(UInt8(string.utf16.count))
        self.writeUTF16(string)
    }

    mutating func writePLPBytes(_ bytes: [UInt8], chunkSizes: [Int]) {
        self.writeInteger(UInt64(bytes.count), endianness: .little)
        var offset = 0
        for chunkSize in chunkSizes {
            let end = Swift.min(offset + chunkSize, bytes.count)
            guard offset < end else { continue }
            self.writeInteger(UInt32(end - offset), endianness: .little)
            self.writeBytes(bytes[offset..<end])
            offset = end
        }
        if offset < bytes.count {
            self.writeInteger(UInt32(bytes.count - offset), endianness: .little)
            self.writeBytes(bytes[offset...])
        }
        self.writeInteger(0 as UInt32, endianness: .little)
    }

    mutating func writeColumnMetadata(type: TDSDataType, name: String) {
        self.writeInteger(0 as UInt32, endianness: .little)
        self.writeInteger(0 as UInt16, endianness: .little)
        self.writeInteger(type.rawValue)
        self.writeInteger(UInt8(name.utf16.count))
        self.writeUTF16(name)
    }

    mutating func writeLengthPrefixedToken(_ token: UInt8, bytes: [UInt8]) {
        self.writeInteger(token)
        self.writeInteger(UInt16(bytes.count), endianness: .little)
        self.writeBytes(bytes)
    }

    mutating func writeBVarbyte(_ bytes: [UInt8]) {
        self.writeInteger(UInt8(bytes.count))
        self.writeBytes(bytes)
    }

    mutating func writeLongLengthPrefixedToken(_ token: UInt8, bytes: [UInt8]) {
        self.writeInteger(token)
        self.writeInteger(UInt32(bytes.count), endianness: .little)
        self.writeBytes(bytes)
    }

    mutating func writeFedAuthInfo(stsURL: String, spn: String) {
        let stsURLBytes = Self.utf16Bytes(stsURL)
        let spnBytes = Self.utf16Bytes(spn)
        let dataOffset = UInt32(4 + 2 * 9)

        self.writeInteger(2 as UInt32, endianness: .little)
        self.writeInteger(0x01 as UInt8)
        self.writeInteger(UInt32(stsURLBytes.count), endianness: .little)
        self.writeInteger(dataOffset, endianness: .little)
        self.writeInteger(0x02 as UInt8)
        self.writeInteger(UInt32(spnBytes.count), endianness: .little)
        self.writeInteger(dataOffset + UInt32(stsURLBytes.count), endianness: .little)
        self.writeBytes(stsURLBytes)
        self.writeBytes(spnBytes)
    }

    static func utf16Bytes(_ string: String) -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.reserveCapacity(string.utf16.count * 2)
        for codeUnit in string.utf16 {
            bytes.append(UInt8(codeUnit & 0x00FF))
            bytes.append(UInt8(codeUnit >> 8))
        }
        return bytes
    }

    mutating func writeXMLColumnMetadata(
        name: String,
        schema: (database: String, owner: String, collection: String)?
    ) {
        self.writeInteger(0 as UInt32, endianness: .little)
        self.writeInteger(0 as UInt16, endianness: .little)
        self.writeInteger(TDSDataType.xml.rawValue)
        if let schema {
            self.writeInteger(1 as UInt8)
            self.writeInteger(UInt8(schema.database.utf16.count))
            self.writeUTF16(schema.database)
            self.writeInteger(UInt8(schema.owner.utf16.count))
            self.writeUTF16(schema.owner)
            self.writeInteger(UInt16(schema.collection.utf16.count), endianness: .little)
            self.writeUTF16(schema.collection)
        } else {
            self.writeInteger(0 as UInt8)
        }
        self.writeInteger(UInt8(name.utf16.count))
        self.writeUTF16(name)
    }

    mutating func writeSQLVariant(
        type: TDSDataType,
        properties: [UInt8],
        value writeValue: (inout ByteBuffer) -> Void
    ) {
        var variant = ByteBufferAllocator().buffer(capacity: 32)
        variant.writeInteger(type.rawValue)
        variant.writeInteger(UInt8(properties.count))
        variant.writeBytes(properties)
        writeValue(&variant)
        self.writeInteger(UInt32(variant.readableBytes), endianness: .little)
        self.writeBuffer(&variant)
    }

    mutating func writeUDTColumnMetadata(
        name: String,
        database: String,
        schema: String,
        typeName: String,
        assemblyQualifiedName: String
    ) {
        self.writeInteger(0 as UInt32, endianness: .little)
        self.writeInteger(0 as UInt16, endianness: .little)
        self.writeInteger(TDSDataType.udt.rawValue)
        self.writeInteger(UInt16.max, endianness: .little)
        self.writeInteger(UInt8(database.utf16.count))
        self.writeUTF16(database)
        self.writeInteger(UInt8(schema.utf16.count))
        self.writeUTF16(schema)
        self.writeInteger(UInt8(typeName.utf16.count))
        self.writeUTF16(typeName)
        self.writeInteger(UInt16(assemblyQualifiedName.utf16.count), endianness: .little)
        self.writeUTF16(assemblyQualifiedName)
        self.writeInteger(UInt8(name.utf16.count))
        self.writeUTF16(name)
    }

    mutating func writeLegacyColumnMetadata(type: TDSDataType, length: UInt8, name: String) {
        self.writeInteger(0 as UInt32, endianness: .little)
        self.writeInteger(0 as UInt16, endianness: .little)
        self.writeInteger(type.rawValue)
        self.writeInteger(length)
        self.writeInteger(UInt8(name.utf16.count))
        self.writeUTF16(name)
    }

    mutating func writeLegacyLOBColumnMetadata(type: TDSDataType, name: String) {
        self.writeInteger(0 as UInt32, endianness: .little)
        self.writeInteger(0 as UInt16, endianness: .little)
        self.writeInteger(type.rawValue)
        self.writeInteger(UInt32.max, endianness: .little)
        if type == .text || type == .nText {
            self.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
        }
        self.writeInteger(1 as UInt8)
        self.writeInteger(3 as UInt16, endianness: .little)
        self.writeUTF16("dbo")
        self.writeInteger(UInt8(name.utf16.count))
        self.writeUTF16(name)
    }

    mutating func writeLegacyLOBBytes(_ bytes: [UInt8]) {
        self.writeInteger(4 as UInt8)
        self.writeBytes([0x01, 0x02, 0x03, 0x04])
        self.writeBytes([0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17])
        self.writeInteger(UInt32(bytes.count), endianness: .little)
        self.writeBytes(bytes)
    }

    mutating func writeDate(year: Int, month: Int, day: Int) {
        self.writeLittleEndianUnsignedInteger(UInt64(Self.daysSince0001(year: year, month: month, day: day)), byteCount: 3)
    }

    mutating func writeTime(
        hour: Int,
        minute: Int,
        second: Int,
        fractionalUnits: UInt64,
        scale: UInt8
    ) {
        let seconds = UInt64(hour * 3600 + minute * 60 + second)
        let unitsPerSecond = UInt64(Self.powerOf10(Int(scale)))
        let units = seconds * unitsPerSecond + fractionalUnits
        let byteCount: Int
        switch scale {
        case 0...2:
            byteCount = 3
        case 3...4:
            byteCount = 4
        default:
            byteCount = 5
        }
        self.writeLittleEndianUnsignedInteger(units, byteCount: byteCount)
    }

    mutating func writeMoney(scaledValue: Int64, byteCount: Int) {
        switch byteCount {
        case 4:
            self.writeInteger(Int32(scaledValue), endianness: .little)
        case 8:
            let bits = UInt64(bitPattern: scaledValue)
            self.writeInteger(UInt32((bits >> 32) & 0xFFFF_FFFF), endianness: .little)
            self.writeInteger(UInt32(bits & 0xFFFF_FFFF), endianness: .little)
        default:
            preconditionFailure("Unsupported money byte count")
        }
    }

    mutating func writeGUID(_ value: TDSGUID) {
        let bytes = Self.guidBytes(from: value.stringValue)
        self.writeInteger(Self.readHexInteger(bytes[0..<4], as: UInt32.self), endianness: .little)
        self.writeInteger(Self.readHexInteger(bytes[4..<6], as: UInt16.self), endianness: .little)
        self.writeInteger(Self.readHexInteger(bytes[6..<8], as: UInt16.self), endianness: .little)
        self.writeBytes(bytes[8..<16])
    }

    static func guidBytes(from string: String) -> [UInt8] {
        let hex = string.filter(\.isHexDigit)
        var bytes: [UInt8] = []
        bytes.reserveCapacity(16)
        var index = hex.startIndex
        while index < hex.endIndex {
            let next = hex.index(index, offsetBy: 2)
            bytes.append(UInt8(hex[index..<next], radix: 16) ?? 0)
            index = next
        }
        return bytes
    }

    static func readHexInteger<T: FixedWidthInteger>(
        _ bytes: ArraySlice<UInt8>,
        as type: T.Type
    ) -> T {
        var value: T = 0
        for byte in bytes {
            value = (value << 8) | T(byte)
        }
        return value
    }

    mutating func writeLegacyDateTime(
        year: Int,
        month: Int,
        day: Int,
        hour: Int,
        minute: Int,
        second: Int
    ) {
        self.writeInteger(
            Int32(Self.daysSince0001(year: year, month: month, day: day) - Self.daysBeforeYear(1900)),
            endianness: .little
        )
        self.writeInteger(UInt32((hour * 3600 + minute * 60 + second) * 300), endianness: .little)
    }

    mutating func writeSmallDateTime(
        year: Int,
        month: Int,
        day: Int,
        hour: Int,
        minute: Int
    ) {
        self.writeInteger(
            UInt16(Self.daysSince0001(year: year, month: month, day: day) - Self.daysBeforeYear(1900)),
            endianness: .little
        )
        self.writeInteger(UInt16(hour * 60 + minute), endianness: .little)
    }

    mutating func writeLittleEndianUnsignedInteger(_ value: UInt64, byteCount: Int) {
        for index in 0..<byteCount {
            self.writeInteger(UInt8((value >> UInt64(index * 8)) & 0xFF))
        }
    }

    static func daysSince0001(year: Int, month: Int, day: Int) -> Int {
        var days = 0
        if year > 1 {
            for y in 1..<year {
                days += Self.isLeapYear(y) ? 366 : 365
            }
        }
        let monthLengths = Self.isLeapYear(year) ?
            [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] :
            [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        days += monthLengths.prefix(month - 1).reduce(0, +)
        days += day - 1
        return days
    }

    static func daysBeforeYear(_ year: Int) -> Int {
        let previousYear = year - 1
        return previousYear * 365 + previousYear / 4 - previousYear / 100 + previousYear / 400
    }

    static func isLeapYear(_ year: Int) -> Bool {
        year.isMultiple(of: 4) && (!year.isMultiple(of: 100) || year.isMultiple(of: 400))
    }

    static func powerOf10(_ exponent: Int) -> Int {
        var value = 1
        for _ in 0..<exponent {
            value *= 10
        }
        return value
    }

}
