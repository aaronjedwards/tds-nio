import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOSSL
import NIOTestUtils
import Testing

@testable import TDSNIO

final class UserEventRecorder: ChannelInboundHandler {
    typealias InboundIn = Never

    var events: [Any] = []

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        self.events.append(event)
        context.fireUserInboundEventTriggered(event)
    }
}

struct AccountID: TDSCodable, Hashable {
    static var tdsSQLType: TDSSQLType { .int }

    var rawValue: Int32

    var tdsData: TDSData { .int32(self.rawValue) }

    static func decode(from value: TDSData) throws -> AccountID {
        AccountID(rawValue: try Int32.decode(from: value))
    }
}

struct JSONPayload: Codable, Sendable, Equatable {
    var ok: Bool
    var count: Int?
}

struct ItemRow: TDSRowDecodable, Equatable {
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

@Suite(.timeLimit(.minutes(5)))
final class TDSTests {
    static let temporalValues: [TDSData] = [
        .date(.init(year: 2024, month: 2, day: 29)),
        .time(.init(hour: 12, minute: 34, second: 56, nanosecond: 123_456_700, scale: 7)),
        .datetime2(
            .init(
                date: .init(year: 2024, month: 2, day: 29),
                time: .init(hour: 1, minute: 2, second: 3, nanosecond: 456_000_000, scale: 3)
            )),
        .datetimeOffset(
            .init(
                dateTime: .init(
                    date: .init(year: 2024, month: 2, day: 29),
                    time: .init(hour: 23, minute: 59, second: 59, nanosecond: 0, scale: 0)
                ),
                offsetMinutes: -420
            )),
    ]

    static let legacyTemporalMoneyValues: [TDSData] = [
        .money("123.4567"),
        .money("-12.3400"),
        .null,
        .datetime(
            .init(
                date: .init(year: 2024, month: 2, day: 29),
                time: .init(hour: 1, minute: 2, second: 3, nanosecond: 0, scale: 3)
            )),
        .datetime(
            .init(
                date: .init(year: 2024, month: 2, day: 29),
                time: .init(hour: 12, minute: 34, second: 0, nanosecond: 0, scale: 0)
            )),
        .null,
    ]

    static let guid = TDSGUID("00112233-4455-6677-8899-aabbccddeeff")

    static func utcDate(
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
        return calendar.date(
            from: DateComponents(
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


    static func packet(
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

    static func preloginResponsePayload(
        encryption: TDSFrontendMessageEncoder.PreloginEncryption,
        fedAuthRequired: Bool? = nil
    ) -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        var options: [(UInt8, [UInt8])] = [
            (0x00, [0x0F, 0x00, 0x10, 0x6A, 0x00, 0x00]),
            (0x01, [encryption.rawValue]),
        ]
        if let fedAuthRequired {
            options.append((0x06, [fedAuthRequired ? 0x01 : 0x00]))
        }

        let tableLength = options.count * 5 + 1
        var offset = tableLength
        for option in options {
            buffer.writeInteger(option.0)
            buffer.writeInteger(UInt16(offset), endianness: .big)
            buffer.writeInteger(UInt16(option.1.count), endianness: .big)
            offset += option.1.count
        }
        buffer.writeInteger(0xFF as UInt8)
        for option in options {
            buffer.writeBytes(option.1)
        }
        return buffer
    }

    static func loginAckAndDonePayload(
        interface: UInt8 = 0x01,
        tdsVersion: UInt32 = 0x7400_0004
    ) -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 64)
        payload.writeInteger(0xAD as UInt8)

        var loginAck = ByteBufferAllocator().buffer(capacity: 32)
        loginAck.writeInteger(interface)
        loginAck.writeInteger(tdsVersion, endianness: .big)
        loginAck.writeInteger(3 as UInt8)
        loginAck.writeUTF16("SQL")
        loginAck.writeBytes([16, 0, 0x10, 0x6A])

        payload.writeInteger(UInt16(loginAck.readableBytes), endianness: .little)
        payload.writeBuffer(&loginAck)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    static func routingEnvChangePayload(
        protocolByte: UInt8 = 0,
        server: String = "redirect.sql.example.test"
    ) -> ByteBuffer {
        var routingData = ByteBufferAllocator().buffer(capacity: 64)
        routingData.writeInteger(protocolByte)
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

    static func stringEnvChangePayload(
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

    static func collationEnvChangePayload(
        new: [UInt8],
        old: [UInt8] = [0x09, 0x04, 0xD0, 0x00, 0x34]
    ) -> ByteBuffer {
        var envChange = ByteBufferAllocator().buffer(capacity: 16)
        envChange.writeInteger(7 as UInt8)
        envChange.writeBVarbyte(new)
        envChange.writeBVarbyte(old)

        var payload = ByteBufferAllocator().buffer(capacity: 32)
        payload.writeLengthPrefixedToken(0xE3, bytes: Array(envChange.readableBytesView))
        return payload
    }

    static func resetConnectionEnvChangePayload() -> ByteBuffer {
        var envChange = ByteBufferAllocator().buffer(capacity: 4)
        envChange.writeInteger(18 as UInt8)
        envChange.writeBVarbyte([])
        envChange.writeBVarbyte([])

        var payload = ByteBufferAllocator().buffer(capacity: 8)
        payload.writeLengthPrefixedToken(0xE3, bytes: Array(envChange.readableBytesView))
        return payload
    }

    static func featureExtAckPayload(featureID: UInt8, data: [UInt8]) -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 16 + data.count)
        payload.writeInteger(0xAE as UInt8)
        payload.writeInteger(featureID)
        payload.writeInteger(UInt32(data.count), endianness: .little)
        payload.writeBytes(data)
        payload.writeInteger(0xFF as UInt8)
        return payload
    }

    static func returnStatusReturnValueAndDonePayload() -> ByteBuffer {
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

    static func errorPayload(
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

    static func infoPayload(
        message: String,
        number: Int32 = 0,
        severity: UInt8 = 0
    ) -> ByteBuffer {
        var payload = Self.errorPayload(message: message, number: number, severity: severity)
        payload.setInteger(0xAB as UInt8, at: payload.readerIndex)
        return payload
    }

    static func donePayload(
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

    static func doneInProcPayload(
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

    static func transactionDescriptorEnvChangePayload(
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

    static func selectOneTokenStreamPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 128)
        var metadata = Self.selectOneMetadataPayload()
        var row = Self.selectOneRowPayload()
        payload.writeBuffer(&metadata)
        payload.writeBuffer(&row)

        var done = Self.donePayload()
        payload.writeBuffer(&done)
        return payload
    }

    static func doneInProcFirstResultSetPayload() -> ByteBuffer {
        var payload = ByteBufferAllocator().buffer(capacity: 128)
        var metadata = Self.selectOneMetadataPayload()
        var row = Self.selectOneRowPayload()
        var doneInProc = Self.doneInProcPayload(status: .count, rowCount: 1)
        payload.writeBuffer(&metadata)
        payload.writeBuffer(&row)
        payload.writeBuffer(&doneInProc)
        return payload
    }

    static func selectOneMetadataPayload() -> ByteBuffer {
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

    static func selectOneRowPayload(
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

    static func optionalMetadataTokenStreamPayload() -> ByteBuffer {
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

    static func sessionStatePayload(
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

    static func multiResultSetTokenStreamPayload() -> ByteBuffer {
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

    static func nbcRowTokenStreamPayload() -> ByteBuffer {
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

    static func dataClassificationTokenStreamPayload() -> ByteBuffer {
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

    static func altMetadataTokenStreamPayload() -> ByteBuffer {
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

    static func legacyCharBinaryTokenStreamPayload() -> ByteBuffer {
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

    static func plpMaxTokenStreamPayload() -> ByteBuffer {
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

    static func xmlTokenStreamPayload() -> ByteBuffer {
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

    static func jsonTokenStreamPayload() -> ByteBuffer {
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

    static func nullTypeTokenStreamPayload() -> ByteBuffer {
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

    static func sqlVariantTokenStreamPayload() -> ByteBuffer {
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

    static func udtTokenStreamPayload() -> ByteBuffer {
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

    static func legacyLOBTokenStreamPayload() -> ByteBuffer {
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

    static func decimalTokenStreamPayload() -> ByteBuffer {
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

    static func temporalTokenStreamPayload() -> ByteBuffer {
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

    static func legacyTemporalMoneyTokenStreamPayload() -> ByteBuffer {
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

    static func guidTokenStreamPayload() -> ByteBuffer {
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

    static func configuration() -> TDSConnection.Configuration {
        TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            database: "master",
            tls: .disable,
            clientHostName: "client"
        )
    }

    static func loggedInChannel(
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
        if configuration.options.startupInitialSQL != nil {
            _ = try channel.readOutbound(as: ByteBuffer.self)
            try channel.writeInbound(
                Self.packet(
                    type: .preloginLoginOrTablularResponse,
                    payload: Self.donePayload()
                ))
        }
        _ = try eventHandler.startupDoneFuture.wait()
        return channel
    }

    static func readyForQueryEventCount(in events: [Any]) -> Int {
        events.filter {
            if case TDSSQLEvent.readyForQuery = $0 {
                return true
            }
            return false
        }.count
    }

    static func resetConnectionEventCount(in events: [Any]) -> Int {
        events.filter {
            if case TDSSQLEvent.resetConnection = $0 {
                return true
            }
            return false
        }.count
    }

    static func loginStringField(index: Int, in packet: inout ByteBuffer) throws -> String {
        let loginStart = TDSPacket.headerLength
        let entry = loginStart + 36 + index * 4
        let offset = try requireUnwrap(
            packet.getInteger(
                at: entry,
                endianness: .little,
                as: UInt16.self
            ))
        let length = try requireUnwrap(
            packet.getInteger(
                at: entry + 2,
                endianness: .little,
                as: UInt16.self
            ))
        var field = try requireUnwrap(
            packet.getSlice(
                at: loginStart + Int(offset),
                length: Int(length) * 2
            ))
        return try requireUnwrap(field.readUTF16(characterCount: Int(length)))
    }

    static func loginPasswordBytes(_ password: String) -> [UInt8] {
        password.utf16.flatMap { codeUnit -> [UInt8] in
            let swapped = ((codeUnit << 4) & 0xF0F0) | ((codeUnit >> 4) & 0x0F0F)
            let encoded = swapped ^ 0xA5A5
            return [UInt8(encoded & 0x00FF), UInt8(encoded >> 8)]
        }
    }
}

extension ByteBuffer {
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
        self.writeLittleEndianUnsignedInteger(
            UInt64(Self.daysSince0001(year: year, month: month, day: day)), byteCount: 3)
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
        let monthLengths =
            Self.isLeapYear(year)
            ? [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] : [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
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
