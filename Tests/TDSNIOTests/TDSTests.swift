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
        encoder.rpc(
            .init(
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
        encoder.rpc(
            .init(
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

    func testRPCResultIncludesReturnStatusAndOutputParameters() throws {
        let channel = try Self.loggedInChannel()

        let rpcPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(
            TDSTask.rpc(
                .init(
                    procedure: "dbo.answer",
                    parameters: [.init(name: "@answer", value: .int(0))]
                ),
                rpcPromise
            ))
        let rpc: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(rpc.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)

        try channel.writeInbound(
            Self.packet(
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
}
