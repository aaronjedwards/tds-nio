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
    @Test func typedNullBindingsDeclareAndEncodeSQLTypes() throws {
        var bindings = TDSBindings()
        bindings.append(.typedNull(.tinyInt), name: "@tiny")
        bindings.append(.typedNull(.smallInt), name: "@small")
        bindings.append(.typedNull(.int), name: "@integer")
        bindings.append(.typedNull(.bigInt), name: "@id")
        bindings.append(.typedNull(.nvarchar(maxBytes: 41)), name: "@label")
        bindings.append(.typedNull(.decimal(precision: 9, scale: 4)), name: "@amount")
        expectEqual(
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

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@tiny")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 1)
        expectEqual(packet.readInteger(as: UInt8.self), 0)

        expectEqual(packet.readInteger(as: UInt8.self), 6)
        expectEqual(packet.readUTF16(characterCount: 6), "@small")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 2)
        expectEqual(packet.readInteger(as: UInt8.self), 0)

        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readUTF16(characterCount: 8), "@integer")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readInteger(as: UInt8.self), 0)

        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readUTF16(characterCount: 3), "@id")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(as: UInt8.self), 0)

        expectEqual(packet.readInteger(as: UInt8.self), 6)
        expectEqual(packet.readUTF16(characterCount: 6), "@label")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 40)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        expectEqual(packet.readInteger(as: UInt8.self), 7)
        expectEqual(packet.readUTF16(characterCount: 7), "@amount")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.decimalN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 17)
        expectEqual(packet.readInteger(as: UInt8.self), 9)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func typedNullBindingsDeclareAndEncodeCharacterAndBinarySQLTypes() throws {
        var bindings = TDSBindings()
        bindings.append(.typedNull(.char(maxBytes: 3)), name: "@char")
        bindings.append(.typedNull(.varchar(maxBytes: 12)), name: "@varchar")
        bindings.append(.typedNull(.varchar()), name: "@varcharMax")
        bindings.append(.typedNull(.nchar(maxBytes: 6)), name: "@nchar")
        bindings.append(.typedNull(.binary(maxBytes: 4)), name: "@binary")
        expectEqual(
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

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@char")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 3)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readUTF16(characterCount: 8), "@varchar")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 12)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        expectEqual(packet.readInteger(as: UInt8.self), 11)
        expectEqual(packet.readUTF16(characterCount: 11), "@varcharMax")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), UInt64.max)

        expectEqual(packet.readInteger(as: UInt8.self), 6)
        expectEqual(packet.readUTF16(characterCount: 6), "@nchar")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 6)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        expectEqual(packet.readInteger(as: UInt8.self), 7)
        expectEqual(packet.readUTF16(characterCount: 7), "@binary")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigBinary.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 4)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func rpcResultIncludesReturnStatusAndOutputParameters() throws {
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
        let rpc: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(rpc.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.returnStatusReturnValueAndDonePayload()
            ))

        let result = try rpcPromise.futureResult.wait()
        expectEqual(result.returnStatus, 7)
        expectEqual(result.outputParameters.count, 1)
        expectEqual(result.outputParameters[0].ordinal, 1)
        expectEqual(result.outputParameters[0].name, "answer")
        expectEqual(result.outputParameters[0].dataType, .intN)
        expectEqual(result.outputParameters[0].metadata.length, 4)
        expectEqual(result.outputParameters[0].value, .int32(42))
        expectEqual(result.outputParameter(at: 1)?.name, "answer")
        expectEqual(result.outputParameter(named: "@answer")?.value, .int32(42))
        expectEqual(result.outputParameter(named: "answer")?.value, .int32(42))

        let answer: Int32 = try result.decodeOutputParameter(named: "@answer")
        expectEqual(answer, 42)
        expectThrowsError(try result.decodeOutputParameter(named: "missing", as: Int.self)) { error in
            expectEqual((error as? TDSDecodingError)?.code, .missingOutputParameter("missing"))
        }
    }
}
