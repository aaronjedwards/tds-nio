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
    @Test func preloginPacketIsEncodedWithTDSHeader() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )

        encoder.prelogin(encryption: .encryptOn)
        var packet = encoder.flush()

        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.prelogin.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), 1)
        expectEqual(packet.readInteger(as: UInt8.self), 0)

        var options: [(UInt8, UInt16, UInt16)] = []
        while let token = packet.readInteger(as: UInt8.self), token != 0xFF {
            let offset = try requireUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            let length = try requireUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            options.append((token, offset, length))
        }

        expectEqual(options.map(\.0), [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06])
        expectEqual(options.map(\.1), [0x0024, 0x002A, 0x002B, 0x002C, 0x0030, 0x0031, 0x0055])
        expectEqual(options.map(\.2), [6, 1, 1, 4, 1, 36, 1])
        expectEqual(
            packet.getBytes(at: TDSPacket.headerLength + Int(options[0].1), length: 6),
            [
                0x09, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        expectEqual(packet.getInteger(at: TDSPacket.headerLength + Int(options[1].1), as: UInt8.self), 0x01)
        expectEqual(packet.getInteger(at: TDSPacket.headerLength + Int(options[2].1), as: UInt8.self), 0x00)
        expectEqual(packet.getInteger(at: TDSPacket.headerLength + Int(options[4].1), as: UInt8.self), 0x00)
        expectEqual(packet.getBytes(at: TDSPacket.headerLength + Int(options[5].1), length: 36)?.count, 36)
        expectEqual(packet.getInteger(at: TDSPacket.headerLength + Int(options[6].1), as: UInt8.self), 0x01)
    }

    @Test func preloginPacketOffsetsAreDynamicWhenEncryptionIsOmitted() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )

        encoder.prelogin(encryption: nil)
        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength)

        var options: [(UInt8, UInt16, UInt16)] = []
        while let token = packet.readInteger(as: UInt8.self), token != 0xFF {
            let offset = try requireUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            let length = try requireUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            options.append((token, offset, length))
        }

        expectEqual(options.map(\.0), [0x00, 0x02, 0x03, 0x04, 0x05, 0x06])
        expectEqual(options.map(\.1), [0x001F, 0x0025, 0x0026, 0x002A, 0x002B, 0x004F])
        expectEqual(options.map(\.2), [6, 1, 4, 1, 36, 1])
        expectEqual(
            packet.getBytes(at: TDSPacket.headerLength + Int(options[0].1), length: 6),
            [
                0x09, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
    }

    @Test func loginPacketEncodesTDS74FeatureExtensions() throws {
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

        expectEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.tds7Login.rawValue)
        expectEqual(
            packet.getInteger(at: loginStart, endianness: .little, as: UInt32.self),
            UInt32(packet.writerIndex - loginStart))
        expectEqual(
            packet.getInteger(at: loginStart + 4, endianness: .little, as: UInt32.self),
            TDSProtocolVersion.v7_4.wireValue)
        expectEqual(
            packet.getInteger(at: loginStart + 8, endianness: .little, as: UInt32.self),
            UInt32(configuration.packetSize))
        expectEqual(packet.getInteger(at: loginStart + 24, as: UInt8.self), 0xE0)
        expectEqual(packet.getInteger(at: loginStart + 25, as: UInt8.self), 0x00)
        expectEqual(packet.getInteger(at: loginStart + 26, as: UInt8.self), 0x00)
        expectEqual(packet.getInteger(at: loginStart + 27, as: UInt8.self), 0x10)

        let extensionEntry = loginStart + 36 + 5 * 4
        let extensionOffset = try requireUnwrap(
            packet.getInteger(at: extensionEntry, endianness: .little, as: UInt16.self))
        let extensionLength = try requireUnwrap(
            packet.getInteger(at: extensionEntry + 2, endianness: .little, as: UInt16.self))
        expectEqual(extensionLength, 4)

        let featureExtOffset = try requireUnwrap(
            packet.getInteger(
                at: loginStart + Int(extensionOffset),
                endianness: .little,
                as: UInt32.self
            ))
        var featureExt = try requireUnwrap(
            packet.getSlice(
                at: loginStart + Int(featureExtOffset),
                length: packet.writerIndex - (loginStart + Int(featureExtOffset))
            ))

        expectEqual(featureExt.readInteger(as: UInt8.self), 0x0A)
        expectEqual(featureExt.readInteger(endianness: .little, as: UInt32.self), 1)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x01)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0xFF)
        expectEqual(featureExt.readableBytes, 0)
    }

    @Test func loginPacketEncodesFederatedAuthenticationFeatureExtension() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        encoder.setFederatedAuthenticationEchoRequired(true)
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "user@example.test",
            password: "Secret123!",
            tls: .disable,
            authentication: .federatedAuthentication(workflow: .integrated)
        )

        encoder.login(configuration: configuration)
        var featureExt = try Self.loginFeatureExtSlice(from: encoder.flush())

        expectEqual(featureExt.readInteger(as: UInt8.self), 0x02)
        expectEqual(featureExt.readInteger(endianness: .little, as: UInt32.self), 2)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x05)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x02)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x0A)
        expectEqual(featureExt.readInteger(endianness: .little, as: UInt32.self), 1)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x01)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0xFF)
        expectEqual(featureExt.readableBytes, 0)
    }

    @Test func loginPacketEncodesFederatedAuthenticationTokenFeatureExtension() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "user@example.test",
            password: "Secret123!",
            tls: .disable,
            authentication: .federatedAuthenticationToken("abc")
        )

        encoder.login(configuration: configuration)
        var featureExt = try Self.loginFeatureExtSlice(from: encoder.flush())

        expectEqual(featureExt.readInteger(as: UInt8.self), 0x02)
        expectEqual(featureExt.readInteger(endianness: .little, as: UInt32.self), 11)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x02)
        expectEqual(featureExt.readInteger(endianness: .little, as: UInt32.self), 6)
        expectEqual(featureExt.readBytes(length: 6), [0x61, 0x00, 0x62, 0x00, 0x63, 0x00])
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x0A)
        expectEqual(featureExt.readInteger(endianness: .little, as: UInt32.self), 1)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x01)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0xFF)
        expectEqual(featureExt.readableBytes, 0)
    }

    @Test func startupPipelineEchoesPreloginFedAuthRequiredInLoginFeatureExtension() throws {
        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds-nio-tests")
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "user@example.test",
            password: "Secret123!",
            tls: .disable,
            authentication: .federatedAuthentication()
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
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff, fedAuthRequired: true)
            ))

        let login = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        var featureExt = try Self.loginFeatureExtSlice(from: login)

        expectEqual(featureExt.readInteger(as: UInt8.self), 0x02)
        expectEqual(featureExt.readInteger(endianness: .little, as: UInt32.self), 2)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x05)
        expectEqual(featureExt.readInteger(as: UInt8.self), 0x01)
    }

    @Test func loginPacketUsesConfiguredClampedPacketSize() throws {
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
        expectEqual(configuration.packetSize, TDSPacket.maximumNegotiatedPacketLength)
        configuration.packetSize = 8

        encoder.login(configuration: configuration)
        let packet = encoder.flush()
        let loginStart = TDSPacket.headerLength

        expectEqual(configuration.packetSize, TDSPacket.minimumPacketLength)
        expectEqual(
            packet.getInteger(at: loginStart + 8, endianness: .little, as: UInt32.self),
            UInt32(TDSPacket.minimumPacketLength)
        )
    }

    @Test func loginPacketEncodesReadOnlyApplicationIntent() throws {
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

        expectEqual(packet.getInteger(at: loginStart + 26, as: UInt8.self), 0x20)
        expectEqual(packet.getInteger(at: loginStart + 27, as: UInt8.self), 0x10)
    }

    @Test func loginPacketEncodesSSPIAuthenticationMode() throws {
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
        let sspiOffset = try requireUnwrap(
            packet.getInteger(
                at: sspiEntry,
                endianness: .little,
                as: UInt16.self
            ))
        let sspiLength = try requireUnwrap(
            packet.getInteger(
                at: sspiEntry + 2,
                endianness: .little,
                as: UInt16.self
            ))
        let sspiLongLength = try requireUnwrap(
            packet.getInteger(
                at: sspiEntry + 12,
                endianness: .little,
                as: UInt32.self
            ))

        expectEqual(packet.getInteger(at: loginStart + 25, as: UInt8.self), 0x80)
        expectEqual(try Self.loginStringField(index: 1, in: &packet), "")
        expectEqual(try Self.loginStringField(index: 2, in: &packet), "")
        expectEqual(sspiLength, UInt16(initialToken.count))
        expectEqual(sspiLongLength, UInt32(initialToken.count))
        expectEqual(packet.getBytes(at: loginStart + Int(sspiOffset), length: initialToken.count), initialToken)
    }

    @Test func loginPacketEncodesInitialLanguage() throws {
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

        expectEqual(try Self.loginStringField(index: 7, in: &packet), "us_english")
        expectEqual(try Self.loginStringField(index: 8, in: &packet), "master")
    }

    @Test func loginPacketBoundsOversizedStringFields() throws {
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
        let languageOffset = try requireUnwrap(
            packet.getInteger(
                at: languageEntry,
                endianness: .little,
                as: UInt16.self
            ))
        let languageLength = try requireUnwrap(
            packet.getInteger(
                at: languageEntry + 2,
                endianness: .little,
                as: UInt16.self
            ))

        let expectedLength = UInt16((Int(UInt16.max) - Int(languageOffset)) / 2)
        expectEqual(languageLength, expectedLength)
        expectEqual(
            packet.getBytes(at: loginStart + Int(languageOffset), length: 2),
            [0x78, 0x00]
        )
        expectEqual(
            packet.getBytes(at: loginStart + Int(languageOffset) + (Int(languageLength) - 1) * 2, length: 2),
            [0x78, 0x00]
        )
        expectEqual(
            packet.getInteger(at: 2, endianness: .big, as: UInt16.self),
            UInt16.max
        )
    }

    @Test func loginPacketObfuscatesPasswordField() throws {
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
        let passwordOffset = try requireUnwrap(
            packet.getInteger(
                at: passwordEntry,
                endianness: .little,
                as: UInt16.self
            ))
        let passwordLength = try requireUnwrap(
            packet.getInteger(
                at: passwordEntry + 2,
                endianness: .little,
                as: UInt16.self
            ))

        expectEqual(passwordLength, UInt16(configuration.password.utf16.count))
        let encodedPassword = try requireUnwrap(
            packet.getBytes(
                at: loginStart + Int(passwordOffset),
                length: Int(passwordLength) * 2
            ))
        expectEqual(encodedPassword, Self.loginPasswordBytes(configuration.password))
        expectNotEqual(
            encodedPassword,
            Array(configuration.password.utf16).flatMap {
                [UInt8($0 & 0x00FF), UInt8($0 >> 8)]
            })
    }

    @Test func capabilitiesTrackLoginAckAndFeatureExtAck() throws {
        var capabilities = Capabilities(requestedProtocolVersion: .v7_4)
        let loginAck = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: TDSProtocolVersion.v7_4.wireValue,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )

        capabilities.adjustForLoginAck(loginAck)
        capabilities.adjustForFeatureExtAck(
            .init(options: [
                .init(featureID: Capabilities.FeatureID.dataClassification.rawValue, data: [0x02]),
                .init(featureID: Capabilities.FeatureID.jsonSupport.rawValue, data: [0x01]),
                .init(featureID: Capabilities.FeatureID.utf8Support.rawValue, data: [0x00]),
                .init(featureID: 0xFE, data: [0xAA]),
            ]))

        expectEqual(capabilities.requestedProtocolVersion.description, "7.4")
        expectEqual(capabilities.negotiatedProtocolVersion?.description, "7.4")
        expectTrue(capabilities.wasAcknowledged(.dataClassification))
        expectEqual(capabilities.dataClassificationVersion, 2)
        expectTrue(capabilities.supportsJSON)
        expectFalse(capabilities.supportsUTF8)
        expectEqual(capabilities.acknowledgedFeatureExtensions[0xFE], [0xAA])
    }

    @Test func sqlBatchPacketEncodesAllHeadersAndUnicodeText() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 128)
        )

        encoder.sqlBatch("SELECT 1")

        var packet = encoder.flush()
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)

        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 22)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 18)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0x02)
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 1)
        expectEqual(packet.readUTF16(characterCount: 8), "SELECT 1")
        expectEqual(packet.readableBytes, 0)
    }

    @Test func boundQueryRPCPacketEncodesSpExecuteSQL() throws {
        let query: TDSQuery = "SELECT * FROM dbo.items WHERE id = \(42) AND label = \("forty-two")"
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 1_024)
        )

        encoder.rpc(query.rpcForExecution())

        var packet = encoder.flush()
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)
        packet.moveReaderIndex(forwardBy: 22)

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 13)
        expectEqual(packet.readUTF16(characterCount: 13), "sp_executesql")
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@stmt")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 108)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 108)
        expectEqual(
            packet.readUTF16(characterCount: 54),
            "SELECT * FROM dbo.items WHERE id = @p0 AND label = @p1"
        )

        expectEqual(packet.readInteger(as: UInt8.self), 7)
        expectEqual(packet.readUTF16(characterCount: 7), "@params")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 58)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 58)
        expectEqual(packet.readUTF16(characterCount: 29), "@p0 bigint, @p1 nvarchar(max)")

        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readUTF16(characterCount: 3), "@p0")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(endianness: .little, as: Int64.self), 42)

        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readUTF16(characterCount: 3), "@p1")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 18)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 18)
        expectEqual(packet.readUTF16(characterCount: 9), "forty-two")
        expectEqual(packet.readableBytes, 0)
    }

    @Test func boundQueryRPCPacketEncodesMaxBinaryParameterAsPLP() throws {
        let value = Array(repeating: UInt8(0xA5), count: 9_001)
        let query: TDSQuery = "SELECT \(value)"
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 10_000)
        )

        encoder.rpc(query.rpcForExecution())

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "sp_executesql".utf16.count * 2 + 2)

        self.skipRPCParameter(&packet, name: "@stmt")
        self.skipRPCParameter(&packet, name: "@params")

        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readUTF16(characterCount: 3), "@p0")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigVarBin.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), UInt64.max - 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 9_001)
        let bytes = try requireUnwrap(packet.readBytes(length: 9_001))
        expectEqual(bytes.count, 9_001)
        expectEqual(bytes.first, 0xA5)
        expectEqual(bytes.last, 0xA5)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
    }

    @Test func rpcPacketEncodesEmptyStringParameterWithValidNVarCharMetadata() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 128)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.emptyText",
                parameters: [
                    .init(name: "@text", value: .string(""))
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.emptyText".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@text")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func rpcPacketUsesDatabaseCollationForStringParameter() throws {
        let collation: [UInt8] = [0x33, 0x08, 0xD0, 0x00, 0x34]
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 128)
        )
        encoder.setDatabaseCollation(collation)
        encoder.rpc(
            .init(
                procedure: "dbo.echo",
                parameters: [
                    .init(name: "@text", value: .string("abc"))
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.echo".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@text")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 6)
        expectEqual(packet.readBytes(length: 5), collation)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 6)
        expectEqual(packet.readUTF16(characterCount: 3), "abc")
        expectEqual(packet.readableBytes, 0)
    }

    @Test func rpcPacketEncodesWidthSpecificIntegerParameters() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.widths",
                parameters: [
                    .init(name: "@tiny", value: .tinyInt(255)),
                    .init(name: "@small", value: .smallInt(-123)),
                    .init(name: "@integer", value: .int32(123_456)),
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.widths".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@tiny")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 1)
        expectEqual(packet.readInteger(as: UInt8.self), 1)
        expectEqual(packet.readInteger(as: UInt8.self), 255)

        expectEqual(packet.readInteger(as: UInt8.self), 6)
        expectEqual(packet.readUTF16(characterCount: 6), "@small")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 2)
        expectEqual(packet.readInteger(as: UInt8.self), 2)
        expectEqual(packet.readInteger(endianness: .little, as: Int16.self), -123)

        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readUTF16(characterCount: 8), "@integer")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readInteger(endianness: .little, as: Int32.self), 123_456)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func rpcPacketEncodesProcedureAndParameters() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.echo",
                parameters: [
                    .init(name: "@id", value: .int(42)),
                    .init(name: "@label", value: .string("forty-two")),
                ]
            ))

        var packet = encoder.flush()
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(packet.writerIndex))
        packet.moveReaderIndex(forwardBy: 4)

        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 22)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 18)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0x02)
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 1)

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 8)
        expectEqual(packet.readUTF16(characterCount: 8), "dbo.echo")
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)

        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readUTF16(characterCount: 3), "@id")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(endianness: .little, as: Int64.self), 42)

        expectEqual(packet.readInteger(as: UInt8.self), 6)
        expectEqual(packet.readUTF16(characterCount: 6), "@label")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 18)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 18)
        expectEqual(packet.readUTF16(characterCount: 9), "forty-two")
    }

    @Test func rpcPacketEncodesOutputParameterStatus() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.answer",
                parameters: [
                    .init(name: "@answer", value: .int(0), isOutput: true)
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.answer".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 7)
        expectEqual(packet.readUTF16(characterCount: 7), "@answer")
        expectEqual(packet.readInteger(as: UInt8.self), 0x01)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(as: UInt8.self), 8)
        expectEqual(packet.readInteger(endianness: .little, as: Int64.self), 0)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func rpcPacketPrefixesBareParameterNames() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.echo",
                parameters: [
                    .init(name: "id", value: .int(42)),
                    .init(name: "", value: .int(7)),
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22)

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 8)
        expectEqual(packet.readUTF16(characterCount: 8), "dbo.echo")
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)

        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readUTF16(characterCount: 3), "@id")
        packet.moveReaderIndex(forwardBy: 1 + 1 + 1 + 8)

        expectEqual(packet.readInteger(as: UInt8.self), 0)
    }

    @Test func rpcPacketEncodesDecimalParameter() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.money",
                parameters: [
                    .init(name: "@amount", value: .decimal("123.45"))
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.money".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 7)
        expectEqual(packet.readUTF16(characterCount: 7), "@amount")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.decimalN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 17)
        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readInteger(as: UInt8.self), 2)
        expectEqual(packet.readInteger(as: UInt8.self), 17)
        expectEqual(packet.readInteger(as: UInt8.self), 1)
        expectEqual(packet.readBytes(length: 2), [0x39, 0x30])
        expectEqual(packet.readBytes(length: 14), Array(repeating: 0, count: 14))
    }

    @Test func rpcPacketEncodesGUIDParameter() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.guid",
                parameters: [
                    .init(name: "@id", value: .guid(Self.guid))
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.guid".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readUTF16(characterCount: 3), "@id")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.guid.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 16)
        expectEqual(packet.readInteger(as: UInt8.self), 16)
        expectEqual(
            packet.readBytes(length: 16),
            [
                0x33, 0x22, 0x11, 0x00,
                0x55, 0x44,
                0x77, 0x66,
                0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
            ])
    }

    @Test func rpcPacketEncodesXMLParameter() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.xml",
                parameters: [
                    .init(name: "@doc", value: .xml([0x3C, 0x72, 0x2F, 0x3E]))
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.xml".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readUTF16(characterCount: 4), "@doc")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.xml.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), UInt64.max - 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 4)
        expectEqual(packet.readBytes(length: 4), [0x3C, 0x72, 0x2F, 0x3E])
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
    }

    @Test func rpcPacketEncodesJSONParameter() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.json",
                parameters: [
                    .init(name: "@doc", value: .json(Array(#"{"ok":true}"#.utf8)))
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.json".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readUTF16(characterCount: 4), "@doc")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.json.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), UInt64.max - 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 11)
        expectEqual(packet.readBytes(length: 11), Array(#"{"ok":true}"#.utf8))
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
    }

    @Test func rpcPacketEncodesLongStringParameterAsPLP() throws {
        let value = String(repeating: "x", count: 5_000)
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 12_000)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.longText",
                parameters: [
                    .init(name: "@text", value: .string(value))
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.longText".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@text")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), UInt64.max - 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 10_000)
        let bytes = try requireUnwrap(packet.readBytes(length: 10_000))
        expectEqual(bytes.count, 10_000)
        expectEqual(bytes.prefix(4), [0x78, 0x00, 0x78, 0x00])
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
    }

    @Test func rpcPacketEncodesLongBytesParameterAsPLP() throws {
        let value = Array(repeating: UInt8(0xA5), count: 9_001)
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 10_000)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.longBytes",
                parameters: [
                    .init(name: "@data", value: .bytes(value))
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.longBytes".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@data")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.bigVarBin.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)
        expectEqual(packet.readInteger(endianness: .little, as: UInt64.self), UInt64.max - 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 9_001)
        let bytes = try requireUnwrap(packet.readBytes(length: 9_001))
        expectEqual(bytes.count, 9_001)
        expectEqual(bytes.first, 0xA5)
        expectEqual(bytes.last, 0xA5)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
    }

    @Test func rpcPacketEncodesTableValuedParameter() throws {
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
        encoder.rpc(
            .init(
                procedure: "dbo.use_tvp",
                parameters: [.init(name: "@items", value: .table(tvp))]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.use_tvp".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 6)
        expectEqual(packet.readUTF16(characterCount: 6), "@items")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), 0xF3)
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), 3)
        expectEqual(packet.readUTF16(characterCount: 3), "dbo")
        expectEqual(packet.readInteger(as: UInt8.self), 13)
        expectEqual(packet.readUTF16(characterCount: 13), "IntStringList")

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readInteger(as: UInt8.self), 0)

        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 40)
        expectEqual(packet.readBytes(length: 5), [0x09, 0x04, 0xD0, 0x00, 0x34])
        expectEqual(packet.readInteger(as: UInt8.self), 0)

        expectEqual(packet.readInteger(as: UInt8.self), 0x00)
        expectEqual(packet.readInteger(as: UInt8.self), 0x01)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readInteger(endianness: .little, as: Int32.self), 7)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 10)
        expectEqual(packet.readUTF16(characterCount: 5), "seven")

        expectEqual(packet.readInteger(as: UInt8.self), 0x01)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readInteger(endianness: .little, as: Int32.self), 8)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), UInt16.max)

        expectEqual(packet.readInteger(as: UInt8.self), 0x00)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func rpcPacketEncodesTableValuedParameterWithoutDefaultSchema() throws {
        let tvp = TDSTableValuedParameter(
            typeName: "IntList",
            columns: [
                .init(dataType: .int(maxBytes: 4))
            ],
            rows: [
                [.int(7)]
            ]
        )
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.use_tvp",
                parameters: [.init(name: "@items", value: .table(tvp))]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.use_tvp".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 6)
        expectEqual(packet.readUTF16(characterCount: 6), "@items")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), 0xF3)
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), 7)
        expectEqual(packet.readUTF16(characterCount: 7), "IntList")

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 0)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.intN.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readInteger(as: UInt8.self), 0)

        expectEqual(packet.readInteger(as: UInt8.self), 0x00)
        expectEqual(packet.readInteger(as: UInt8.self), 0x01)
        expectEqual(packet.readInteger(as: UInt8.self), 4)
        expectEqual(packet.readInteger(endianness: .little, as: Int32.self), 7)
        expectEqual(packet.readInteger(as: UInt8.self), 0x00)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func rpcPacketBoundsTableValuedParameterVariableValuesToColumnMax() throws {
        let tvp = TDSTableValuedParameter(
            schemaName: "dbo",
            typeName: "BoundedList",
            columns: [
                .init(dataType: .nVarChar(maxBytes: 4)),
                .init(dataType: .varBinary(maxBytes: 3)),
            ],
            rows: [
                [.string("abcdef"), .bytes([1, 2, 3, 4, 5])]
            ]
        )
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        encoder.rpc(
            .init(
                procedure: "dbo.use_tvp",
                parameters: [.init(name: "@items", value: .table(tvp))]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.use_tvp".utf16.count * 2 + 2)
        packet.moveReaderIndex(forwardBy: 1 + "@items".utf16.count * 2 + 1 + 1)
        packet.moveReaderIndex(forwardBy: 1 + 1 + "dbo".utf16.count * 2 + 1 + "BoundedList".utf16.count * 2)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        for _ in 0..<2 {
            packet.moveReaderIndex(forwardBy: 4 + 2)
            let type = try requireUnwrap(packet.readInteger(as: UInt8.self))
            if type == TDSDataType.nVarChar.rawValue {
                packet.moveReaderIndex(forwardBy: 2 + 5)
            } else {
                packet.moveReaderIndex(forwardBy: 2)
            }
            packet.moveReaderIndex(forwardBy: 1)
        }
        expectEqual(packet.readInteger(as: UInt8.self), 0x00)
        expectEqual(packet.readInteger(as: UInt8.self), 0x01)

        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 4)
        expectEqual(packet.readUTF16(characterCount: 2), "ab")
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 3)
        expectEqual(packet.readBytes(length: 3), [1, 2, 3])
        expectEqual(packet.readInteger(as: UInt8.self), 0x00)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func attentionPacketIsEncodedWithEmptyPayload() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 32)
        )
        encoder.attention()

        var packet = encoder.flush()
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.attentionSignal.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength))
        expectEqual(packet.readableBytes, 4)
    }

    @Test func sspiPacketEncodesRawAuthenticationBytes() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 32)
        )
        encoder.sspi([0x4E, 0x54, 0x4C, 0x4D])

        var packet = encoder.flush()
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sspi.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength + 4))
        packet.moveReaderIndex(forwardBy: 4)
        expectEqual(packet.readBytes(length: 4), [0x4E, 0x54, 0x4C, 0x4D])
        expectEqual(packet.readableBytes, 0)
    }

    @Test func federatedAuthenticationPacketEncodesTokenAndNonce() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 64)
        )
        let nonce = Array(UInt8(0)..<UInt8(32))
        encoder.federatedAuthenticationToken(token: [0xAA, 0xBB, 0xCC], nonce: nonce)

        var packet = encoder.flush()
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.federatedAuthenticationToken.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength + 43))
        packet.moveReaderIndex(forwardBy: 4)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 39)
        expectEqual(packet.readInteger(endianness: .little, as: UInt32.self), 3)
        expectEqual(packet.readBytes(length: 3), [0xAA, 0xBB, 0xCC])
        expectEqual(packet.readBytes(length: 32), nonce)
        expectEqual(packet.readableBytes, 0)
    }

    @Test func authenticationTokenOutboundEventWritesContinuationPacket() throws {
        let channel = try Self.loggedInChannel()

        try channel.pipeline.triggerUserOutboundEvent(
            TDSAuthenticationToken.sspi([0x01, 0x02, 0x03])
        ).wait()

        var packet = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sspi.rawValue)
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(packet.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength + 3))
        packet.moveReaderIndex(forwardBy: 4)
        expectEqual(packet.readBytes(length: 3), [0x01, 0x02, 0x03])
    }

    @Test func federatedAuthenticationOutboundEventRejectsInvalidNonceLength() throws {
        let channel = try Self.loggedInChannel()

        expectThrowsError(
            try channel.pipeline.triggerUserOutboundEvent(
                TDSAuthenticationToken.federated(token: [0xAA], nonce: [0x01])
            ).wait()
        ) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .connectionError)
        }
        expectNil(try channel.readOutbound(as: ByteBuffer.self))
    }

    @Test func transactionManagerTaskUsesPacketTypeAndCompletesOnDone() throws {
        let channel = try Self.loggedInChannel()

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.transactionManager(.commit(), promise))

        var packet: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.transactionManagerRequest.rawValue)
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 - 1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 7)
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readableBytes, 0)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))
        let result = try promise.futureResult.wait()
        expectEqual(result.rows.count, 0)
        expectEqual(result.resultSets.count, 0)
    }

    @Test func bulkLoadPacketBoundsVariableValuesToColumnMax() throws {
        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 256)
        )
        encoder.bulkLoad(
            .init(
                columns: [
                    .init(name: "label", dataType: .nVarChar(maxBytes: 4)),
                    .init(name: "payload", dataType: .varBinary(maxBytes: 3)),
                ],
                rows: [
                    [.string("abcdef"), .bytes([1, 2, 3, 4, 5])]
                ]
            ))

        var packet = encoder.flush()
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength)
        expectEqual(packet.readInteger(as: UInt8.self), 0x81)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 2)
        for _ in 0..<2 {
            packet.moveReaderIndex(forwardBy: 4 + 2)
            let type = try requireUnwrap(packet.readInteger(as: UInt8.self))
            if type == TDSDataType.nVarChar.rawValue {
                packet.moveReaderIndex(forwardBy: 2 + 5)
            } else {
                packet.moveReaderIndex(forwardBy: 2)
            }
            let nameLength = Int(try requireUnwrap(packet.readInteger(as: UInt8.self)))
            packet.moveReaderIndex(forwardBy: nameLength * 2)
        }

        expectEqual(packet.readInteger(as: UInt8.self), 0xD1)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 4)
        expectEqual(packet.readUTF16(characterCount: 2), "ab")
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 3)
        expectEqual(packet.readBytes(length: 3), [1, 2, 3])
        expectEqual(packet.readInteger(as: UInt8.self), 0xFD)
    }

    @Test func bulkLoadTaskUsesPacketTypeAndCompletesOnDone() throws {
        let channel = try Self.loggedInChannel()

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(
            TDSTask.bulkLoad(
                .init(
                    columns: [.init(name: "id", dataType: .int)],
                    rows: [[.int(1)]]
                ), promise))

        let packet: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.bulkLoadData.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload(status: .count, rowCount: 1)
            ))
        let result = try promise.futureResult.wait()
        expectEqual(result.rowsAffected, 1)
    }

    @Test func postProcessorSplitsLargePacketsWithEOMOnlyOnFinalPacketAndResetOnlyOnFirstPacket() throws {
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

        var first = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(first.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(first.readInteger(as: UInt8.self), TDSPacket.StatusFlag.resetConnection.rawValue)
        expectEqual(first.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.maximumPacketLength))
        first.moveReaderIndex(forwardBy: 2)
        expectEqual(first.readInteger(as: UInt8.self), 1)
        first.moveReaderIndex(forwardBy: 1)
        expectEqual(first.readableBytes, TDSPacket.maximumPacketDataLength)

        var second = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(second.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(second.readInteger(as: UInt8.self), 0)
        expectEqual(second.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.maximumPacketLength))
        second.moveReaderIndex(forwardBy: 2)
        expectEqual(second.readInteger(as: UInt8.self), 2)
        second.moveReaderIndex(forwardBy: 1)
        expectEqual(second.readableBytes, TDSPacket.maximumPacketDataLength)

        var third = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(third.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(third.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(
            third.readInteger(endianness: .big, as: UInt16.self),
            UInt16(TDSPacket.headerLength + 17)
        )
        third.moveReaderIndex(forwardBy: 2)
        expectEqual(third.readInteger(as: UInt8.self), 3)
        third.moveReaderIndex(forwardBy: 1)
        expectEqual(third.readableBytes, 17)
        expectNil(try channel.readOutbound(as: ByteBuffer.self))
    }

    @Test func packetSizeEnvChangeUpdatesOutboundPacketSplitting() throws {
        let channel = try Self.loggedInChannel()

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.stringEnvChangePayload(type: 4, new: "512", old: "\(TDSPacket.maximumPacketLength)")
            ))

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT '\(String(repeating: "x", count: 700))'", queryPromise))

        var first = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(first.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(first.readInteger(as: UInt8.self), 0)
        expectEqual(first.readInteger(endianness: .big, as: UInt16.self), 512)

        var second = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(second.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(second.readInteger(as: UInt8.self), 0)
        expectEqual(second.readInteger(endianness: .big, as: UInt16.self), 512)

        var final = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(final.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(final.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectLessThan(try requireUnwrap(final.readInteger(endianness: .big, as: UInt16.self)), 512)
    }

    @Test func packetSizeEnvChangeClampsInvalidSmallValues() throws {
        let channel = try Self.loggedInChannel()

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.stringEnvChangePayload(type: 4, new: "8", old: "\(TDSPacket.maximumPacketLength)")
            ))

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT '\(String(repeating: "x", count: 600))'", queryPromise))

        var packets: [ByteBuffer] = []
        while let packet = try channel.readOutbound(as: ByteBuffer.self) {
            packets.append(packet)
        }

        expectGreaterThan(packets.count, 1)
        for index in packets.indices {
            var packet = packets[index]
            expectEqual(packet.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
            let status = try requireUnwrap(packet.readInteger(as: UInt8.self))
            let packetLength = try requireUnwrap(packet.readInteger(endianness: .big, as: UInt16.self))
            expectLessThanOrEqual(packetLength, UInt16(TDSPacket.minimumPacketLength))
            packet.moveReaderIndex(forwardBy: 4)
            expectGreaterThan(packet.readableBytes, 0)
            expectEqual(
                status & TDSPacket.StatusFlag.eom.rawValue,
                index == packets.indices.last ? TDSPacket.StatusFlag.eom.rawValue : 0
            )
        }
    }

    @Test func collationEnvChangeUpdatesLaterRPCStringMetadata() throws {
        let channel = try Self.loggedInChannel()
        let collation: [UInt8] = [0x33, 0x08, 0xD0, 0x00, 0x34]

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.collationEnvChangePayload(new: collation)
            ))

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(
            TDSTask.rpc(
                .init(
                    procedure: "dbo.echo",
                    parameters: [.init(name: "@text", value: .string("abc"))]
                ),
                promise
            ))

        var packet = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        packet.moveReaderIndex(forwardBy: TDSPacket.headerLength + 22 + 2 + "dbo.echo".utf16.count * 2 + 2)

        expectEqual(packet.readInteger(as: UInt8.self), 5)
        expectEqual(packet.readUTF16(characterCount: 5), "@text")
        expectEqual(packet.readInteger(as: UInt8.self), 0)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        expectEqual(packet.readInteger(endianness: .little, as: UInt16.self), 6)
        expectEqual(packet.readBytes(length: 5), collation)
    }

    @Test func configuredPacketSizeControlsInitialOutboundSplitting() throws {
        var configuration = Self.configuration()
        configuration.packetSize = 512
        let channel = try Self.loggedInChannel(configuration: configuration)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT '\(String(repeating: "x", count: 600))'", queryPromise))

        var first = try requireUnwrap(channel.readOutbound(as: ByteBuffer.self))
        expectEqual(first.readInteger(as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        expectEqual(first.readInteger(as: UInt8.self), 0)
        expectEqual(first.readInteger(endianness: .big, as: UInt16.self), 512)
    }

    @Test func backendDecoderDecodesPreloginResponse() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.preloginResponsePayload(encryption: .encryptOff)
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)
        try channel.writeInbound(packet)

        let containers: TinySequence<TDSBackendMessageDecoder.Container> = try requireUnwrap(
            channel.readInbound()
        )
        let container = try requireUnwrap(containers.first)
        let message = try requireUnwrap(container.messages.first)

        guard case .prelogin(let response) = message else {
            Issue.record("Expected prelogin response, got \(message)")
            return
        }
        expectEqual(response.encryption, .encryptOff)
        expectEqual(response.version?.major, 15)
    }

    @Test func backendDecoderFailsUnknownToken() throws {
        var payload = ByteBufferAllocator().buffer(capacity: 1)
        payload.writeInteger(0x01 as UInt8)
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: payload
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)

        expectThrowsError(try channel.writeInbound(packet)) { error in
            let decodingError = error as? TDSMessageDecodingError
            expectEqual(decodingError?.packetID, TDSPacket.MessageType.preloginLoginOrTablularResponse.rawValue)
            expectEqual(
                decodingError?.description,
                """
                Received a token with type '1'. There is no token type associated \
                with this token identifier.
                """
            )
        }
    }

    @Test func backendDecoderRejectsPacketLengthSmallerThanHeader() throws {
        var packet = ByteBufferAllocator().buffer(capacity: TDSPacket.headerLength)
        packet.writeInteger(TDSPacket.MessageType.preloginLoginOrTablularResponse.rawValue)
        packet.writeInteger(TDSPacket.StatusFlag.eom.rawValue)
        packet.writeInteger(UInt16(4), endianness: .big)
        packet.writeInteger(UInt16(0), endianness: .big)
        packet.writeInteger(UInt8(0))
        packet.writeInteger(UInt8(0))

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)

        expectThrowsError(try channel.writeInbound(packet)) { error in
            let decodingError = error as? TDSMessageDecodingError
            expectEqual(decodingError?.packetID, TDSPacket.MessageType.preloginLoginOrTablularResponse.rawValue)
            expectEqual(
                decodingError?.description,
                "Received a packet length of '4', expected at least '8'."
            )
        }
    }

    @Test func backendDecoderReassemblesSplitPacketsBeforeDecoding() throws {
        var payload = Self.selectOneTokenStreamPayload()
        let firstPayload = payload.readSlice(length: 18)!
        let secondPayload = payload

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                statusFlags: [],
                payload: firstPayload
            ))
        expectNil(try channel.readInbound(as: TinySequence<TDSBackendMessageDecoder.Container>.self))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: secondPayload
            ))

        var messages: [TDSBackendMessage] = []
        while let containers = try channel.readInbound(as: TinySequence<TDSBackendMessageDecoder.Container>.self) {
            for container in containers {
                messages.append(contentsOf: container.messages)
            }
        }

        expectEqual(messages.count, 3)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["id", "label"])
        guard case .row(let row) = messages[1] else {
            Issue.record("Expected ROW")
            return
        }
        expectEqual(row.values, [.int32(1), .string("one")])
        guard case .done = messages[2] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderFramesOptionalMetadataTokens() throws {
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

        expectEqual(messages.count, 11)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["id"])
        guard case .tabName(let tabName) = messages[1] else {
            Issue.record("Expected TABNAME")
            return
        }
        expectEqual(tabName.tableNames, ["dbo"])
        guard case .colInfo(let colInfo) = messages[2] else {
            Issue.record("Expected COLINFO")
            return
        }
        expectEqual(colInfo.columns.count, 1)
        expectEqual(colInfo.columns[0].columnNumber, 1)
        expectEqual(colInfo.columns[0].tableNumber, 1)
        expectTrue(colInfo.columns[0].status.contains(.differentName))
        expectEqual(colInfo.columns[0].baseColumnName, "baseId")
        guard case .order(let order) = messages[3] else {
            Issue.record("Expected ORDER")
            return
        }
        expectEqual(order.columnNumbers, [1])
        guard case .offset(let offset) = messages[4] else {
            Issue.record("Expected OFFSET")
            return
        }
        expectEqual(offset.identifier, 0x0102)
        expectEqual(offset.offset, 42)
        guard case .featureExtAck(let featureExtAck) = messages[5] else {
            Issue.record("Expected FEATUREEXTACK")
            return
        }
        expectEqual(featureExtAck.options.count, 2)
        expectEqual(featureExtAck.options[0].featureID, 0x0A)
        expectEqual(featureExtAck.options[0].data, [0x01])
        expectEqual(featureExtAck.options[1].featureID, 0x0D)
        expectEqual(featureExtAck.options[1].data, [0x01, 0x02])
        guard case .sspi(let sspi) = messages[6] else {
            Issue.record("Expected SSPI")
            return
        }
        expectEqual(sspi, [0x01, 0x00])
        guard case .sessionState(let sessionState) = messages[7] else {
            Issue.record("Expected SESSIONSTATE")
            return
        }
        expectEqual(sessionState.sequenceNumber, 1)
        expectTrue(sessionState.status.contains(.recoverable))
        expectEqual(sessionState.entries.count, 1)
        expectEqual(sessionState.entries[0].stateID, 9)
        expectEqual(sessionState.entries[0].value, [0xFF, 0xFF, 0xFF, 0xFF])
        guard case .fedAuthInfo(let fedAuthInfo) = messages[8] else {
            Issue.record("Expected FEDAUTHINFO")
            return
        }
        expectEqual(fedAuthInfo.options.map(\.id), [0x01, 0x02])
        expectEqual(fedAuthInfo.stsURL, "https://sts.example.test")
        expectEqual(fedAuthInfo.spn, "MSSQLSvc/sql.example.test:1433")
        guard case .row(let row) = messages[9] else {
            Issue.record("Expected ROW after optional metadata")
            return
        }
        expectEqual(row.values, [.int32(1)])
        guard case .done = messages[10] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesRoutingEnvChange() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.routingEnvChangePayload()
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)
        try channel.writeInbound(packet)

        let containers: TinySequence<TDSBackendMessageDecoder.Container> = try requireUnwrap(
            channel.readInbound()
        )
        let message = try requireUnwrap(containers.first?.messages.first)

        guard
            case .envChange(let envChange) = message,
            case .routing(let routing) = envChange.value
        else {
            Issue.record("Expected routing ENVCHANGE, got \(message)")
            return
        }
        expectEqual(envChange.type, 20)
        expectEqual(routing.protocolByte, 0)
        expectEqual(routing.port, 1444)
        expectEqual(routing.server, "redirect.sql.example.test")
    }

    @Test func backendDecoderPreservesUnknownEnvChangeTypes() throws {
        var envChange = ByteBufferAllocator().buffer(capacity: 8)
        envChange.writeInteger(0xFE as UInt8)
        envChange.writeInteger(0xCAFE_BABE as UInt32, endianness: .little)

        var payload = ByteBufferAllocator().buffer(capacity: 32)
        payload.writeLengthPrefixedToken(0xE3, bytes: Array(envChange.readableBytesView))
        var done = Self.donePayload()
        payload.writeBuffer(&done)

        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: payload
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)
        try channel.writeInbound(packet)

        let containers: TinySequence<TDSBackendMessageDecoder.Container> = try requireUnwrap(
            channel.readInbound()
        )
        let messages = try requireUnwrap(containers.first?.messages)
        expectEqual(messages.count, 2)
        guard
            case .envChange(let decodedEnvChange) = messages[0],
            case .unknown(var data) = decodedEnvChange.value
        else {
            Issue.record("Expected unknown ENVCHANGE before DONE, got \(messages[0])")
            return
        }
        expectEqual(decodedEnvChange.type, 0xFE)
        expectEqual(data.readInteger(endianness: .little, as: UInt32.self), 0xCAFE_BABE)
        guard case .done = messages[1] else {
            Issue.record("Expected DONE after unknown ENVCHANGE, got \(messages[1])")
            return
        }
    }

    @Test func backendDecoderRejectsUnsupportedRoutingProtocol() throws {
        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: Self.routingEnvChangePayload(protocolByte: 1)
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)

        expectThrowsError(try channel.writeInbound(packet)) { error in
            let decodingError = error as? TDSMessageDecodingError
            expectEqual(decodingError?.description, "Unsupported routing ENVCHANGE protocol byte '1'.")
        }
    }

    @Test func backendDecoderBoundsRoutingEnvChangeToNewValueLength() throws {
        var envChange = ByteBufferAllocator().buffer(capacity: 16)
        envChange.writeInteger(20 as UInt8)
        envChange.writeInteger(5 as UInt16, endianness: .little)
        envChange.writeInteger(0 as UInt8)
        envChange.writeInteger(1444 as UInt16, endianness: .little)
        envChange.writeInteger(1 as UInt16, endianness: .little)
        envChange.writeInteger(0 as UInt16, endianness: .little)

        var payload = ByteBufferAllocator().buffer(capacity: 32)
        payload.writeLengthPrefixedToken(0xE3, bytes: Array(envChange.readableBytesView))

        let packet = Self.packet(
            type: .preloginLoginOrTablularResponse,
            payload: payload
        )

        let decoder = ByteToMessageHandler(TDSBackendMessageDecoder())
        let channel = EmbeddedChannel(handler: decoder)

        expectThrowsError(try channel.writeInbound(packet)) { error in
            let decodingError = error as? TDSMessageDecodingError
            expectEqual(decodingError?.description, "Could not read 'EnvChange' from ByteBuffer.")
        }
    }

    @Test func backendDecoderDecodesReturnStatusAndReturnValue() throws {
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

        expectEqual(messages.count, 3)
        guard case .returnStatus(let status) = messages[0] else {
            Issue.record("Expected RETURNSTATUS")
            return
        }
        expectEqual(status, 7)

        guard case .returnValue(let value) = messages[1] else {
            Issue.record("Expected RETURNVALUE")
            return
        }
        expectEqual(value.ordinal, 1)
        expectEqual(value.name, "answer")
        expectEqual(value.status, 1)
        expectEqual(value.typeInfo.dataType, .intN)
        expectEqual(value.typeInfo.length, 4)
        expectEqual(value.value, .int32(42))

        guard case .done = messages[2] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesDataClassificationToken() throws {
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

        expectEqual(messages.count, 5)
        guard case .featureExtAck(let featureExtAck) = messages[0] else {
            Issue.record("Expected FEATUREEXTACK")
            return
        }
        expectEqual(featureExtAck.options.first?.featureID, 0x09)
        expectEqual(featureExtAck.options.first?.data, [0x02, 0x01])
        guard case .colMetadata(let metadata) = messages[1] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["amount"])
        guard case .dataClassification(let dataClassification) = messages[2] else {
            Issue.record("Expected DATACLASSIFICATION")
            return
        }
        expectEqual(
            dataClassification.labels,
            [
                .init(name: "Confidential", id: "label-id")
            ])
        expectEqual(
            dataClassification.informationTypes,
            [
                .init(name: "Financial", id: "info-id")
            ])
        expectEqual(dataClassification.columns.count, 1)
        expectEqual(dataClassification.columns[0].properties.count, 1)
        expectEqual(dataClassification.columns[0].properties[0].labelIndex, 0)
        expectEqual(dataClassification.columns[0].properties[0].informationTypeIndex, 0)
        expectEqual(dataClassification.columns[0].properties[0].rank, 10)
        guard case .row(let row) = messages[3] else {
            Issue.record("Expected ROW after DATACLASSIFICATION")
            return
        }
        expectEqual(row.values, [.int32(42)])
        guard case .done = messages[4] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesPLPMaxValues() throws {
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

        expectEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["text", "blob"])
        guard case .row(let firstRow) = messages[1] else {
            Issue.record("Expected first ROW")
            return
        }
        expectEqual(firstRow.values, [.string("hello world"), .bytes([0xDE, 0xAD, 0xBE, 0xEF])])
        guard case .row(let secondRow) = messages[2] else {
            Issue.record("Expected second ROW")
            return
        }
        expectEqual(secondRow.values, [.null, .null])
        guard case .done = messages[3] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesXMLValues() throws {
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

        expectEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["doc", "typedDoc"])
        expectNil(metadata.columns[0].typeInfo.xmlInfo)
        expectEqual(
            metadata.columns[1].typeInfo.xmlInfo,
            .init(
                databaseName: "master",
                owningSchema: "dbo",
                schemaCollection: "docSchema"
            ))
        guard case .row(let firstRow) = messages[1] else {
            Issue.record("Expected first ROW")
            return
        }
        expectEqual(firstRow.values, [.xml([0x3C, 0x72, 0x2F, 0x3E]), .xml([0x01, 0x02, 0x03])])
        guard case .row(let secondRow) = messages[2] else {
            Issue.record("Expected second ROW")
            return
        }
        expectEqual(secondRow.values, [.null, .null])
        guard case .done = messages[3] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesJSONValues() throws {
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

        expectEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["doc"])
        expectEqual(metadata.columns[0].typeInfo.dataType, .json)
        guard case .row(let firstRow) = messages[1] else {
            Issue.record("Expected first ROW")
            return
        }
        expectEqual(firstRow.values, [.json(Array(#"{"ok":true}"#.utf8))])
        guard case .row(let secondRow) = messages[2] else {
            Issue.record("Expected second ROW")
            return
        }
        expectEqual(secondRow.values, [.null])
        guard case .done = messages[3] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesNullTypeValues() throws {
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

        expectEqual(messages.count, 3)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["nothing"])
        expectEqual(metadata.columns[0].typeInfo.dataType, .null)
        guard case .row(let row) = messages[1] else {
            Issue.record("Expected ROW")
            return
        }
        expectEqual(row.values, [.null])
        guard case .done = messages[2] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesSQLVariantValues() throws {
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

        expectEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["variant"])
        expectEqual(metadata.columns[0].typeInfo.dataType, .sqlVariant)
        guard case .row(let firstRow) = messages[1] else {
            Issue.record("Expected first ROW")
            return
        }
        expectEqual(firstRow.values, [.int32(42)])
        guard case .row(let secondRow) = messages[2] else {
            Issue.record("Expected second ROW")
            return
        }
        expectEqual(secondRow.values, [.string("variant")])
        guard case .done = messages[3] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesUDTValues() throws {
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

        expectEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["location"])
        expectEqual(metadata.columns[0].typeInfo.dataType, .udt)
        expectEqual(metadata.columns[0].typeInfo.length, UInt64(UInt16.max))
        expectEqual(metadata.columns[0].typeInfo.udtInfo?.databaseName, "master")
        expectEqual(metadata.columns[0].typeInfo.udtInfo?.schemaName, "sys")
        expectEqual(metadata.columns[0].typeInfo.udtInfo?.typeName, "geography")
        expectEqual(
            metadata.columns[0].typeInfo.udtInfo?.assemblyQualifiedName, "Microsoft.SqlServer.Types.SqlGeography")
        guard case .row(let firstRow) = messages[1] else {
            Issue.record("Expected first ROW")
            return
        }
        expectEqual(firstRow.values, [.bytes([0xE6, 0x10, 0x00, 0x01])])
        guard case .row(let secondRow) = messages[2] else {
            Issue.record("Expected second ROW")
            return
        }
        expectEqual(secondRow.values, [.null])
        guard case .done = messages[3] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesLegacyCharAndBinaryValues() throws {
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

        expectEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["varchar", "char", "varbinary", "binary"])
        guard case .row(let firstRow) = messages[1] else {
            Issue.record("Expected first ROW")
            return
        }
        expectEqual(
            firstRow.values, [.string("hello"), .string("abc"), .bytes([0xDE, 0xAD]), .bytes([0xBE, 0xEF])])
        guard case .row(let secondRow) = messages[2] else {
            Issue.record("Expected second ROW")
            return
        }
        expectEqual(secondRow.values, [.null, .string("xyz"), .null, .bytes([0x12, 0x34])])
        guard case .done = messages[3] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesLegacyLOBValues() throws {
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

        expectEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["body", "unicodeBody", "picture"])
        guard case .row(let firstRow) = messages[1] else {
            Issue.record("Expected first ROW")
            return
        }
        expectEqual(firstRow.values, [.string("hello text"), .string("wide text"), .bytes([0xCA, 0xFE])])
        guard case .row(let secondRow) = messages[2] else {
            Issue.record("Expected second ROW")
            return
        }
        expectEqual(secondRow.values, [.null, .null, .null])
        guard case .done = messages[3] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesDecimalValues() throws {
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

        expectEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["amount"])
        guard case .row(let firstRow) = messages[1] else {
            Issue.record("Expected first ROW")
            return
        }
        expectEqual(firstRow.values, [.decimal("123.45")])
        guard case .row(let secondRow) = messages[2] else {
            Issue.record("Expected second ROW")
            return
        }
        expectEqual(secondRow.values, [.decimal("-1.23")])
        guard case .done = messages[3] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func backendDecoderDecodesGUIDValues() throws {
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

        expectEqual(messages.count, 4)
        guard case .colMetadata(let metadata) = messages[0] else {
            Issue.record("Expected COLMETADATA")
            return
        }
        expectEqual(metadata.columns.map(\.name), ["id"])
        guard case .row(let firstRow) = messages[1] else {
            Issue.record("Expected first ROW")
            return
        }
        expectEqual(firstRow.values, [.guid(Self.guid)])
        guard case .row(let secondRow) = messages[2] else {
            Issue.record("Expected second ROW")
            return
        }
        expectEqual(secondRow.values, [.null])
        guard case .done = messages[3] else {
            Issue.record("Expected DONE")
            return
        }
    }

    @Test func startupPipelineSendsPreloginLoginAndFiresStartupDone() throws {
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

        let prelogin: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(prelogin.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.prelogin.rawValue)
        var preloginOptions = prelogin
        preloginOptions.moveReaderIndex(forwardBy: TDSPacket.headerLength)
        var encryptionOffset: UInt16?
        while let token = preloginOptions.readInteger(as: UInt8.self), token != 0xFF {
            let offset = try requireUnwrap(preloginOptions.readInteger(endianness: .big, as: UInt16.self))
            let _: UInt16 = try requireUnwrap(preloginOptions.readInteger(endianness: .big, as: UInt16.self))
            if token == 0x01 {
                encryptionOffset = offset
            }
        }
        let offset = try requireUnwrap(encryptionOffset)
        expectEqual(
            prelogin.getInteger(at: TDSPacket.headerLength + Int(offset), as: UInt8.self),
            TDSFrontendMessageEncoder.PreloginEncryption.encryptNotSup.rawValue
        )

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))

        let login: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(login.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.tds7Login.rawValue)
        expectEqual(login.getInteger(at: 2, endianness: .big, as: UInt16.self), UInt16(login.writerIndex))

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.loginAckAndDonePayload()
            ))
        let initialSQL: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(initialSQL.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))

        let context = try eventHandler.startupDoneFuture.wait()
        expectEqual(context.version, .v7_4)
        expectEqual(context.sessionID, 0)
        expectEqual(context.serialNumber, 0)

        let queryPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(TDSTask.sqlBatch("SELECT 1", queryPromise))
        let sqlBatch: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(sqlBatch.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))
        let result = try queryPromise.futureResult.wait()
        expectEqual(result.columns.map(\.name), ["id", "label"])
        expectEqual(result.rows.count, 1)
        expectEqual(result.rows[0].values, [.int32(1), .string("one")])
        expectEqual(result.rows[0]["label"], .string("one"))

        let rpcPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(
            TDSTask.rpc(
                .init(procedure: "dbo.echo", parameters: [.init(name: "@id", value: .int(1))]),
                rpcPromise
            ))
        let rpc: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(rpc.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))
        let rpcResult = try rpcPromise.futureResult.wait()
        expectEqual(rpcResult.rows[0]["id"], .int32(1))
    }

    @Test func startupPipelineFailsStartupFutureOnLoginError() throws {
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
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)
        expectThrowsError(
            try channel.writeInbound(
                Self.packet(
                    type: .preloginLoginOrTablularResponse,
                    payload: Self.errorPayload(message: "Login failed for user 'sa'.", number: 18456)
                ))
        ) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .server)
            expectEqual(sqlError?.serverInfo?.number, 18456)
        }

        expectThrowsError(try eventHandler.startupDoneFuture.wait()) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .server)
            expectEqual(sqlError?.serverInfo?.number, 18456)
        }
    }

    @Test func startupPipelineAllowsLoginAckAfterLoginError() throws {
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
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        var loginResponse = Self.errorPayload(message: "Retrying login.", number: 18456)
        var loginAckAndDone = Self.loginAckAndDonePayload()
        loginResponse.writeBuffer(&loginAckAndDone)

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: loginResponse
            ))
        let initialSQL: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(initialSQL.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.sqlBatch.rawValue)
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))

        let context = try eventHandler.startupDoneFuture.wait()
        expectEqual(context.version, .v7_4)
    }

    @Test func startupPipelineFailsStartupFutureOnUnsupportedLoginAck() throws {
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
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        expectThrowsError(
            try channel.writeInbound(
                Self.packet(
                    type: .preloginLoginOrTablularResponse,
                    payload: Self.loginAckAndDonePayload(tdsVersion: 0x7100_0001)
                ))
        ) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .connectionError)
        }

        expectThrowsError(try eventHandler.startupDoneFuture.wait()) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .connectionError)
        }
    }

    @Test func startupPipelineFailsStartupFutureOnLoginDoneWithoutLoginAck() throws {
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
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        expectThrowsError(
            try channel.writeInbound(
                Self.packet(
                    type: .preloginLoginOrTablularResponse,
                    payload: Self.donePayload()
                ))
        ) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .connectionError)
        }

        expectThrowsError(try eventHandler.startupDoneFuture.wait()) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .connectionError)
        }
    }

    @Test func startupPipelineFailsStartupFutureOnUnexpectedFedAuthFeatureAck() throws {
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
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        expectThrowsError(
            try channel.writeInbound(
                Self.packet(
                    type: .preloginLoginOrTablularResponse,
                    payload: Self.featureExtAckPayload(featureID: 0x02, data: [])
                ))
        ) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .connectionError)
        }

        expectThrowsError(try eventHandler.startupDoneFuture.wait()) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .connectionError)
        }
    }

    @Test func startupPipelineFailsStartupFutureOnUnexpectedFeatureAck() throws {
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
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)

        expectThrowsError(
            try channel.writeInbound(
                Self.packet(
                    type: .preloginLoginOrTablularResponse,
                    payload: Self.featureExtAckPayload(featureID: 0x55, data: [])
                ))
        ) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .connectionError)
        }

        expectThrowsError(try eventHandler.startupDoneFuture.wait()) { error in
            let sqlError = error as? TDSSQLError
            expectEqual(sqlError?.code, .connectionError)
        }
    }

    @Test func preloginEncryptionIsDerivedFromTLSMode() throws {
        let sslContext = try NIOSSLContext(configuration: .makeClientConfiguration())

        expectEqual(TDSConnection.Configuration.TLS.disable.preloginEncryption, .encryptNotSup)
        expectEqual(TDSConnection.Configuration.TLS.prefer(sslContext).preloginEncryption, .encryptOn)
        expectEqual(TDSConnection.Configuration.TLS.require(sslContext).preloginEncryption, .encryptReq)

        expectFalse(TDSConnection.Configuration.TLS.disable.isCompatible(with: .encryptReq))
        expectFalse(TDSConnection.Configuration.TLS.require(sslContext).isCompatible(with: .encryptNotSup))
        expectTrue(TDSConnection.Configuration.TLS.prefer(sslContext).isCompatible(with: .encryptOff))
    }

    @Test func preloginTLSHandlerWrapsAndUnwrapsTLSBytes() throws {
        let channel = EmbeddedChannel(handler: TDSPreloginTLSHandler())

        var outboundTLS = ByteBufferAllocator().buffer(capacity: 8)
        outboundTLS.writeBytes([0x16, 0x03, 0x03, 0x00, 0x2A])
        try channel.writeOutbound(outboundTLS)

        var wrapped: ByteBuffer = try requireUnwrap(channel.readOutbound())
        expectEqual(wrapped.readInteger(as: UInt8.self), TDSPacket.MessageType.prelogin.rawValue)
        expectEqual(wrapped.readInteger(as: UInt8.self), TDSPacket.StatusFlag.eom.rawValue)
        expectEqual(wrapped.readInteger(endianness: .big, as: UInt16.self), UInt16(TDSPacket.headerLength + 5))
        wrapped.moveReaderIndex(forwardBy: 4)
        expectEqual(wrapped.readBytes(length: 5), [0x16, 0x03, 0x03, 0x00, 0x2A])

        var inboundTLS = ByteBufferAllocator().buffer(capacity: 8)
        inboundTLS.writeBytes([0x16, 0x03, 0x03, 0x00, 0x11])
        try channel.writeInbound(Self.packet(type: .prelogin, payload: inboundTLS))

        var unwrapped: ByteBuffer = try requireUnwrap(channel.readInbound())
        expectEqual(unwrapped.readBytes(length: 5), [0x16, 0x03, 0x03, 0x00, 0x11])
    }

    private static func loginFeatureExtSlice(from packet: ByteBuffer) throws -> ByteBuffer {
        let loginStart = TDSPacket.headerLength
        let extensionEntry = loginStart + 36 + 5 * 4
        let extensionOffset = try requireUnwrap(
            packet.getInteger(at: extensionEntry, endianness: .little, as: UInt16.self))
        let featureExtOffset = try requireUnwrap(
            packet.getInteger(
                at: loginStart + Int(extensionOffset),
                endianness: .little,
                as: UInt32.self
            ))
        return try requireUnwrap(
            packet.getSlice(
                at: loginStart + Int(featureExtOffset),
                length: packet.writerIndex - (loginStart + Int(featureExtOffset))
            ))
    }

    private func skipRPCParameter(_ packet: inout ByteBuffer, name: String) {
        expectEqual(packet.readInteger(as: UInt8.self), UInt8(name.utf16.count))
        expectEqual(packet.readUTF16(characterCount: name.utf16.count), name)
        _ = packet.readInteger(as: UInt8.self)
        expectEqual(packet.readInteger(as: UInt8.self), TDSDataType.nVarChar.rawValue)
        let maxBytes = packet.readInteger(endianness: .little, as: UInt16.self)
        _ = packet.readBytes(length: 5)
        if maxBytes == UInt16.max {
            let totalLength = packet.readInteger(endianness: .little, as: UInt64.self) ?? 0
            let chunkLength = packet.readInteger(endianness: .little, as: UInt32.self) ?? 0
            _ = packet.readBytes(length: Int(chunkLength))
            _ = packet.readInteger(endianness: .little, as: UInt32.self)
            expectEqual(totalLength, UInt64.max - 1)
        } else {
            let valueLength = packet.readInteger(endianness: .little, as: UInt16.self) ?? 0
            _ = packet.readBytes(length: Int(valueLength))
        }
    }
}
