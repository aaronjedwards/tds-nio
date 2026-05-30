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
        XCTAssertEqual(
            packet.getBytes(at: TDSPacket.headerLength + Int(options[0].1), length: 6),
            [
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
        XCTAssertEqual(
            packet.getBytes(at: TDSPacket.headerLength + Int(options[0].1), length: 6),
            [
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
        XCTAssertEqual(
            packet.getInteger(at: loginStart, endianness: .little, as: UInt32.self),
            UInt32(packet.writerIndex - loginStart))
        XCTAssertEqual(
            packet.getInteger(at: loginStart + 4, endianness: .little, as: UInt32.self),
            TDSProtocolVersion.v7_4.wireValue)
        XCTAssertEqual(
            packet.getInteger(at: loginStart + 8, endianness: .little, as: UInt32.self),
            UInt32(configuration.packetSize))
        XCTAssertEqual(packet.getInteger(at: loginStart + 26, as: UInt8.self), 0x00)
        XCTAssertEqual(packet.getInteger(at: loginStart + 27, as: UInt8.self), 0x10)

        let extensionEntry = loginStart + 36 + 5 * 4
        let extensionOffset = try XCTUnwrap(packet.getInteger(at: extensionEntry, endianness: .little, as: UInt16.self))
        let extensionLength = try XCTUnwrap(
            packet.getInteger(at: extensionEntry + 2, endianness: .little, as: UInt16.self))
        XCTAssertEqual(extensionLength, 4)

        let featureExtOffset = try XCTUnwrap(
            packet.getInteger(
                at: loginStart + Int(extensionOffset),
                endianness: .little,
                as: UInt32.self
            ))
        var featureExt = try XCTUnwrap(
            packet.getSlice(
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
        let sspiOffset = try XCTUnwrap(
            packet.getInteger(
                at: sspiEntry,
                endianness: .little,
                as: UInt16.self
            ))
        let sspiLength = try XCTUnwrap(
            packet.getInteger(
                at: sspiEntry + 2,
                endianness: .little,
                as: UInt16.self
            ))
        let sspiLongLength = try XCTUnwrap(
            packet.getInteger(
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
        let languageOffset = try XCTUnwrap(
            packet.getInteger(
                at: languageEntry,
                endianness: .little,
                as: UInt16.self
            ))
        let languageLength = try XCTUnwrap(
            packet.getInteger(
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
        let passwordOffset = try XCTUnwrap(
            packet.getInteger(
                at: passwordEntry,
                endianness: .little,
                as: UInt16.self
            ))
        let passwordLength = try XCTUnwrap(
            packet.getInteger(
                at: passwordEntry + 2,
                endianness: .little,
                as: UInt16.self
            ))

        XCTAssertEqual(passwordLength, UInt16(configuration.password.utf16.count))
        let encodedPassword = try XCTUnwrap(
            packet.getBytes(
                at: loginStart + Int(passwordOffset),
                length: Int(passwordLength) * 2
            ))
        XCTAssertEqual(encodedPassword, Self.loginPasswordBytes(configuration.password))
        XCTAssertNotEqual(
            encodedPassword,
            Array(configuration.password.utf16).flatMap {
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
        capabilities.adjustForFeatureExtAck(
            .init(options: [
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
        encoder.rpc(
            .init(
                procedure: "dbo.emptyText",
                parameters: [
                    .init(name: "@text", value: .string(""))
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

    func testRPCPacketEncodesWidthSpecificIntegerParameters() throws {
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
        encoder.rpc(
            .init(
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
        encoder.rpc(
            .init(
                procedure: "dbo.answer",
                parameters: [
                    .init(name: "@answer", value: .int(0), isOutput: true)
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
        encoder.rpc(
            .init(
                procedure: "dbo.money",
                parameters: [
                    .init(name: "@amount", value: .decimal("123.45"))
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
        encoder.rpc(
            .init(
                procedure: "dbo.guid",
                parameters: [
                    .init(name: "@id", value: .guid(Self.guid))
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
        XCTAssertEqual(
            packet.readBytes(length: 16),
            [
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
        encoder.rpc(
            .init(
                procedure: "dbo.xml",
                parameters: [
                    .init(name: "@doc", value: .xml([0x3C, 0x72, 0x2F, 0x3E]))
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
        encoder.rpc(
            .init(
                procedure: "dbo.json",
                parameters: [
                    .init(name: "@doc", value: .json(Array(#"{"ok":true}"#.utf8)))
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
        encoder.rpc(
            .init(
                procedure: "dbo.longText",
                parameters: [
                    .init(name: "@text", value: .string(value))
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
        encoder.rpc(
            .init(
                procedure: "dbo.longBytes",
                parameters: [
                    .init(name: "@data", value: .bytes(value))
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
        encoder.rpc(
            .init(
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

        XCTAssertThrowsError(
            try channel.pipeline.triggerUserOutboundEvent(
                TDSAuthenticationToken.federated(token: [0xAA], nonce: [0x01])
            ).wait()
        ) { error in
            let sqlError = error as? TDSSQLError
            XCTAssertEqual(sqlError?.code, .connectionError)
        }
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
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

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.donePayload()
            ))
        let result = try promise.futureResult.wait()
        XCTAssertEqual(result.rows.count, 0)
        XCTAssertEqual(result.resultSets.count, 0)
    }

    func testBulkLoadPacketBoundsVariableValuesToColumnMax() throws {
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
        try channel.writeOutbound(
            TDSTask.bulkLoad(
                .init(
                    columns: [.init(name: "id", dataType: .int)],
                    rows: [[.int(1)]]
                ), promise))

        let packet: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(packet.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.bulkLoadData.rawValue)

        try channel.writeInbound(
            Self.packet(
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

        try channel.writeInbound(
            Self.packet(
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

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                statusFlags: [],
                payload: firstPayload
            ))
        XCTAssertNil(try channel.readInbound(as: TinySequence<TDSBackendMessageDecoder.Container>.self))

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
        XCTAssertEqual(
            dataClassification.labels,
            [
                .init(name: "Confidential", id: "label-id")
            ])
        XCTAssertEqual(
            dataClassification.informationTypes,
            [
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
        XCTAssertEqual(
            metadata.columns[1].typeInfo.xmlInfo,
            .init(
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
        XCTAssertEqual(
            metadata.columns[0].typeInfo.udtInfo?.assemblyQualifiedName, "Microsoft.SqlServer.Types.SqlGeography")
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
        XCTAssertEqual(
            firstRow.values, [.string("hello"), .string("abc"), .bytes([0xDE, 0xAD]), .bytes([0xBE, 0xEF])])
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

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))

        let login: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(login.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.tds7Login.rawValue)
        XCTAssertEqual(login.getInteger(at: 2, endianness: .big, as: UInt16.self), UInt16(login.writerIndex))

        try channel.writeInbound(
            Self.packet(
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

        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.selectOneTokenStreamPayload()
            ))
        let result = try queryPromise.futureResult.wait()
        XCTAssertEqual(result.columns.map(\.name), ["id", "label"])
        XCTAssertEqual(result.rows.count, 1)
        XCTAssertEqual(result.rows[0].values, [.int32(1), .string("one")])
        XCTAssertEqual(result.rows[0]["label"], .string("one"))

        let rpcPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        try channel.writeOutbound(
            TDSTask.rpc(
                .init(procedure: "dbo.echo", parameters: [.init(name: "@id", value: .int(1))]),
                rpcPromise
            ))
        let rpc: ByteBuffer = try XCTUnwrap(channel.readOutbound())
        XCTAssertEqual(rpc.getInteger(at: 0, as: UInt8.self), TDSPacket.MessageType.rpc.rawValue)

        try channel.writeInbound(
            Self.packet(
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
        try channel.writeInbound(
            Self.packet(
                type: .preloginLoginOrTablularResponse,
                payload: Self.preloginResponsePayload(encryption: .encryptOff)
            ))
        _ = try channel.readOutbound(as: ByteBuffer.self)
        XCTAssertThrowsError(
            try channel.writeInbound(
                Self.packet(
                    type: .preloginLoginOrTablularResponse,
                    payload: Self.errorPayload(message: "Login failed for user 'sa'.", number: 18456)
                ))
        ) { error in
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

    func testPreloginEncryptionIsDerivedFromTLSMode() throws {
        let sslContext = try NIOSSLContext(configuration: .makeClientConfiguration())

        XCTAssertEqual(TDSConnection.Configuration.TLS.disable.preloginEncryption, .encryptNotSup)
        XCTAssertEqual(TDSConnection.Configuration.TLS.prefer(sslContext).preloginEncryption, .encryptOn)
        XCTAssertEqual(TDSConnection.Configuration.TLS.require(sslContext).preloginEncryption, .encryptReq)

        XCTAssertFalse(TDSConnection.Configuration.TLS.disable.isCompatible(with: .encryptReq))
        XCTAssertFalse(TDSConnection.Configuration.TLS.require(sslContext).isCompatible(with: .encryptNotSup))
        XCTAssertTrue(TDSConnection.Configuration.TLS.prefer(sslContext).isCompatible(with: .encryptOff))
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
}
