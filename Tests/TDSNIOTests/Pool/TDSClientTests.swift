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
}
