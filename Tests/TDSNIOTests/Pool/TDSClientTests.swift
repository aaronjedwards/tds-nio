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
import NIOPosix
import NIOSSL
import NIOTestUtils
import Testing

@testable import TDSNIO

extension TDSTests {
    @Test func loginPacketNormalizesClientIDToSixBytes() throws {
        var shortConfiguration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!",
            tls: .disable,
            clientID: [0xAA, 0xBB]
        )
        expectEqual(shortConfiguration.clientID, [0xAA, 0xBB, 0, 0, 0, 0])

        shortConfiguration.clientID = [1, 2, 3, 4, 5, 6, 7, 8]
        expectEqual(shortConfiguration.clientID, [1, 2, 3, 4, 5, 6])

        var encoder = TDSFrontendMessageEncoder(
            buffer: ByteBufferAllocator().buffer(capacity: 512)
        )
        encoder.login(configuration: shortConfiguration)
        let packet = encoder.flush()

        expectEqual(
            packet.getBytes(at: TDSPacket.headerLength + 36 + 9 * 4, length: 6),
            [1, 2, 3, 4, 5, 6]
        )
    }

    @Test func clientInitializesConnectionFactoryAndKeepAliveBehavior() throws {
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

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            try? group.syncShutdownGracefully()
        }

        let client = TDSClient(configuration: configuration, options: options, eventLoopGroup: group)

        expectEqual(client.factory.configuration.host, "pooled.sql.example.test")
        expectEqual(client.factory.configuration.database, "master")
        expectEqual(client.factory.configuration.packetSize, 2048)
        expectEqual(TDSKeepAliveBehavior(options.keepAliveBehavior).keepAliveFrequency, .seconds(3))
        expectNil(TDSKeepAliveBehavior(nil).keepAliveFrequency)
    }
}
