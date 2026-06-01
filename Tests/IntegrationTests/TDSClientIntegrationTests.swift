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
import NIOCore
import NIOPosix
import TDSNIO
import Testing

@Suite(
    .disabled(
        if: env("TDS_INTEGRATION_TESTS") != "1", "Set TDS_INTEGRATION_TESTS=1 to run SQL Server integration tests."),
    .disabled(if: env("SMOKE_TEST_ONLY") == "1", "Skipping integration suite while SMOKE_TEST_ONLY=1."),
    .timeLimit(.minutes(5))
)
final class TDSClientIntegrationTests {
    private let group = NIOSingletons.posixEventLoopGroup

    @Test func pool() async throws {
        var options = TDSClient.Options()
        options.maximumConnections = env("TDS_CLIENT_MAX_CONNECTIONS").flatMap(Int.init) ?? 8
        options.connectionIdleTimeout = .seconds(5)

        let client = TDSClient(
            configuration: try TDSConnection.testConfig(),
            options: options,
            eventLoopGroup: self.group,
            backgroundLogger: .tdsTest
        )
        try await withRunningClient(client) {
            let iterations = env("TDS_CLIENT_POOL_ITERATIONS").flatMap(Int.init) ?? 10_000
            let concurrency = env("TDS_CLIENT_POOL_CONCURRENCY").flatMap(Int.init) ?? options.maximumConnections * 16
            try await runPoolIterations(iterations, concurrency: concurrency, client: client)
        }
    }

    @Test func pingPong() async throws {
        var options = TDSClient.Options()
        options.maximumConnections = 2
        options.keepAliveBehavior?.frequency = .milliseconds(100)
        options.connectionIdleTimeout = .milliseconds(500)

        let client = TDSClient(
            configuration: try TDSConnection.testConfig(),
            options: options,
            eventLoopGroup: self.group,
            backgroundLogger: .tdsTest
        )
        try await withRunningClient(client) {
            let value = try await client.withConnection { connection in
                let rows = try await connection.execute("SELECT CAST(N'hello' AS nvarchar(20)) AS value").rows
                return try rows.first?.decode(column: "value", as: String.self)
            }
            expectEqual(value, "hello")

            try await Task.sleep(for: .seconds(1))

            let nextValue = try await client.withConnection { connection in
                let rows = try await connection.execute("SELECT CAST(N'next' AS nvarchar(20)) AS value").rows
                return try rows.first?.decode(column: "value", as: String.self)
            }
            expectEqual(nextValue, "next")
        }
    }
}

private func withRunningClient(
    _ client: TDSClient,
    _ body: () async throws -> Void
) async throws {
    try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask {
            await client.run()
        }
        await Task.yield()

        do {
            try await body()
        } catch {
            group.cancelAll()
            throw error
        }

        group.cancelAll()
    }
}

private func runPoolIterations(
    _ iterations: Int,
    concurrency: Int,
    client: TDSClient
) async throws {
    let inFlightLimit = max(1, min(iterations, concurrency))
    var submitted = 0
    var completed = 0

    try await withThrowingTaskGroup(of: Void.self) { group in
        func submitOne() {
            submitted += 1
            group.addTask {
                try await client.withConnection { connection in
                    let rows = try await connection.execute(
                        "SELECT CAST(1 AS int) AS user_id, CAST(N'AJ' AS nvarchar(20)) AS name, CAST(23 AS int) AS age"
                    ).rows

                    expectEqual(rows.count, 1)
                    expectEqual(try rows[0].decode(column: "user_id", as: Int.self), 1)
                    expectEqual(try rows[0].decode(column: "name", as: String.self), "AJ")
                    expectEqual(try rows[0].decode(column: "age", as: Int.self), 23)
                }
            }
        }

        for _ in 0..<inFlightLimit {
            submitOne()
        }

        while completed < iterations {
            try await group.next()
            completed += 1

            if submitted < iterations {
                submitOne()
            }
        }
    }
}
