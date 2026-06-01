import NIOCore
import NIOPosix
import TDSNIO
import Testing

@Suite(
    .disabled(if: env("TDS_INTEGRATION_TESTS") != "1", "Set TDS_INTEGRATION_TESTS=1 to run SQL Server integration tests."),
    .disabled(if: env("SMOKE_TEST_ONLY") == "1", "Skipping integration suite while SMOKE_TEST_ONLY=1."),
    .timeLimit(.minutes(5))
)
final class TDSClientIntegrationTests {
    private let group: MultiThreadedEventLoopGroup

    init() {
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
    }

    deinit {
        try? self.group.syncShutdownGracefully()
    }

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
        let clientTask = Task {
            await client.run()
        }
        defer {
            clientTask.cancel()
        }

        let iterations = env("TDS_CLIENT_POOL_ITERATIONS").flatMap(Int.init) ?? 10_000
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<iterations {
                group.addTask {
                    try await client.withConnection { connection in
                        let rows = try await connection.execute(
                            "SELECT CAST(1 AS int) AS user_id, CAST(N'Timo' AS nvarchar(20)) AS name, CAST(23 AS int) AS age"
                        ).rows

                        expectEqual(rows.count, 1)
                        expectEqual(try rows[0].decode(column: "user_id", as: Int.self), 1)
                        expectEqual(try rows[0].decode(column: "name", as: String.self), "Timo")
                        expectEqual(try rows[0].decode(column: "age", as: Int.self), 23)
                    }
                }
            }

            for _ in 0..<iterations {
                try await group.next()
            }
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
        let clientTask = Task {
            await client.run()
        }
        defer {
            clientTask.cancel()
        }

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
