import Foundation
import NIOCore
import NIOPosix
import TDSNIO
import XCTest

final class TDSNIOIntegrationTests: XCTestCase {
    private var group: MultiThreadedEventLoopGroup!

    private var eventLoop: EventLoop {
        self.group.next()
    }

    override func setUpWithError() throws {
        try super.setUpWithError()
        guard env("TDS_INTEGRATION_TESTS") == "1" else {
            throw XCTSkip("Set TDS_INTEGRATION_TESTS=1 to run SQL Server integration tests.")
        }
        if env("SMOKE_TEST_ONLY") == "1" {
            throw XCTSkip("Skipping integration suite while SMOKE_TEST_ONLY=1.")
        }
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }

    override func tearDownWithError() throws {
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }

    func testConnectionAndClose() async throws {
        let connection = try await TDSConnection.test(on: self.eventLoop)
        try await connection.close()
    }

    func testAuthenticationFailure() async throws {
        var configuration = try TDSConnection.testConfig()
        configuration.password = "wrong_password"

        var connection: TDSConnection?
        do {
            connection = try await TDSConnection.connect(
                on: self.eventLoop,
                configuration: configuration,
                id: 1,
                logger: .tdsTest
            )
            XCTFail("Authentication should fail")
        } catch {
            // expected
        }

        try await connection?.close()
    }

    func testMultipleFailingAttempts() async throws {
        var configuration = try TDSConnection.testConfig()
        configuration.password = "wrong_password"
        configuration.retryCount = 3
        configuration.retryDelay = 0

        var connection: TDSConnection?
        do {
            connection = try await TDSConnection.connect(
                on: self.eventLoop,
                configuration: configuration,
                id: 1,
                logger: .tdsTest
            )
            XCTFail("Authentication should fail")
        } catch {
            // expected
        }

        try await connection?.close()
    }

    func testConnectionAttemptCancels() async throws {
        var configuration = try TDSConnection.testConfig()
        configuration.password = "wrong_password"
        configuration.retryCount = 20
        configuration.retryDelay = 10

        let connectTask = Task {
            try await TDSConnection.connect(
                on: self.eventLoop,
                configuration: configuration,
                id: 1,
                logger: .tdsTest
            )
        }

        try await Task.sleep(for: .seconds(2))
        connectTask.cancel()

        do {
            let connection = try await connectTask.value
            try await connection.close()
            XCTFail("Retrying connection attempt should have been cancelled")
        } catch is CancellationError {
            // expected
        }
    }

    func testPing() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            try await connection.ping()
        }
    }

    func testSimpleQuery() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                "SELECT CAST(1 AS int) AS id, CAST(N'test' AS nvarchar(20)) AS label"
            ).rows

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows[0].decode(column: "id", as: Int.self), 1)
            XCTAssertEqual(try rows[0].decode(column: "label", as: String.self), "test")
        }
    }

    func testSimpleStreamingQuery() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.query(
                "SELECT CAST(1 AS int) AS id, CAST(N'test' AS nvarchar(20)) AS label"
            ).collect()

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows[0].decode(column: "id", as: Int.self), 1)
            XCTAssertEqual(try rows[0].decode(column: "label", as: String.self), "test")
        }
    }

    func testSimpleQuery2() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                "SELECT CAST(1 AS int) AS id"
            ).rows

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows.first?.decode(Int.self), 1)
        }
    }

    func testFirstColumnIntegerDecodeMatchesOracleNIOStyle() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                "SELECT CAST(1 AS int) AS id"
            ).rows

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows.first?.decode(Int.self), 1)
        }
    }

    func testFirstColumnDecodeMatchesOracleNIOStyle() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                "SELECT CAST(N'test' AS nvarchar(20)) AS value"
            ).rows

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows.first?.decode(String.self), "test")
        }
    }

    func testStreamingScalarDecodeMatchesOracleNIOStyle() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.query(
                """
                SELECT CAST(1 AS int) AS id
                UNION ALL
                SELECT CAST(2 AS int) AS id
                UNION ALL
                SELECT CAST(3 AS int) AS id
                ORDER BY id
                """
            )

            var values: [Int] = []
            for try await id in rows.decode(Int.self) {
                values.append(id)
            }

            XCTAssertEqual(values, [1, 2, 3])
        }
    }

    func testStreamingTupleDecodeMatchesOracleNIOStyle() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.query(
                """
                SELECT
                    CAST(1 AS int) AS user_id,
                    CAST(N'Timo' AS nvarchar(20)) AS name,
                    CAST(23 AS int) AS age
                """
            )

            var received = 0
            for try await (userID, name, age) in rows.decode((Int, String, Int).self) {
                XCTAssertEqual(userID, 1)
                XCTAssertEqual(name, "Timo")
                XCTAssertEqual(age, 23)
                received += 1
            }

            XCTAssertEqual(received, 1)
        }
    }

    func testNoRowsQuery() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                "SELECT CAST(NULL AS int) AS value WHERE 1 = 0"
            ).rows

            XCTAssertEqual(rows.count, 0)
        }
    }

    func testBoundQuery() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let value = "smoke"
            let rows = try await connection.execute(
                "SELECT \(value) AS value"
            ).rows

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows[0].decode(column: "value", as: String.self), value)
        }
    }

    func testUnusedBindDoesNotCrash() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            var binds = TDSBindings()
            binds.append(.int32(42), name: "@unused")

            let rows = try await connection.execute(TDSQuery(
                unsafeSQL: "SELECT CAST(1 AS int) AS value",
                binds: binds
            )).rows

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows.first?.decode(Int.self), 1)
        }
    }

    func testSimpleDateQuery() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                "SELECT CAST('2024-01-22T10:46:18.713' AS datetime) AS value"
            ).rows

            XCTAssertEqual(rows.count, 1)
            let value = try rows[0].decode(column: "value", as: Date.self)
            XCTAssertEqual(value.timeIntervalSince1970, 1_705_920_378.713, accuracy: 0.004)
        }
    }

    func testSimpleOptionalBinds() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let present: String? = "test"
            var rows = try await connection.execute("SELECT \(present) AS value").rows
            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows[0].decode(column: "value", as: String?.self), "test")

            let missing: String? = nil
            rows = try await connection.execute("SELECT \(missing) AS value").rows
            XCTAssertEqual(rows.count, 1)
            XCTAssertNil(try rows[0].decode(column: "value", as: String?.self))
        }
    }

    func testQuery10kItems() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                """
                WITH numbers AS (
                    SELECT TOP (10000)
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
                    FROM sys.all_objects a CROSS JOIN sys.all_objects b
                )
                SELECT CAST(id AS bigint) AS id
                FROM numbers
                ORDER BY id
                """
            ).rows

            XCTAssertEqual(rows.count, 10_000)
            for (index, row) in rows.enumerated() {
                XCTAssertEqual(try row.decode(column: "id", as: Int64.self), Int64(index + 1))
            }
        }
    }

    func testStreamingQuery10kItems() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.query(
                """
                WITH numbers AS (
                    SELECT TOP (10000)
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
                    FROM sys.all_objects a CROSS JOIN sys.all_objects b
                )
                SELECT CAST(id AS bigint) AS id
                FROM numbers
                ORDER BY id
                """
            )

            var received: Int64 = 0
            for try await row in rows {
                received += 1
                XCTAssertEqual(try row.decode(column: "id", as: Int64.self), received)
            }

            XCTAssertEqual(received, 10_000)
        }
    }

    func testFloatingPointNumbers() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                """
                WITH numbers AS (
                    SELECT TOP (100)
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
                    FROM sys.all_objects
                )
                SELECT CAST(id AS real) / CAST(100 AS real) AS value
                FROM numbers
                ORDER BY id
                """
            ).rows

            XCTAssertEqual(rows.count, 100)
            for (index, row) in rows.enumerated() {
                let expected = Float(index + 1) / 100
                XCTAssertEqual(try row.decode(column: "value", as: Float.self), expected, accuracy: 0.000_001)
            }
        }
    }

    func testStreamingFloatingPointNumbers() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.query(
                """
                WITH numbers AS (
                    SELECT TOP (100)
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
                    FROM sys.all_objects
                )
                SELECT CAST(id AS real) / CAST(100 AS real) AS value
                FROM numbers
                ORDER BY id
                """
            )

            var received = 0
            for try await row in rows {
                received += 1
                XCTAssertEqual(
                    try row.decode(column: "value", as: Float.self),
                    Float(received) / 100,
                    accuracy: 0.000_001
                )
            }

            XCTAssertEqual(received, 100)
        }
    }

    func testDuplicateColumnValues() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("duplicate")
            let identifier = sqlIdentifier(table)
            defer {
                Task {
                    try? await dropTableIfExists(table, on: connection)
                }
            }

            _ = try await connection.execute(
                "CREATE TABLE \(unescaped: identifier) (id int NOT NULL, title nvarchar(150) NOT NULL)"
            )
            _ = try await connection.execute(
                """
                INSERT INTO \(unescaped: identifier) (id, title)
                VALUES
                    (1, N'hello!'),
                    (2, N'hi!'),
                    (3, N'hello, there!'),
                    (4, N'hello, there!'),
                    (5, N'hello, guys!')
                """
            )

            let rows = try await connection.execute(
                "SELECT id, title FROM \(unescaped: identifier) ORDER BY id"
            ).rows

            XCTAssertEqual(rows.count, 5)
            let expected = ["hello!", "hi!", "hello, there!", "hello, there!", "hello, guys!"]
            for (index, row) in rows.enumerated() {
                XCTAssertEqual(try row.decode(column: "id", as: Int.self), index + 1)
                XCTAssertEqual(try row.decode(column: "title", as: String.self), expected[index])
            }

            try await dropTableIfExists(table, on: connection)
        }
    }

    func testStreamingDuplicateColumnValues() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("streaming_duplicate")
            let identifier = sqlIdentifier(table)
            defer {
                Task {
                    try? await dropTableIfExists(table, on: connection)
                }
            }

            _ = try await connection.execute(
                "CREATE TABLE \(unescaped: identifier) (id int NOT NULL, title nvarchar(150) NOT NULL)"
            )
            _ = try await connection.execute(
                """
                INSERT INTO \(unescaped: identifier) (id, title)
                VALUES
                    (1, N'hello!'),
                    (2, N'hi!'),
                    (3, N'hello, there!'),
                    (4, N'hello, there!'),
                    (5, N'hello, guys!')
                """
            )

            let rows = try await connection.query(
                "SELECT id, title FROM \(unescaped: identifier) ORDER BY id"
            )

            let expected = ["hello!", "hi!", "hello, there!", "hello, there!", "hello, guys!"]
            var index = 0
            for try await (id, title) in rows.decode((Int, String).self) {
                XCTAssertEqual(id, index + 1)
                XCTAssertEqual(title, expected[index])
                index += 1
            }
            XCTAssertEqual(index, expected.count)

            try await dropTableIfExists(table, on: connection)
        }
    }

    func testDuplicateColumnValueInEveryRow() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("duplicate_every_row")
            let identifier = sqlIdentifier(table)
            defer {
                Task {
                    try? await dropTableIfExists(table, on: connection)
                }
            }

            _ = try await connection.execute(
                "CREATE TABLE \(unescaped: identifier) (id int NOT NULL, title nvarchar(150) NOT NULL)"
            )
            _ = try await connection.execute(
                """
                INSERT INTO \(unescaped: identifier) (id, title)
                VALUES
                    (1, N'hello!'),
                    (2, N'hello!'),
                    (3, N'hello!'),
                    (4, N'hello!'),
                    (5, N'hello!')
                """
            )

            let rows = try await connection.execute(
                "SELECT id, title FROM \(unescaped: identifier) ORDER BY id"
            ).rows

            XCTAssertEqual(rows.count, 5)
            for (index, row) in rows.enumerated() {
                XCTAssertEqual(try row.decode(column: "id", as: Int.self), index + 1)
                XCTAssertEqual(try row.decode(column: "title", as: String.self), "hello!")
            }

            try await dropTableIfExists(table, on: connection)
        }
    }

    func testNoRowsQueryFromActualTable() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("empty")
            let identifier = sqlIdentifier(table)
            defer {
                Task {
                    try? await dropTableIfExists(table, on: connection)
                }
            }

            _ = try await connection.execute(
                "CREATE TABLE \(unescaped: identifier) (id int NOT NULL, title nvarchar(150) NOT NULL)"
            )
            let rows = try await connection.execute(
                "SELECT id, title FROM \(unescaped: identifier) ORDER BY id"
            ).rows

            XCTAssertEqual(rows.count, 0)
            try await dropTableIfExists(table, on: connection)
        }
    }

    func testStreamingNoRowsQueryFromActualTable() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("streaming_empty")
            let identifier = sqlIdentifier(table)
            defer {
                Task {
                    try? await dropTableIfExists(table, on: connection)
                }
            }

            _ = try await connection.execute(
                "CREATE TABLE \(unescaped: identifier) (id int NOT NULL, title nvarchar(150) NOT NULL)"
            )
            let rows = try await connection.query(
                "SELECT id, title FROM \(unescaped: identifier) ORDER BY id"
            ).collect()

            XCTAssertEqual(rows.count, 0)
            try await dropTableIfExists(table, on: connection)
        }
    }

    func testEmptyStringBind() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                "SELECT \("") AS value"
            ).rows

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows[0].decode(column: "value", as: String?.self), "")
            XCTAssertEqual(try rows[0].decode(column: "value", as: String.self), "")
        }
    }

    func testSimpleTSQLBatch() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let input = 42
            _ = try await connection.execute(
                """
                DECLARE @result int;
                SET @result = \(input) + 69;
                """
            )
        }
    }

    func testCommit() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("commit")
            let identifier = sqlIdentifier(table)
            defer {
                Task {
                    try? await dropTableIfExists(table, on: connection)
                }
            }

            _ = try await connection.execute(
                "CREATE TABLE \(unescaped: identifier) (id int NOT NULL PRIMARY KEY, label nvarchar(20) NOT NULL)"
            )

            try await connection.beginTransaction()
            _ = try await connection.execute(
                "INSERT INTO \(unescaped: identifier) (id, label) VALUES (1, N'committed')"
            )
            try await connection.commit()

            let rows = try await connection.execute(
                "SELECT label FROM \(unescaped: identifier) WHERE id = 1"
            ).rows

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows[0].decode(String.self), "committed")
            try await dropTableIfExists(table, on: connection)
        }
    }

    func testRollback() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("rollback")
            let identifier = sqlIdentifier(table)
            defer {
                Task {
                    try? await dropTableIfExists(table, on: connection)
                }
            }

            _ = try await connection.execute(
                "CREATE TABLE \(unescaped: identifier) (id int NOT NULL PRIMARY KEY, label nvarchar(20) NOT NULL)"
            )

            try await connection.beginTransaction()
            _ = try await connection.execute(
                "INSERT INTO \(unescaped: identifier) (id, label) VALUES (1, N'rolled_back')"
            )
            try await connection.rollback()

            let rows = try await connection.execute(
                "SELECT label FROM \(unescaped: identifier) WHERE id = 1"
            ).rows

            XCTAssertEqual(rows.count, 0)
            try await dropTableIfExists(table, on: connection)
        }
    }

    func testSimpleBinaryValueViaData() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("binary")
            let identifier = sqlIdentifier(table)
            let payload = Data((0..<255).map(UInt8.init))

            do {
                _ = try await connection.execute(
                    "CREATE TABLE \(unescaped: identifier) (id int NOT NULL, content varbinary(max) NOT NULL)"
                )
                _ = try await connection.execute(
                    "INSERT INTO \(unescaped: identifier) (id, content) VALUES (1, \(payload))"
                )

                let rows = try await connection.execute(
                    "SELECT id, content FROM \(unescaped: identifier) ORDER BY id"
                ).rows

                XCTAssertEqual(rows.count, 1)
                XCTAssertEqual(try rows[0].decode(column: "id", as: Int.self), 1)
                XCTAssertEqual(try rows[0].decode(column: "content", as: Data.self), payload)
            } catch {
                try? await dropTableIfExists(table, on: connection)
                throw error
            }

            try await dropTableIfExists(table, on: connection)
        }
    }

    func testSimpleBinaryValueViaByteBuffer() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("binary_buffer")
            let identifier = sqlIdentifier(table)
            var payload = ByteBuffer()
            payload.reserveCapacity("binary data".utf8.count * 5000)
            for _ in 0..<5000 {
                payload.writeString("binary data")
            }

            do {
                _ = try await connection.execute(
                    "CREATE TABLE \(unescaped: identifier) (id int NOT NULL, content varbinary(max) NOT NULL)"
                )
                _ = try await connection.execute(
                    "INSERT INTO \(unescaped: identifier) (id, content) VALUES (1, \(payload))"
                )

                let rows = try await connection.execute(
                    "SELECT id, content FROM \(unescaped: identifier) ORDER BY id"
                ).rows

                XCTAssertEqual(rows.count, 1)
                XCTAssertEqual(try rows[0].decode(column: "id", as: Int.self), 1)
                XCTAssertEqual(try rows[0].decode(column: "content", as: ByteBuffer.self), payload)
            } catch {
                try? await dropTableIfExists(table, on: connection)
                throw error
            }

            try await dropTableIfExists(table, on: connection)
        }
    }

    func testSimpleBinaryValueConcurrently5Times() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("binary_concurrent")
            let identifier = sqlIdentifier(table)
            var payload = ByteBuffer()
            payload.reserveCapacity("binary data".utf8.count * 5000)
            for _ in 0..<5000 {
                payload.writeString("binary data")
            }

            do {
                _ = try await connection.execute(
                    "CREATE TABLE \(unescaped: identifier) (id int NOT NULL, content varbinary(max) NOT NULL)"
                )
                _ = try await connection.execute(
                    "INSERT INTO \(unescaped: identifier) (id, content) VALUES (1, \(payload))"
                )

                try await withThrowingTaskGroup(of: [TDSRow].self) { group in
                    for _ in 0..<5 {
                        group.addTask {
                            try await connection.execute(
                                "SELECT id, content FROM \(unescaped: identifier) ORDER BY id"
                            ).rows
                        }
                    }

                    for try await rows in group {
                        XCTAssertEqual(rows.count, 1)
                        XCTAssertEqual(try rows[0].decode(column: "id", as: Int.self), 1)
                        XCTAssertEqual(try rows[0].decode(column: "content", as: ByteBuffer.self), payload)
                    }
                }
            } catch {
                try? await dropTableIfExists(table, on: connection)
                throw error
            }

            try await dropTableIfExists(table, on: connection)
        }
    }

    func testLargeBinaryBindBeforeNonLargeBindWorks() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let table = uniqueTableName("large_binary_order")
            let identifier = sqlIdentifier(table)
            var payload = ByteBuffer()
            payload.reserveCapacity("binary data".utf8.count * 5000)
            for _ in 0..<5000 {
                payload.writeString("binary data")
            }

            do {
                _ = try await connection.execute(
                    """
                    CREATE TABLE \(unescaped: identifier) (
                        id int NOT NULL,
                        mimetype nvarchar(50) NOT NULL,
                        filename nvarchar(100) NOT NULL,
                        data varbinary(max) NOT NULL
                    )
                    """
                )
                _ = try await connection.execute(
                    """
                    INSERT INTO \(unescaped: identifier) (id, mimetype, filename, data)
                    VALUES (1, \("image/jpeg"), \("image.jpeg"), \(payload))
                    """
                )

                var rows = try await connection.execute(
                    "SELECT data, filename FROM \(unescaped: identifier) WHERE id = 1"
                ).rows
                XCTAssertEqual(rows.count, 1)
                var (data, filename) = try rows[0].decode((ByteBuffer, String).self)
                XCTAssertEqual(data, payload)
                XCTAssertEqual(filename, "image.jpeg")

                payload.clear(minimumCapacity: "binory doto".utf8.count * 5000)
                for _ in 0..<5000 {
                    payload.writeString("binory doto")
                }
                _ = try await connection.execute(
                    "UPDATE \(unescaped: identifier) SET data = \(payload) WHERE id = 1"
                )

                rows = try await connection.execute(
                    "SELECT data, filename FROM \(unescaped: identifier) WHERE id = 1"
                ).rows
                XCTAssertEqual(rows.count, 1)
                (data, filename) = try rows[0].decode((ByteBuffer, String).self)
                XCTAssertEqual(data, payload)
                XCTAssertEqual(filename, "image.jpeg")
            } catch {
                try? await dropTableIfExists(table, on: connection)
                throw error
            }

            try await dropTableIfExists(table, on: connection)
        }
    }

    func testJSONBindAndDecode() async throws {
        struct Payload: Codable, Equatable, Sendable {
            var id: Int
            var name: String
            var tags: [String]
        }

        try await withTDSConnection(on: self.eventLoop) { connection in
            let payload = Payload(id: 7, name: "sql-server", tags: ["tds", "nio"])
            let encoded = try String(decoding: JSONEncoder().encode(payload), as: UTF8.self)
            let rows = try await connection.execute(
                "SELECT CAST(\(encoded) AS nvarchar(max)) AS payload"
            ).rows

            XCTAssertEqual(rows.count, 1)
            let decoded = try rows[0].decode(column: "payload", as: TDSJSONValue<Payload>.self)
            XCTAssertEqual(decoded.value, payload)
        }
    }

    func testTemporalTypesRoundTrip() async throws {
        let date = TDSDate(year: 2024, month: 1, day: 22)
        let time = TDSTime(hour: 10, minute: 46, second: 18, nanosecond: 713_000_000, scale: 3)
        let dateTime = TDSDateTime(date: date, time: time)
        let offset = TDSDateTimeOffset(dateTime: dateTime, offsetMinutes: 60)

        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                """
                SELECT
                    \(date) AS bound_date,
                    \(time) AS bound_time,
                    \(dateTime) AS bound_datetime2,
                    \(offset) AS bound_datetimeoffset,
                    CAST('2024-01-22' AS date) AS selected_date,
                    CAST('10:46:18.713' AS time(3)) AS selected_time,
                    CAST('2024-01-22T10:46:18.713' AS datetime2(3)) AS selected_datetime2,
                    CAST('2024-01-22T10:46:18.713+01:00' AS datetimeoffset(3)) AS selected_datetimeoffset
                """
            ).rows

            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(try rows[0].decode(column: "bound_date", as: TDSDate.self), date)
            XCTAssertEqual(try rows[0].decode(column: "bound_time", as: TDSTime.self), time)
            XCTAssertEqual(try rows[0].decode(column: "bound_datetime2", as: TDSDateTime.self), dateTime)
            XCTAssertEqual(try rows[0].decode(column: "bound_datetimeoffset", as: TDSDateTimeOffset.self), offset)
            XCTAssertEqual(try rows[0].decode(column: "selected_date", as: TDSDate.self), date)
            XCTAssertEqual(try rows[0].decode(column: "selected_time", as: TDSTime.self), time)
            XCTAssertEqual(try rows[0].decode(column: "selected_datetime2", as: TDSDateTime.self), dateTime)
            XCTAssertEqual(try rows[0].decode(column: "selected_datetimeoffset", as: TDSDateTimeOffset.self), offset)
        }
    }

    func testMultipleRowsWithFourColumnsWork() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.execute(
                """
                WITH numbers AS (
                    SELECT CAST(1 AS int) AS id
                    UNION ALL
                    SELECT CAST(2 AS int) AS id
                    UNION ALL
                    SELECT CAST(3 AS int) AS id
                    UNION ALL
                    SELECT CAST(4 AS int) AS id
                )
                SELECT
                    id,
                    CAST('2024-01-22T10:46:18.713' AS datetime) AS created_at,
                    CONCAT(N'user_', id) AS username,
                    CAST(N'test' AS nvarchar(20)) AS suffix
                FROM numbers
                ORDER BY id
                """
            ).rows

            XCTAssertEqual(rows.count, 4)
            for (index, row) in rows.enumerated() {
                let (level, _, username, suffix) = try row.decode((Int, Date, String, String).self)
                XCTAssertEqual(level, index + 1)
                XCTAssertEqual(username, "user_\(index + 1)")
                XCTAssertEqual(suffix, "test")
            }
        }
    }

    func testMalformedQuery() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            do {
                _ = try await connection.execute("SELECT 'hello")
                XCTFail("Malformed query should fail")
            } catch let error as TDSSQLError {
                XCTAssertEqual(error.code, .server)
                XCTAssertEqual(error.serverInfo?.number, 105)
            }
        }
    }

    func testQueryOnMissingTableFails() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            do {
                _ = try await connection.execute("SELECT id FROM dbo.tds_nio_missing_table")
                XCTFail("Query on missing table should fail")
            } catch let error as TDSSQLError {
                XCTAssertEqual(error.code, .server)
                XCTAssertEqual(error.serverInfo?.number, 208)
            }
        }
    }

    func testTableWithUnfulfilledConstraintFails() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let parent = uniqueTableName("constraint_parent")
            let child = uniqueTableName("constraint_child")
            let parentIdentifier = sqlIdentifier(parent)
            let childIdentifier = sqlIdentifier(child)

            do {
                _ = try await connection.execute(
                    "CREATE TABLE \(unescaped: parentIdentifier) (id int NOT NULL PRIMARY KEY)"
                )
                _ = try await connection.execute(
                    """
                    CREATE TABLE \(unescaped: childIdentifier) (
                        id int NOT NULL PRIMARY KEY,
                        parent_id int NOT NULL REFERENCES \(unescaped: parentIdentifier)(id)
                    )
                    """
                )

                do {
                    _ = try await connection.execute(
                        "INSERT INTO \(unescaped: childIdentifier) (id, parent_id) VALUES (1, 2)"
                    )
                    XCTFail("Insert with invalid foreign key should fail")
                } catch let error as TDSSQLError {
                    XCTAssertEqual(error.code, .server)
                    XCTAssertEqual(error.serverInfo?.number, 547)
                }
            } catch {
                try? await dropTableIfExists(child, on: connection)
                try? await dropTableIfExists(parent, on: connection)
                throw error
            }

            try await dropTableIfExists(child, on: connection)
            try await dropTableIfExists(parent, on: connection)
        }
    }

    func testPingAndCloseDontCrash() async throws {
        let connection = try await TDSConnection.test(on: self.eventLoop)
        let ping = Task {
            try await connection.ping()
        }
        try await connection.close()
        _ = try? await ping.value
    }

    func testPlainQueryWorks() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            _ = try await connection.execute("SELECT 1")
        }
    }

    func testPendingTasksAreExecuted() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await connection.ping()
                }
                group.addTask {
                    try await connection.ping()
                }

                for try await value in group {
                    value
                }
            }
        }
    }

    func testEarlyReturnAfterStreamCompleteDoesNotCrash() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.query(
                """
                SELECT CAST(1 AS int) AS id
                UNION ALL
                SELECT CAST(2 AS int) AS id
                """
            )

            for try await row in rows {
                XCTAssertEqual(try row.decode(column: "id", as: Int.self), 1)
                break
            }

            try await Task.sleep(for: .milliseconds(500))
        }
    }

    func testQueryAfterEarlyStreamExitDoesNotDeadlock() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let rows = try await connection.query(
                """
                WITH numbers AS (
                    SELECT TOP (10000)
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
                    FROM sys.all_objects a CROSS JOIN sys.all_objects b
                )
                SELECT CAST(id AS bigint) AS id
                FROM numbers
                ORDER BY id
                """
            )

            var received: Int64 = 0
            for try await row in rows {
                received += 1
                XCTAssertEqual(try row.decode(Int64.self), received)
                if received > 100 {
                    break
                }
            }

            let nextRows = try await connection.query(
                "SELECT CAST(N'next_query' AS nvarchar(20)) AS value"
            )
            for try await row in nextRows {
                XCTAssertEqual(try row.decode(String.self), "next_query")
                return
            }
            XCTFail("Next query must return exactly one row")
        }
    }

    func testDecodingFailureInStreamCausesDecodingError() async throws {
        var received: Int64 = 0
        try await withTDSConnection(on: self.eventLoop) { connection in
            do {
                let rows = try await connection.query(
                    """
                    WITH numbers AS (
                        SELECT TOP (10000)
                            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                        FROM sys.all_objects a CROSS JOIN sys.all_objects b
                    )
                    SELECT
                        CASE WHEN n = 6969 THEN CAST(NULL AS bigint) ELSE CAST(n AS bigint) END AS id
                    FROM numbers
                    ORDER BY n
                    """
                )
                for try await _ in rows.decode(Int64.self) {
                    received += 1
                }
                XCTFail("Expected stream decoding to fail")
            } catch is TDSDecodingError {
                XCTAssertEqual(received, 6968)
            }
        }
    }

    func testStoredProcedureOutputParameter() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let procedure = uniqueTableName("get_length")
            let procedureIdentifier = sqlIdentifier(procedure)

            do {
                _ = try await connection.execute(
                    """
                    CREATE PROCEDURE dbo.\(unescaped: procedureIdentifier)
                        @value nvarchar(max),
                        @value_length int OUTPUT
                    AS
                    BEGIN
                        SET NOCOUNT ON;
                        SELECT @value_length = LEN(@value);
                    END
                    """
                )

                let result = try await connection.executeRPC(.init(
                    procedure: "dbo.\(procedure)",
                    parameters: [
                        .init(name: "@value", value: .string("Hello, there!")),
                        .init(name: "@value_length", value: .typedNull(.int), isOutput: true),
                    ]
                ))
                let value: Int = try result.decodeOutputParameter(named: "@value_length")
                XCTAssertEqual(value, 13)
            } catch {
                _ = try? await connection.execute("DROP PROCEDURE IF EXISTS dbo.\(unescaped: procedureIdentifier)")
                throw error
            }

            _ = try await connection.execute("DROP PROCEDURE IF EXISTS dbo.\(unescaped: procedureIdentifier)")
        }
    }

    func testStoredProcedureVarcharOutputParameter() async throws {
        try await withTDSConnection(on: self.eventLoop) { connection in
            let procedure = uniqueTableName("get_name")
            let procedureIdentifier = sqlIdentifier(procedure)

            do {
                _ = try await connection.execute(
                    """
                    CREATE PROCEDURE dbo.\(unescaped: procedureIdentifier)
                        @name nvarchar(50) OUTPUT
                    AS
                    BEGIN
                        SET NOCOUNT ON;
                        SELECT @name = N'DummyName';
                    END
                    """
                )

                let result = try await connection.executeRPC(.init(
                    procedure: "dbo.\(procedure)",
                    parameters: [
                        .init(name: "@name", value: .typedNull(.nvarchar(maxBytes: 100)), isOutput: true),
                    ]
                ))
                let value: String = try result.decodeOutputParameter(named: "@name")
                XCTAssertEqual(value, "DummyName")
            } catch {
                _ = try? await connection.execute("DROP PROCEDURE IF EXISTS dbo.\(unescaped: procedureIdentifier)")
                throw error
            }

            _ = try await connection.execute("DROP PROCEDURE IF EXISTS dbo.\(unescaped: procedureIdentifier)")
        }
    }
}
