#if DistributedTracingSupport
    import InMemoryTracing
    import Logging
    import NIOCore
    import NIOEmbedded
    import XCTest

    @testable import TDSNIO

    final class DistributedTracingTests: XCTestCase {
        func testQuerySpanAttributes() throws {
            let tracer = InMemoryTracer()
            var configuration = TDSTests.configuration()
            configuration.tracing.tracer = tracer
            let (connection, channel) = try Self.connection(configuration: configuration)

            let span = try XCTUnwrap(connection.startSpan(for: TDSQuery(unsafeSQL: "SELECT 1")))
            span.attributes["db.response.returned_rows"] = Int64(1)
            span.end()
            _ = try channel.finish(acceptAlreadyClosed: true)

            XCTAssertEqual(tracer.finishedSpans.count, 1)
            let finishedSpan = try XCTUnwrap(tracer.finishedSpans.first)
            XCTAssertEqual(finishedSpan.operationName, "SELECT")
            XCTAssertEqual(finishedSpan.kind, .client)
            XCTAssertEqual(finishedSpan.attributes.get("server.address"), .string(configuration.host))
            XCTAssertEqual(finishedSpan.attributes.get("server.port"), .int64(Int64(configuration.port)))
            XCTAssertEqual(finishedSpan.attributes.get("db.system"), .string("mssql"))
            XCTAssertEqual(finishedSpan.attributes.get("db.namespace"), .string("master"))
            XCTAssertEqual(finishedSpan.attributes.get("db.query.summary"), .string("SELECT"))
            XCTAssertEqual(finishedSpan.attributes.get("db.query.text"), .string("SELECT 1"))
            XCTAssertEqual(finishedSpan.attributes.get("db.response.returned_rows"), .int64(1))
            XCTAssertTrue(finishedSpan.errors.isEmpty)
            XCTAssertNil(finishedSpan.status)
        }

        func testErrorSpanAttributes() throws {
            let tracer = InMemoryTracer()
            var configuration = TDSTests.configuration()
            configuration.tracing.tracer = tracer
            let (connection, channel) = try Self.connection(configuration: configuration)

            let span = try XCTUnwrap(connection.startSpan(for: TDSQuery(unsafeSQL: "SELECT id FROM dbo.missing")))
            let serverError = TDSBackendMessage.InfoError(
                number: 208,
                state: 1,
                severity: 16,
                message: "Invalid object name",
                serverName: "sql.example.test",
                procedureName: "",
                lineNumber: 1
            )
            connection.record(TDSSQLError.server(serverError), on: span)
            span.end()
            _ = try channel.finish(acceptAlreadyClosed: true)

            XCTAssertEqual(tracer.finishedSpans.count, 1)
            let finishedSpan = try XCTUnwrap(tracer.finishedSpans.first)
            XCTAssertEqual(finishedSpan.operationName, "SELECT")
            XCTAssertEqual(finishedSpan.kind, .client)
            XCTAssertEqual(finishedSpan.attributes.get("server.address"), .string(configuration.host))
            XCTAssertEqual(finishedSpan.attributes.get("server.port"), .int64(Int64(configuration.port)))
            XCTAssertEqual(finishedSpan.attributes.get("db.system"), .string("mssql"))
            XCTAssertEqual(finishedSpan.attributes.get("db.namespace"), .string("master"))
            XCTAssertEqual(finishedSpan.attributes.get("db.query.summary"), .string("SELECT dbo.missing"))
            XCTAssertEqual(finishedSpan.attributes.get("db.query.text"), .string("SELECT id FROM dbo.missing"))
            XCTAssertEqual(finishedSpan.attributes.get("error.type"), .string("server"))
            XCTAssertEqual(finishedSpan.attributes.get("db.response.status_code"), .string("208"))
            XCTAssertEqual(finishedSpan.errors.count, 1)
            XCTAssertEqual(finishedSpan.status?.code, .error)
        }

        private static func connection(
            configuration: TDSConnection.Configuration
        ) throws -> (TDSConnection, EmbeddedChannel) {
            let channel = EmbeddedChannel()
            let connection = TDSConnection(
                configuration: configuration,
                channel: channel,
                connectionID: 1,
                logger: Logger(label: "tds-nio-tests"),
                protocolVersion: .v7_4
            )
            return (connection, channel)
        }

    }
#endif
