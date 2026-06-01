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
#if DistributedTracingSupport
    import InMemoryTracing
    import Logging
    import NIOCore
    import NIOEmbedded
    import Testing

    @testable import TDSNIO

    @Suite(.timeLimit(.minutes(5)))
    final class DistributedTracingTests {
        @Test func querySpanAttributes() throws {
            let tracer = InMemoryTracer()
            var configuration = TDSTests.configuration()
            configuration.tracing.tracer = tracer
            let (connection, channel) = try Self.connection(configuration: configuration)

            let span = try requireUnwrap(connection.startSpan(for: TDSQuery(unsafeSQL: "SELECT 1")))
            span.attributes["db.response.returned_rows"] = Int64(1)
            span.end()
            _ = try channel.finish(acceptAlreadyClosed: true)

            expectEqual(tracer.finishedSpans.count, 1)
            let finishedSpan = try requireUnwrap(tracer.finishedSpans.first)
            expectEqual(finishedSpan.operationName, "SELECT")
            expectEqual(finishedSpan.kind, .client)
            expectEqual(finishedSpan.attributes.get("server.address"), .string(configuration.host))
            expectEqual(finishedSpan.attributes.get("server.port"), .int64(Int64(configuration.port)))
            expectEqual(finishedSpan.attributes.get("db.system"), .string("mssql"))
            expectEqual(finishedSpan.attributes.get("db.namespace"), .string("master"))
            expectEqual(finishedSpan.attributes.get("db.query.summary"), .string("SELECT"))
            expectEqual(finishedSpan.attributes.get("db.query.text"), .string("SELECT 1"))
            expectEqual(finishedSpan.attributes.get("db.response.returned_rows"), .int64(1))
            expectTrue(finishedSpan.errors.isEmpty)
            expectNil(finishedSpan.status)
        }

        @Test func errorSpanAttributes() throws {
            let tracer = InMemoryTracer()
            var configuration = TDSTests.configuration()
            configuration.tracing.tracer = tracer
            let (connection, channel) = try Self.connection(configuration: configuration)

            let span = try requireUnwrap(connection.startSpan(for: TDSQuery(unsafeSQL: "SELECT id FROM dbo.missing")))
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

            expectEqual(tracer.finishedSpans.count, 1)
            let finishedSpan = try requireUnwrap(tracer.finishedSpans.first)
            expectEqual(finishedSpan.operationName, "SELECT")
            expectEqual(finishedSpan.kind, .client)
            expectEqual(finishedSpan.attributes.get("server.address"), .string(configuration.host))
            expectEqual(finishedSpan.attributes.get("server.port"), .int64(Int64(configuration.port)))
            expectEqual(finishedSpan.attributes.get("db.system"), .string("mssql"))
            expectEqual(finishedSpan.attributes.get("db.namespace"), .string("master"))
            expectEqual(finishedSpan.attributes.get("db.query.summary"), .string("SELECT dbo.missing"))
            expectEqual(finishedSpan.attributes.get("db.query.text"), .string("SELECT id FROM dbo.missing"))
            expectEqual(finishedSpan.attributes.get("error.type"), .string("server"))
            expectEqual(finishedSpan.attributes.get("db.response.status_code"), .string("208"))
            expectEqual(finishedSpan.errors.count, 1)
            expectEqual(finishedSpan.status?.code, .error)
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
