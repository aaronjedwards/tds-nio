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

import Logging
import NIOCore

#if DistributedTracingSupport
    import Tracing
#endif

extension TDSConnection {
    /// Executes a SQL batch and completes when SQL Server sends a DONE token for the batch.
    public func execute(
        _ query: TDSQuery,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSQueryResult {
        #if DistributedTracingSupport
            let span = self.startSpan(for: query)
            defer { span?.end() }
        #endif

        do {
            let result = try await self._execute(query).get()
            #if DistributedTracingSupport
                span?.attributes[
                    self.configuration.tracing.attributeNames.databaseResponseReturnedRows
                ] = Int64(result.rows.count)
            #endif
            return result
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            error.query = query
            #if DistributedTracingSupport
                self.record(error, on: span)
            #endif
            throw error
        }
    }

    /// Executes a query and returns rows from the first result set as an async sequence.
    @discardableResult
    public func query(
        _ query: TDSQuery,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSRowSequence {
        #if DistributedTracingSupport
            let span = self.startSpan(for: query)
            defer { span?.end() }
        #endif

        do {
            let stream = try await self._rowStream(for: query).get()
            return stream.asyncSequence()
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            error.query = query
            #if DistributedTracingSupport
                self.record(error, on: span)
            #endif
            throw error
        }
    }

    /// Executes a query and returns the rows from the first result set as an async sequence.
    public func rows(
        for query: TDSQuery,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSRowSequence {
        try await self.query(query, file: file, line: line)
    }

    /// Executes a query and returns a consumable row stream for the first result set.
    public func rowStream(
        for query: TDSQuery,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSRowStream {
        #if DistributedTracingSupport
            let span = self.startSpan(for: query)
            defer { span?.end() }
        #endif

        do {
            return try await self._rowStream(for: query).get()
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            error.query = query
            #if DistributedTracingSupport
                self.record(error, on: span)
            #endif
            throw error
        }
    }

    func _rowStream(
        for query: TDSQuery
    ) -> EventLoopFuture<TDSRowStream> {
        let promise = self.eventLoop.makePromise(of: TDSRowStream.self)
        self.prepareForNextRequestIfNeeded()
        if query.binds.isEmpty {
            self.writeAndFlush(TDSTask.sqlBatchRows(query.sql, promise, query))
        } else {
            self.writeAndFlush(TDSTask.rpcRows(query.rpcForExecution(), promise, query))
        }
        return promise.futureResult
    }

    func _execute(
        _ query: TDSQuery
    ) -> EventLoopFuture<TDSQueryResult> {
        let promise = self.eventLoop.makePromise(of: TDSQueryResult.self)
        self.prepareForNextRequestIfNeeded()
        if query.binds.isEmpty {
            self.writeAndFlush(TDSTask.sqlBatch(query.sql, promise, query))
        } else {
            self.writeAndFlush(TDSTask.rpc(query.rpcForExecution(), promise, query))
        }
        return promise.futureResult
    }

    /// Sends a lightweight probe to the database server.
    public func ping() async throws {
        try await self._ping().get()
    }

    func _ping() -> EventLoopFuture<Void> {
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.prepareForNextRequestIfNeeded()
        self.writeAndFlush(TDSTask.ping(promise))
        return promise.futureResult
    }

    public func executeRPC(
        _ rpc: TDSRPC,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSQueryResult {
        #if DistributedTracingSupport
            let span = self.startSpan(for: rpc)
            defer { span?.end() }
        #endif

        do {
            let result = try await self._executeRPC(rpc).get()
            #if DistributedTracingSupport
                span?.attributes[
                    self.configuration.tracing.attributeNames.databaseResponseReturnedRows
                ] = Int64(result.rows.count)
            #endif
            return result
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            #if DistributedTracingSupport
                self.record(error, on: span)
            #endif
            throw error
        }
    }

    func _executeRPC(
        _ rpc: TDSRPC
    ) -> EventLoopFuture<TDSQueryResult> {
        let promise = self.eventLoop.makePromise(of: TDSQueryResult.self)
        self.prepareForNextRequestIfNeeded()
        self.writeAndFlush(TDSTask.rpc(rpc, promise))
        return promise.futureResult
    }

    public func cancelCurrentRequest() async throws {
        try await self._cancelCurrentRequest().get()
    }

    func _cancelCurrentRequest() -> EventLoopFuture<Void> {
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.logger.debug("Explicit request cancellation requested; enqueueing TDS attention packet.")
        self.writeAndFlush(TDSTask.attention(promise))
        return promise.futureResult
    }
}

#if DistributedTracingSupport
    extension TDSConnection {
        func startSpan(for query: TDSQuery) -> (any Span)? {
            let summary = query.tracingSummary
            let span = self.tracer?.startSpan(query.tracingOperationName, ofKind: .client)
            span?.updateAttributes { attributes in
                self.applyCommonAttributes(to: &attributes, querySummary: summary, queryText: query.sql)
            }
            return span
        }

        func startSpan(for rpc: TDSRPC) -> (any Span)? {
            let span = self.tracer?.startSpan("RPC", ofKind: .client)
            span?.updateAttributes { attributes in
                self.applyCommonAttributes(
                    to: &attributes, querySummary: "RPC \(rpc.procedure)", queryText: rpc.procedure)
                attributes[self.configuration.tracing.attributeNames.databaseOperationName] = rpc.procedure
            }
            return span
        }

        func applyCommonAttributes(
            to attributes: inout SpanAttributes,
            querySummary: String,
            queryText: String
        ) {
            attributes[self.configuration.tracing.attributeNames.databaseNamespace] = self.databaseNamespace
            attributes[self.configuration.tracing.attributeNames.databaseQuerySummary] = querySummary
            attributes[self.configuration.tracing.attributeNames.databaseQueryText] = queryText
            attributes[self.configuration.tracing.attributeNames.databaseSystem] =
                self.configuration.tracing.attributeValues.databaseSystem
            attributes[self.configuration.tracing.attributeNames.serverAddress] = self.configuration.host
            attributes[self.configuration.tracing.attributeNames.serverPort] = self.configuration.port
        }

        func record(_ error: TDSSQLError, on span: (any Span)?) {
            span?.recordError(error)
            span?.setStatus(SpanStatus(code: .error))
            span?.attributes[self.configuration.tracing.attributeNames.errorType] = error.code.description
            if let number = error.serverInfo?.number {
                span?.attributes[self.configuration.tracing.attributeNames.databaseResponseStatusCode] = "\(number)"
            }
        }
    }
#endif
