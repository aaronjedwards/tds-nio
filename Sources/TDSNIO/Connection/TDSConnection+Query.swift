import NIOCore

extension TDSConnection {
    /// Executes a SQL batch and completes when SQL Server sends a DONE token for the batch.
    public func execute(
        _ query: TDSQuery,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSQueryResult {
        do {
            return try await self._execute(query).get()
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            error.query = query
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
        do {
            let stream = try await self.rowStream(for: query, file: file, line: line)
            return stream.asyncSequence()
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            error.query = query
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
        do {
            return try await self._rowStream(for: query).get()
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            error.query = query
            throw error
        }
    }

    func _rowStream(
        for query: TDSQuery
    ) -> EventLoopFuture<TDSRowStream> {
        let promise = self.eventLoop.makePromise(of: TDSRowStream.self)
        self.prepareForNextRequestIfNeeded()
        let onCancel: @Sendable () -> Void = { [weak self] in
            guard let self else { return }
            self.eventLoop.execute {
                guard !self.isClosed else { return }
                self.logger.debug(
                    "Row stream consumer requested cancellation; enqueueing TDS attention packet.",
                    metadata: ["tds.query": "\(query.sql)"])
                let promise = self.eventLoop.makePromise(of: Void.self)
                self.channel.writeAndFlush(TDSTask.attention(promise), promise: nil)
            }
        }
        if query.binds.isEmpty {
            self.channel.writeAndFlush(TDSTask.sqlBatchRows(query.sql, promise, query, onCancel: onCancel), promise: nil)
        } else {
            self.channel.writeAndFlush(TDSTask.rpcRows(query.rpcForExecution(), promise, query, onCancel: onCancel), promise: nil)
        }
        return promise.futureResult
    }

    func _execute(
        _ query: TDSQuery
    ) -> EventLoopFuture<TDSQueryResult> {
        let promise = self.eventLoop.makePromise(of: TDSQueryResult.self)
        self.prepareForNextRequestIfNeeded()
        if query.binds.isEmpty {
            self.channel.writeAndFlush(TDSTask.sqlBatch(query.sql, promise, query), promise: nil)
        } else {
            self.channel.writeAndFlush(TDSTask.rpc(query.rpcForExecution(), promise, query), promise: nil)
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
        self.channel.writeAndFlush(TDSTask.ping(promise), promise: nil)
        return promise.futureResult
    }

    public func executeRPC(
        _ rpc: TDSRPC,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSQueryResult {
        do {
            return try await self._executeRPC(rpc).get()
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            throw error
        }
    }

    func _executeRPC(
        _ rpc: TDSRPC
    ) -> EventLoopFuture<TDSQueryResult> {
        let promise = self.eventLoop.makePromise(of: TDSQueryResult.self)
        self.prepareForNextRequestIfNeeded()
        self.channel.writeAndFlush(TDSTask.rpc(rpc, promise), promise: nil)
        return promise.futureResult
    }

    public func cancelCurrentRequest() async throws {
        try await self._cancelCurrentRequest().get()
    }

    func _cancelCurrentRequest() -> EventLoopFuture<Void> {
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.logger.debug("Explicit request cancellation requested; enqueueing TDS attention packet.")
        self.channel.writeAndFlush(TDSTask.attention(promise), promise: nil)
        return promise.futureResult
    }
}
