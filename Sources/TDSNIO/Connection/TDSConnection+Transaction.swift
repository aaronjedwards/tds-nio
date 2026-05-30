import NIOCore

extension TDSConnection {
    /// Sends a raw TDS transaction-manager request and returns the server token-stream result.
    public func executeTransactionManagerRequest(
        _ request: TDSTransactionManagerRequest,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSQueryResult {
        do {
            return try await self._executeTransactionManagerRequest(request).get()
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            throw error
        }
    }

    /// Begins a local transaction using the TDS transaction manager request packet.
    @discardableResult
    public func beginTransaction(
        isolationLevel: TDSTransactionManagerRequest.IsolationLevel = .current,
        name: String = "",
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSQueryResult {
        let isolationSQL = Self.sqlIsolationLevel(isolationLevel)
        let nameSQL = Self.sqlTransactionName(name)
        return try await self.execute(
            "\(unescaped: isolationSQL)BEGIN TRANSACTION\(unescaped: nameSQL)",
            file: file,
            line: line
        )
    }

    /// Commits the current local transaction.
    public func commit(
        name: String = "",
        beginAfterwards: (isolationLevel: TDSTransactionManagerRequest.IsolationLevel, name: String)? = nil,
        file: String = #fileID,
        line: Int = #line
    ) async throws {
        let nameSQL = Self.sqlTransactionName(name)
        var sql = "COMMIT TRANSACTION\(nameSQL)"
        if let beginAfterwards {
            sql += "; \(Self.sqlIsolationLevel(beginAfterwards.isolationLevel))BEGIN TRANSACTION\(Self.sqlTransactionName(beginAfterwards.name))"
        }
        _ = try await self.execute("\(unescaped: sql)", file: file, line: line)
    }

    /// Rolls back the current local transaction or a named savepoint.
    public func rollback(
        to name: String = "",
        beginAfterwards: (isolationLevel: TDSTransactionManagerRequest.IsolationLevel, name: String)? = nil,
        file: String = #fileID,
        line: Int = #line
    ) async throws {
        let nameSQL = Self.sqlTransactionName(name)
        var sql = "ROLLBACK TRANSACTION\(nameSQL)"
        if let beginAfterwards {
            sql += "; \(Self.sqlIsolationLevel(beginAfterwards.isolationLevel))BEGIN TRANSACTION\(Self.sqlTransactionName(beginAfterwards.name))"
        }
        _ = try await self.execute("\(unescaped: sql)", file: file, line: line)
    }

    /// Creates a transaction savepoint.
    public func saveTransaction(
        name: String,
        file: String = #fileID,
        line: Int = #line
    ) async throws {
        _ = try await self.execute(
            "SAVE TRANSACTION\(unescaped: Self.sqlTransactionName(name))",
            file: file,
            line: line
        )
    }

    func _executeTransactionManagerRequest(
        _ request: TDSTransactionManagerRequest
    ) -> EventLoopFuture<TDSQueryResult> {
        let promise = self.eventLoop.makePromise(of: TDSQueryResult.self)
        self.prepareForNextRequestIfNeeded()
        self.channel.writeAndFlush(TDSTask.transactionManager(request, promise), promise: nil)
        return promise.futureResult
    }

    private static func sqlIsolationLevel(_ isolationLevel: TDSTransactionManagerRequest.IsolationLevel) -> String {
        switch isolationLevel {
        case .current:
            return ""
        case .readUncommitted:
            return "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; "
        case .readCommitted:
            return "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; "
        case .repeatableRead:
            return "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; "
        case .serializable:
            return "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; "
        case .snapshot:
            return "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; "
        }
    }

    private static func sqlTransactionName(_ name: String) -> String {
        guard !name.isEmpty else {
            return ""
        }
        return " [\(name.replacingOccurrences(of: "]", with: "]]"))]"
    }
}
