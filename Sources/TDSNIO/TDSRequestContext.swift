import NIOCore

final class TDSRequestContext {
    enum FrontendMessage {
        case sqlBatch(String)
        case rpc(TDSRPC)
        case transactionManager(TDSTransactionManagerRequest)
        case bulkLoad(TDSBulkLoadRequest)
        case ping
    }

    enum ResultMode {
        case bufferedQueryResult
        case rowStream
        case void
    }

    let task: TDSTask
    let frontendMessage: FrontendMessage
    let resultMode: ResultMode
    let query: TDSQuery?

    init(task: TDSTask) {
        self.task = task
        self.query = task.query

        switch task {
        case .sqlBatch(let sql, _, _):
            self.frontendMessage = .sqlBatch(sql)
            self.resultMode = .bufferedQueryResult
        case .rpc(let rpc, _, _):
            self.frontendMessage = .rpc(rpc)
            self.resultMode = .bufferedQueryResult
        case .transactionManager(let request, _):
            self.frontendMessage = .transactionManager(request)
            self.resultMode = .bufferedQueryResult
        case .bulkLoad(let request, _):
            self.frontendMessage = .bulkLoad(request)
            self.resultMode = .bufferedQueryResult
        case .sqlBatchRows(let sql, _, _):
            self.frontendMessage = .sqlBatch(sql)
            self.resultMode = .rowStream
        case .rpcRows(let rpc, _, _):
            self.frontendMessage = .rpc(rpc)
            self.resultMode = .rowStream
        case .ping:
            self.frontendMessage = .ping
            self.resultMode = .void
        case .attention:
            preconditionFailure("Attention is a connection-level control task.")
        }
    }

    func fail(_ error: TDSSQLError) {
        var error = error
        error.query = self.query
        self.task.fail(error)
    }

    var queryResultPromise: EventLoopPromise<TDSQueryResult>? {
        switch self.task {
        case .sqlBatch(_, let promise, _), .rpc(_, let promise, _),
            .transactionManager(_, let promise), .bulkLoad(_, let promise):
            return promise
        case .sqlBatchRows, .rpcRows, .ping, .attention:
            return nil
        }
    }

    var rowStreamPromise: EventLoopPromise<TDSRowStream>? {
        switch self.task {
        case .sqlBatchRows(_, let promise, _), .rpcRows(_, let promise, _):
            return promise
        case .sqlBatch, .rpc, .transactionManager, .bulkLoad, .ping, .attention:
            return nil
        }
    }

    var voidPromise: EventLoopPromise<Void>? {
        switch self.task {
        case .ping(let promise):
            return promise
        case .sqlBatch, .rpc, .transactionManager, .bulkLoad, .sqlBatchRows, .rpcRows, .attention:
            return nil
        }
    }
}
