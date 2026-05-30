import Logging
import NIOCore
import RegexBuilder

enum TDSTask {
    case sqlBatch(String, EventLoopPromise<TDSQueryResult>, TDSQuery? = nil)
    case rpc(TDSRPC, EventLoopPromise<TDSQueryResult>, TDSQuery? = nil)
    case transactionManager(TDSTransactionManagerRequest, EventLoopPromise<TDSQueryResult>)
    case bulkLoad(TDSBulkLoadRequest, EventLoopPromise<TDSQueryResult>)
    case sqlBatchRows(
        String,
        EventLoopPromise<TDSRowStream>,
        TDSQuery? = nil,
        onCancel: (@Sendable () -> Void)? = nil
    )
    case rpcRows(
        TDSRPC,
        EventLoopPromise<TDSRowStream>,
        TDSQuery? = nil,
        onCancel: (@Sendable () -> Void)? = nil
    )
    case ping(EventLoopPromise<Void>)
    case attention(EventLoopPromise<Void>)

    func fail(_ error: TDSSQLError) {
        var error = error
        error.query = self.query
        switch self {
        case .sqlBatch(_, let promise, _), .rpc(_, let promise, _),
            .transactionManager(_, let promise), .bulkLoad(_, let promise):
            promise.fail(error)
        case .sqlBatchRows(_, let promise, _, _), .rpcRows(_, let promise, _, _):
            promise.fail(error)
        case .ping(let promise), .attention(let promise):
            promise.fail(error)
        }
    }

    var query: TDSQuery? {
        switch self {
        case .sqlBatch(let sql, _, let query), .sqlBatchRows(let sql, _, let query, _):
            return query ?? TDSQuery(unsafeSQL: sql)
        case .rpc(_, _, let query), .rpcRows(_, _, let query, _):
            return query
        case .transactionManager, .bulkLoad, .ping:
            return nil
        case .attention:
            return nil
        }
    }
}
