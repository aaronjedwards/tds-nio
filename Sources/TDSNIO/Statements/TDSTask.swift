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
import RegexBuilder

enum TDSTask {
    case sqlBatch(String, EventLoopPromise<TDSQueryResult>, TDSQuery? = nil)
    case rpc(TDSRPC, EventLoopPromise<TDSQueryResult>, TDSQuery? = nil)
    case transactionManager(TDSTransactionManagerRequest, EventLoopPromise<TDSQueryResult>)
    case bulkLoad(TDSBulkLoadRequest, EventLoopPromise<TDSQueryResult>)
    case sqlBatchRows(
        String,
        EventLoopPromise<TDSRowStream>,
        TDSQuery? = nil
    )
    case rpcRows(
        TDSRPC,
        EventLoopPromise<TDSRowStream>,
        TDSQuery? = nil
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
        case .sqlBatchRows(_, let promise, _), .rpcRows(_, let promise, _):
            promise.fail(error)
        case .ping(let promise), .attention(let promise):
            promise.fail(error)
        }
    }

    var query: TDSQuery? {
        switch self {
        case .sqlBatch(let sql, _, let query), .sqlBatchRows(let sql, _, let query):
            return query ?? TDSQuery(unsafeSQL: sql)
        case .rpc(_, _, let query), .rpcRows(_, _, let query):
            return query
        case .transactionManager, .bulkLoad, .ping:
            return nil
        case .attention:
            return nil
        }
    }
}
