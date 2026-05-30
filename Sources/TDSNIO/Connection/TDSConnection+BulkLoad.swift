//===----------------------------------------------------------------------===//
//
// This source file is part of the TDSNIO open source project
//
// Copyright (c) 2026 TDSNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
// See CONTRIBUTORS.md for the list of TDSNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIOCore

extension TDSConnection {
    /// Sends a Bulk Load BCP stream and returns SQL Server's response token stream.
    public func bulkLoad(
        _ request: TDSBulkLoadRequest,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> TDSQueryResult {
        do {
            return try await self._bulkLoad(request).get()
        } catch var error as TDSSQLError {
            error.file = file
            error.line = line
            throw error
        }
    }

    func _bulkLoad(_ request: TDSBulkLoadRequest) -> EventLoopFuture<TDSQueryResult> {
        let promise = self.eventLoop.makePromise(of: TDSQueryResult.self)
        self.prepareForNextRequestIfNeeded()
        self.channel.writeAndFlush(TDSTask.bulkLoad(request, promise), promise: nil)
        return promise.futureResult
    }
}
