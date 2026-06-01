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

import Foundation
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
        try await self.executeTransactionManagerRequest(
            .begin(isolationLevel: isolationLevel, name: name),
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
        _ = try await self.executeTransactionManagerRequest(
            .commit(name: name, beginAfterwards: beginAfterwards),
            file: file,
            line: line
        )
    }

    /// Rolls back the current local transaction or a named savepoint.
    public func rollback(
        to name: String = "",
        beginAfterwards: (isolationLevel: TDSTransactionManagerRequest.IsolationLevel, name: String)? = nil,
        file: String = #fileID,
        line: Int = #line
    ) async throws {
        _ = try await self.executeTransactionManagerRequest(
            .rollback(name: name, beginAfterwards: beginAfterwards),
            file: file,
            line: line
        )
    }

    /// Creates a transaction savepoint.
    public func saveTransaction(
        name: String,
        file: String = #fileID,
        line: Int = #line
    ) async throws {
        _ = try await self.executeTransactionManagerRequest(
            .savepoint(name: name),
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

}
