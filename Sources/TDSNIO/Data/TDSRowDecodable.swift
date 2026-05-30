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

/// A type that can be initialized from a TDS result row.
public protocol TDSRowDecodable: Sendable {
    init(row: TDSRow) throws
}

extension TDSRow {
    public func decode<T: TDSRowDecodable>(
        as type: T.Type = T.self
    ) throws -> T {
        try T(row: self)
    }
}

extension TDSQueryResult {
    public func decodeRows<T: TDSRowDecodable>(
        as type: T.Type = T.self
    ) throws -> [T] {
        try self.rows.map { try $0.decode(as: type) }
    }
}

extension TDSResultSet {
    public func decodeRows<T: TDSRowDecodable>(
        as type: T.Type = T.self
    ) throws -> [T] {
        try self.rows.map { try $0.decode(as: type) }
    }
}

extension TDSRowSequence {
    public func collect<T: TDSRowDecodable>(
        as type: T.Type = T.self
    ) async throws -> [T] {
        var result: [T] = []
        for try await row in self {
            try result.append(row.decode(as: type))
        }
        return result
    }
}
