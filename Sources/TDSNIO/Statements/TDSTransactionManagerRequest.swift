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

/// A transaction manager request packet as defined by MS-TDS section 2.2.6.8.
public struct TDSTransactionManagerRequest: Sendable, Hashable {
    public enum IsolationLevel: UInt8, Sendable, Hashable {
        case current = 0x00
        case readUncommitted = 0x01
        case readCommitted = 0x02
        case repeatableRead = 0x03
        case serializable = 0x04
        case snapshot = 0x05
    }

    enum Payload: Sendable, Hashable {
        case varBytes([UInt8])
        case begin(isolationLevel: IsolationLevel, name: [UInt8])
        case commitOrRollback(name: [UInt8], beginAfterwards: BeginAfterwards?)
        case savepoint(name: [UInt8])
        case none
    }

    struct BeginAfterwards: Sendable, Hashable {
        var isolationLevel: IsolationLevel
        var name: [UInt8]
    }

    var requestType: UInt16
    var payload: Payload

    public static func getDTCAddress() -> Self {
        .init(requestType: 0, payload: .varBytes([]))
    }

    public static func propagateDTCTransaction(_ bytes: [UInt8]) -> Self {
        .init(requestType: 1, payload: .varBytes(bytes))
    }

    public static func begin(
        isolationLevel: IsolationLevel = .current,
        name: [UInt8] = []
    ) -> Self {
        .init(requestType: 5, payload: .begin(isolationLevel: isolationLevel, name: name))
    }

    public static func promote() -> Self {
        .init(requestType: 6, payload: .none)
    }

    public static func commit(
        name: [UInt8] = [],
        beginAfterwards: (isolationLevel: IsolationLevel, name: [UInt8])? = nil
    ) -> Self {
        .init(
            requestType: 7,
            payload: .commitOrRollback(
                name: name,
                beginAfterwards: beginAfterwards.map {
                    .init(isolationLevel: $0.isolationLevel, name: $0.name)
                }
            )
        )
    }

    public static func rollback(
        name: [UInt8] = [],
        beginAfterwards: (isolationLevel: IsolationLevel, name: [UInt8])? = nil
    ) -> Self {
        .init(
            requestType: 8,
            payload: .commitOrRollback(
                name: name,
                beginAfterwards: beginAfterwards.map {
                    .init(isolationLevel: $0.isolationLevel, name: $0.name)
                }
            )
        )
    }

    public static func savepoint(name: [UInt8]) -> Self {
        .init(requestType: 9, payload: .savepoint(name: name))
    }
}
