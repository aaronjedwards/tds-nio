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

/// A session-state update sent by SQL Server.
///
/// SQL Server sends SESSIONSTATE tokens to describe recoverable connection state. The
/// raw state entries are exposed so higher layers can decide whether to cache, log, or
/// participate in recovery behavior.
public struct TDSSessionStateMessage: Sendable, Hashable {
    public struct Entry: Sendable, Hashable {
        public let stateID: UInt8
        public let value: [UInt8]
    }

    public let sequenceNumber: UInt32
    public let status: UInt8
    public let entries: [Entry]

    public var isRecoverable: Bool {
        self.status & 0x01 == 0x01
    }

    init(_ sessionState: TDSBackendMessage.SessionState) {
        self.sequenceNumber = sessionState.sequenceNumber
        self.status = sessionState.status.rawValue
        self.entries = sessionState.entries.map {
            Entry(stateID: $0.stateID, value: $0.value)
        }
    }
}
