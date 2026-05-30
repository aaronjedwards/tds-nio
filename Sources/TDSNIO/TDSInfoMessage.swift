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

/// An informational message sent by SQL Server.
///
/// SQL Server sends these with the TDS INFO token for messages such as PRINT output and
/// low-severity notices. They do not fail the active request.
public struct TDSInfoMessage: Sendable, Hashable {
    public let number: Int32
    public let state: UInt8
    public let severity: UInt8
    public let message: String
    public let serverName: String
    public let procedureName: String
    public let lineNumber: UInt32

    init(_ info: TDSBackendMessage.InfoError) {
        self.number = info.number
        self.state = info.state
        self.severity = info.severity
        self.message = info.message
        self.serverName = info.serverName
        self.procedureName = info.procedureName
        self.lineNumber = info.lineNumber
    }
}
