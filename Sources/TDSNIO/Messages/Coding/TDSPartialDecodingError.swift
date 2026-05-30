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

struct TDSPartialDecodingError: Error {
    /// A textual description of the error.
    let description: String

    /// The file this error was thrown in.
    let file: String

    /// The line in ``file`` this error was thrown in.
    let line: Int

    static func expectedAtLeastNRemainingBytes(
        _ expected: Int, actual: Int,
        file: String = #fileID, line: Int = #line
    ) -> Self {
        TDSPartialDecodingError(
            description: "Expected at least '\(expected)' remaining bytes. But found \(actual).",
            file: file, line: line
        )
    }

    static func fieldNotDecodable(
        type: Any.Type, file: String = #fileID, line: Int = #line
    ) -> Self {
        TDSPartialDecodingError(
            description: "Could not read '\(type)' from ByteBuffer.", file: file, line: line)
    }

    static func unknownMessageIDReceived(
        messageID: UInt8,
        file: String = #fileID,
        line: Int = #line
    ) -> Self {
        TDSPartialDecodingError(
            description: """
                Received a message with messageID '\(messageID)'. There is no \
                message type associated with this message identifier.
                """,
            file: file,
            line: line
        )
    }
}
