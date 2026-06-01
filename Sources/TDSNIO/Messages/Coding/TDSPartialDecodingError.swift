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

    static func unknownTokenReceived(
        token: UInt8,
        file: String = #fileID,
        line: Int = #line
    ) -> Self {
        TDSPartialDecodingError(
            description: """
                Received a token with type '\(token)'. There is no token type \
                associated with this token identifier.
                """,
            file: file,
            line: line
        )
    }

    static func invalidPacketLength(
        _ length: Int,
        minimum: Int,
        file: String = #fileID,
        line: Int = #line
    ) -> Self {
        TDSPartialDecodingError(
            description: "Received a packet length of '\(length)', expected at least '\(minimum)'.",
            file: file,
            line: line
        )
    }

    static func unsupportedRoutingProtocol(
        _ protocolByte: UInt8,
        file: String = #fileID,
        line: Int = #line
    ) -> Self {
        TDSPartialDecodingError(
            description: "Unsupported routing ENVCHANGE protocol byte '\(protocolByte)'.",
            file: file,
            line: line
        )
    }
}
