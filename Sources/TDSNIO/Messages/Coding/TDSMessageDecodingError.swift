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

import NIOCore

struct TDSMessageDecodingError: Error {

    /// The backend message packet ID byte.
    let packetID: UInt8

    /// The backend message's payload encoded in base64.
    let payload: String

    /// A textual description of the error.
    let description: String

    /// The file this error was thrown in.
    let file: String

    /// The line in ``file`` this error was thrown in.
    let line: Int

    static func withPartialError(
        _ partialError: TDSPartialDecodingError,
        packetID: UInt8,
        messageBytes: ByteBuffer
    ) -> Self {
        let data = messageBytes.hexDump(format: .plain)

        return TDSMessageDecodingError(
            packetID: packetID,
            payload: data,
            description: partialError.description,
            file: partialError.file,
            line: partialError.line
        )
    }

    static func unknownPacketIDReceived(
        packetID: UInt8,
        packetType: UInt8,
        messageBytes: ByteBuffer,
        file: String = #fileID,
        line: Int = #line
    ) -> Self {
        return TDSMessageDecodingError(
            packetID: packetID,
            payload: messageBytes.hexDump(format: .plain),
            description: """
                Received a message with packetID '\(packetID)'. There is no \
                packet type associated with this packet identifier.
                """,
            file: file,
            line: line
        )
    }

}
