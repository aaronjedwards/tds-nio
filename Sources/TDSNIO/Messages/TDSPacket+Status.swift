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

import NIO

extension TDSPacket {
    enum StatusFlag: Byte {
        case normal = 0x00
        case eom = 0x01
        case ignoreThisEvent = 0x02
        case resetConnection = 0x08
        case resetConnectionSkipTran = 0x10
    }

    struct Status: OptionSet, Sendable, Hashable {
        let rawValue: UInt8

        static let eom = Status(rawValue: StatusFlag.eom.rawValue)
        static let ignoreThisEvent = Status(rawValue: StatusFlag.ignoreThisEvent.rawValue)
        static let resetConnection = Status(rawValue: StatusFlag.resetConnection.rawValue)
        static let resetConnectionSkipTran = Status(rawValue: StatusFlag.resetConnectionSkipTran.rawValue)
    }
}
