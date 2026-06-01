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

extension ByteBuffer {

    mutating func prepareSend(
        packetType: TDSPacket.MessageType,
        statusFlags: [TDSPacket.StatusFlag] = [.normal],
        payloadLength: UInt16,
        packetId: UInt8 = 1
    ) {
        var statusByte: UInt8 = 0x00
        for flag in statusFlags {
            statusByte |= flag.rawValue
        }
        self.prepareSend(
            packetTypeByte: packetType.rawValue,
            statusByte: statusByte,
            payloadLength: payloadLength,
            packetId: packetId
        )
    }

    mutating func prepareSend(
        packetTypeByte: UInt8,
        statusByte: UInt8,
        payloadLength: UInt16,
        packetId: UInt8
    ) {
        var position = 0
        self.setInteger(packetTypeByte, at: position)
        position += MemoryLayout<UInt8>.size
        self.setInteger(statusByte, at: position)
        position += MemoryLayout<UInt8>.size
        self.setInteger(payloadLength + UInt16(TDSPacket.headerLength), at: position)
        position += MemoryLayout<UInt16>.size
        self.setInteger(0x00 as UInt16, at: position)  // SPID
        position += MemoryLayout<UInt16>.size
        self.setInteger(packetId, at: position)  // PacketID
        position += MemoryLayout<UInt8>.size
        self.setInteger(0x00 as UInt8, at: position)  // Window
    }
}
