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

/// Typealiases for making the translation of the TDS protocol more clear
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/d2ed21d6-527b-46ac-8035-94f6f68eb9a8

// BYTE
typealias Byte = UInt8

// BYTELEN
typealias ByteLen = Byte

// USHORT
typealias UShort = UInt16

// LONG
typealias Long = Int32

// ULONG
typealias ULong = UInt32

// DWORD
typealias DWord = UInt32

// LONGLONG
typealias LongLong = Int64

// ULONGLONG
typealias ULongLong = UInt64

// UCHAR
typealias UChar = Byte

// USHORTLEN
typealias UShortLen = UInt16

// USHORTCHARBINLEN
typealias UShortCharBinLen = UInt16

// LONGLEN
typealias LongLen = Int32

// ULONGLONGLEN
typealias ULongLongLen = UInt64

// PRECISION
typealias Precision = UInt8

// SCALE
typealias Scale = UInt8

// GEN_NULL
typealias GenNull = UInt8

// CHARBIN_NULL
typealias CharBinNull = UInt16

// FRESERVEDBYTE
typealias FReservedByte = Byte

// UNICODECHAR
typealias UnicodeChar = UInt16
