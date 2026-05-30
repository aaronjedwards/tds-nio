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

/// A table-valued parameter (TVP) used as an RPC input parameter.
///
/// TVPs are encoded using the MS-TDS `TVP_TYPE_INFO` structure and are available for TDS 7.3 and
/// later. The server requires the SQL parameter declaration to include `READONLY`.
public struct TDSTableValuedParameter: Sendable, Hashable {
    public struct Column: Sendable, Hashable {
        public enum DataType: Sendable, Hashable {
            case int(maxBytes: UInt8 = 8)
            case bit
            case nVarChar(maxBytes: UInt16, collation: [UInt8] = [0x09, 0x04, 0xD0, 0x00, 0x34])
            case varBinary(maxBytes: UInt16)
        }

        public var dataType: DataType
        public var userType: UInt32
        public var flags: UInt16

        public init(
            dataType: DataType,
            userType: UInt32 = 0,
            flags: UInt16 = 0
        ) {
            self.dataType = dataType
            self.userType = userType
            self.flags = flags
        }
    }

    public var databaseName: String
    public var schemaName: String
    public var typeName: String
    public var columns: [Column]
    public var rows: [[TDSData]]

    public init(
        databaseName: String = "",
        schemaName: String = "dbo",
        typeName: String,
        columns: [Column],
        rows: [[TDSData]]
    ) {
        self.databaseName = databaseName
        self.schemaName = schemaName
        self.typeName = typeName
        self.columns = columns
        self.rows = rows
    }

    var sqlTypeDeclaration: String {
        if self.schemaName.isEmpty {
            return "\(self.typeName) READONLY"
        }
        return "\(self.schemaName).\(self.typeName) READONLY"
    }
}
