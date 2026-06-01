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

public import Foundation

/// A JSON document bound to SQL Server's JSON-capable TDS type.
public struct TDSJSON<Value: Encodable & Sendable>: TDSBindable, Sendable {
    public static var tdsSQLType: TDSSQLType { .json }

    public var bytes: [UInt8]

    public init(_ value: Value, encoder: JSONEncoder = JSONEncoder()) throws {
        self.bytes = Array(try encoder.encode(value))
    }

    public var tdsData: TDSData {
        .json(self.bytes)
    }
}

/// A JSON decoding target for values read from SQL Server JSON columns.
public struct TDSJSONValue<Value: Decodable & Sendable>: TDSDecodable, Sendable {
    public var value: Value

    public init(_ value: Value) {
        self.value = value
    }

    public static func decode(from value: TDSData) throws -> TDSJSONValue<Value> {
        try TDSJSONValue(Value.decodeJSON(from: value))
    }
}

extension Decodable where Self: Sendable {
    public static func decodeJSON(
        from value: TDSData,
        decoder: JSONDecoder = JSONDecoder()
    ) throws -> Self {
        let bytes: [UInt8]
        switch value {
        case .json(let json), .xml(let json), .bytes(let json):
            bytes = json
        case .string(let string):
            bytes = Array(string.utf8)
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "JSON")
        default:
            throw TDSDecodingError.typeMismatch(expected: "JSON", actual: value)
        }

        do {
            return try decoder.decode(Self.self, from: Data(bytes))
        } catch {
            throw TDSDecodingError.valueOutOfRange(expected: "JSON", actual: value)
        }
    }
}
