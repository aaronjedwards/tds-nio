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

public import Foundation
import NIOConcurrencyHelpers
import NIOCore

/// A TDS SQL query, that can be executed on a TDS server. Contains the raw sql string and bindings.
public struct TDSQuery: Sendable, Hashable {
    /// The query string.
    public var sql: String
    /// The query binds.
    public var binds: TDSBindings

    public init(
        unsafeSQL sql: String,
        binds: TDSBindings = TDSBindings()
    ) {
        self.sql = sql
        self.binds = binds
    }
}

extension TDSQuery: ExpressibleByStringInterpolation {
    public init(stringLiteral value: StringLiteralType) {
        self.sql = value
        self.binds = TDSBindings()
    }

    public init(stringInterpolation: StringInterpolation) {
        self.sql = stringInterpolation.sql
        self.binds = stringInterpolation.binds
    }

    public struct StringInterpolation: StringInterpolationProtocol {
        public typealias StringLiteralType = String

        @usableFromInline
        var sql: String
        @usableFromInline
        var binds: TDSBindings

        public init(literalCapacity: Int, interpolationCount: Int) {
            self.sql = ""
            self.sql.reserveCapacity(literalCapacity + interpolationCount * 4)
            self.binds = TDSBindings(capacity: interpolationCount)
        }

        public mutating func appendLiteral(_ literal: String) {
            self.sql.append(literal)
        }

        public mutating func appendInterpolation(_ value: TDSData) {
            let name = self.binds.append(value)
            self.sql.append(name)
        }

        public mutating func appendInterpolation<Value: TDSBindable>(_ value: Value) {
            self.appendInterpolation(value.tdsData)
        }

        public mutating func appendInterpolation<Value: TDSBindable>(_ value: Value?) {
            self.appendInterpolation(value?.tdsData ?? .typedNull(Value.tdsSQLType))
        }

        public mutating func appendInterpolation(_ value: String) {
            self.appendInterpolation(.string(value))
        }

        public mutating func appendInterpolation(_ value: String?) {
            self.appendInterpolation(value.map(TDSData.string) ?? .typedNull(.nvarchar()))
        }

        public mutating func appendInterpolation(_ value: Bool) {
            self.appendInterpolation(.bool(value))
        }

        public mutating func appendInterpolation(_ value: Bool?) {
            self.appendInterpolation(value.map(TDSData.bool) ?? .typedNull(.bit))
        }

        public mutating func appendInterpolation(_ value: UInt8) {
            self.appendInterpolation(.tinyInt(value))
        }

        public mutating func appendInterpolation(_ value: UInt8?) {
            self.appendInterpolation(value.map(TDSData.tinyInt) ?? .typedNull(.tinyInt))
        }

        public mutating func appendInterpolation(_ value: Int16) {
            self.appendInterpolation(.smallInt(value))
        }

        public mutating func appendInterpolation(_ value: Int16?) {
            self.appendInterpolation(value.map(TDSData.smallInt) ?? .typedNull(.smallInt))
        }

        public mutating func appendInterpolation(_ value: Int32) {
            self.appendInterpolation(.int32(value))
        }

        public mutating func appendInterpolation(_ value: Int32?) {
            self.appendInterpolation(value.map(TDSData.int32) ?? .typedNull(.int))
        }

        public mutating func appendInterpolation(_ value: Int) {
            self.appendInterpolation(.int(Int64(value)))
        }

        public mutating func appendInterpolation(_ value: Int?) {
            self.appendInterpolation(value.map { .int(Int64($0)) } ?? .typedNull(.bigInt))
        }

        public mutating func appendInterpolation(_ value: Int64) {
            self.appendInterpolation(.int(value))
        }

        public mutating func appendInterpolation(_ value: Int64?) {
            self.appendInterpolation(value.map(TDSData.int) ?? .typedNull(.bigInt))
        }

        public mutating func appendInterpolation(_ value: Float) {
            self.appendInterpolation(.float(value))
        }

        public mutating func appendInterpolation(_ value: Float?) {
            self.appendInterpolation(value.map(TDSData.float) ?? .typedNull(.real))
        }

        public mutating func appendInterpolation(_ value: Double) {
            self.appendInterpolation(.double(value))
        }

        public mutating func appendInterpolation(_ value: Double?) {
            self.appendInterpolation(value.map(TDSData.double) ?? .typedNull(.float))
        }

        public mutating func appendInterpolation(_ value: Decimal) {
            self.appendInterpolation(.decimal(Self.decimalString(value)))
        }

        public mutating func appendInterpolation(_ value: Decimal?) {
            self.appendInterpolation(value.map { .decimal(Self.decimalString($0)) } ?? .typedNull(.decimal()))
        }

        public mutating func appendInterpolation(_ value: [UInt8]) {
            self.appendInterpolation(.bytes(value))
        }

        public mutating func appendInterpolation(_ value: [UInt8]?) {
            self.appendInterpolation(value.map(TDSData.bytes) ?? .typedNull(.varbinary()))
        }

        public mutating func appendInterpolation(_ value: Data) {
            self.appendInterpolation(.bytes(Array(value)))
        }

        public mutating func appendInterpolation(_ value: Data?) {
            self.appendInterpolation(value.map { .bytes(Array($0)) } ?? .typedNull(.varbinary()))
        }

        public mutating func appendInterpolation(_ value: TDSGUID?) {
            self.appendInterpolation(value.map(TDSData.guid) ?? .typedNull(.uniqueIdentifier))
        }

        public mutating func appendInterpolation(_ value: UUID) {
            self.appendInterpolation(.guid(TDSGUID(value)))
        }

        public mutating func appendInterpolation(_ value: UUID?) {
            self.appendInterpolation(value.map { .guid(TDSGUID($0)) } ?? .typedNull(.uniqueIdentifier))
        }

        public mutating func appendInterpolation(_ value: TDSDate?) {
            self.appendInterpolation(value.map(TDSData.date) ?? .typedNull(.date))
        }

        public mutating func appendInterpolation(_ value: TDSTime?) {
            self.appendInterpolation(value.map(TDSData.time) ?? .typedNull(.time()))
        }

        public mutating func appendInterpolation(_ value: TDSDateTime?) {
            self.appendInterpolation(value.map(TDSData.datetime2) ?? .typedNull(.datetime2()))
        }

        public mutating func appendInterpolation(_ value: TDSDateTimeOffset?) {
            self.appendInterpolation(value.map(TDSData.datetimeOffset) ?? .typedNull(.datetimeOffset()))
        }

        public mutating func appendInterpolation(_ value: Date) {
            self.appendInterpolation(.datetime2(TDSDateTime(value)))
        }

        public mutating func appendInterpolation(_ value: Date?) {
            self.appendInterpolation(value.map { .datetime2(TDSDateTime($0)) } ?? .typedNull(.datetime2()))
        }

        public mutating func appendInterpolation(unescaped interpolation: String) {
            self.sql.append(interpolation)
        }

        private static func decimalString(_ value: Decimal) -> String {
            NSDecimalNumber(decimal: value).stringValue
        }
    }
}

extension TDSQuery: CustomStringConvertible {
    public var description: String {
        "\(self.sql) \(self.binds)"
    }
}

extension TDSQuery: CustomDebugStringConvertible {
    public var debugDescription: String {
        "TDSQuery(sql: \(String(describing: self.sql)), binds: \(String(reflecting: self.binds)))"
    }
}

public struct TDSBindings: Sendable, Hashable {
    public private(set) var parameters: [TDSRPC.Parameter]

    public var count: Int {
        self.parameters.count
    }

    public var isEmpty: Bool {
        self.parameters.isEmpty
    }

    public init() {
        self.parameters = []
    }

    init(capacity: Int) {
        self.parameters = []
        self.parameters.reserveCapacity(capacity)
    }

    @discardableResult
    public mutating func append(_ value: TDSData, name: String? = nil) -> String {
        let parameterName = name ?? "@p\(self.parameters.count)"
        self.parameters.append(.init(name: parameterName, value: value))
        return parameterName
    }
}

extension TDSBindings: CustomStringConvertible {
    public var description: String {
        self.parameters.description
    }
}

extension TDSBindings: CustomDebugStringConvertible {
    public var debugDescription: String {
        "TDSBindings(parameters: \(String(reflecting: self.parameters)))"
    }
}

extension TDSQuery {
    func rpcForExecution() -> TDSRPC {
        var parameters = [
            TDSRPC.Parameter(name: "@stmt", value: .string(self.sql)),
            TDSRPC.Parameter(name: "@params", value: .string(self.binds.declarationList)),
        ]
        parameters.append(contentsOf: self.binds.parameters)
        return TDSRPC(procedure: "sp_executesql", parameters: parameters)
    }
}

#if DistributedTracingSupport
    extension TDSQuery {
        var tracingOperationName: String {
            Self.tracingTokens(for: self.sql).first?.uppercased() ?? "SQL"
        }

        var tracingSummary: String {
            let tokens = Self.tracingTokens(for: self.sql)
            guard let first = tokens.first?.uppercased() else {
                return "SQL"
            }

            switch first {
            case "SELECT":
                if let fromIndex = tokens.firstIndex(where: { $0.uppercased() == "FROM" }),
                    tokens.indices.contains(tokens.index(after: fromIndex))
                {
                    return "SELECT \(Self.normalizedIdentifier(tokens[tokens.index(after: fromIndex)]))"
                }
                return "SELECT"
            case "INSERT":
                return Self.summary(first, following: "INTO", in: tokens)
            case "UPDATE", "MERGE":
                return Self.summary(first, followingFirstTokenIn: tokens)
            case "DELETE":
                return Self.summary(first, following: "FROM", in: tokens)
            case "EXEC", "EXECUTE":
                return Self.summary("EXEC", followingFirstTokenIn: tokens)
            default:
                return first
            }
        }

        private static func summary(_ operation: String, following keyword: String, in tokens: [String]) -> String {
            if let index = tokens.firstIndex(where: { $0.uppercased() == keyword }),
                tokens.indices.contains(tokens.index(after: index))
            {
                return "\(operation) \(Self.normalizedIdentifier(tokens[tokens.index(after: index)]))"
            }
            return operation
        }

        private static func summary(_ operation: String, followingFirstTokenIn tokens: [String]) -> String {
            guard tokens.count > 1 else {
                return operation
            }
            return "\(operation) \(Self.normalizedIdentifier(tokens[1]))"
        }

        private static func tracingTokens(for sql: String) -> [String] {
            sql.split { character in
                character.isWhitespace || character == "(" || character == ")" || character == "," || character == ";"
            }
            .map(String.init)
            .filter { !$0.isEmpty }
        }

        private static func normalizedIdentifier(_ token: String) -> String {
            token.trimmingCharacters(in: CharacterSet(charactersIn: "[]\"`"))
        }
    }
#endif

extension TDSBindings {
    var declarationList: String {
        self.parameters
            .map { "\($0.name) \($0.value.sqlTypeDeclaration)" }
            .joined(separator: ", ")
    }
}

public struct TDSRPC: Sendable, Hashable {
    public var procedure: String
    public var parameters: [Parameter]

    public init(
        procedure: String,
        parameters: [Parameter] = []
    ) {
        self.procedure = procedure
        self.parameters = parameters
    }

    public struct Parameter: Sendable, Hashable {
        public var name: String
        public var value: TDSData
        public var isOutput: Bool

        public init(
            name: String = "",
            value: TDSData,
            isOutput: Bool = false
        ) {
            self.name = name
            self.value = value
            self.isOutput = isOutput
        }
    }
}

extension TDSData {
    var sqlTypeDeclaration: String {
        switch self {
        case .null:
            "nvarchar(max)"
        case .typedNull(let type):
            type.sqlTypeDeclaration
        case .bool:
            "bit"
        case .tinyInt:
            "tinyint"
        case .smallInt:
            "smallint"
        case .int32:
            "int"
        case .int:
            "bigint"
        case .float:
            "real"
        case .double:
            "float"
        case .decimal(let value):
            Self.decimalDeclaration(value)
        case .money:
            "money"
        case .date:
            "date"
        case .time(let value):
            "time(\(value.scale))"
        case .datetime:
            "datetime"
        case .datetime2(let value):
            "datetime2(\(value.time.scale))"
        case .datetimeOffset(let value):
            "datetimeoffset(\(value.dateTime.time.scale))"
        case .guid:
            "uniqueidentifier"
        case .string:
            "nvarchar(max)"
        case .bytes:
            "varbinary(max)"
        case .xml:
            "xml"
        case .json:
            "nvarchar(max)"
        case .table(let value):
            value.sqlTypeDeclaration
        }
    }

    private static func decimalDeclaration(_ value: String) -> String {
        var text = value.trimmingCharacters(in: .whitespacesAndNewlines)
        if text.first == "-" || text.first == "+" {
            text.removeFirst()
        }
        let pieces = text.split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false)
        let integerDigits = pieces.first?.filter(\.isNumber).count ?? 0
        let scale = pieces.count == 2 ? pieces[1].filter(\.isNumber).count : 0
        let precision = min(max(integerDigits + scale, 1), 38)
        return "decimal(\(precision), \(min(scale, 38)))"
    }
}

extension TDSSQLType {
    var sqlTypeDeclaration: String {
        switch self {
        case .bit:
            "bit"
        case .tinyInt:
            "tinyint"
        case .smallInt:
            "smallint"
        case .int:
            "int"
        case .bigInt:
            "bigint"
        case .real:
            "real"
        case .float:
            "float"
        case .decimal(let precision, let scale):
            "decimal(\(Self.clampedPrecision(precision)), \(Self.clampedScale(scale, precision: precision)))"
        case .money:
            "money"
        case .date:
            "date"
        case .time(let scale):
            "time(\(Self.clampedTemporalScale(scale)))"
        case .datetime:
            "datetime"
        case .datetime2(let scale):
            "datetime2(\(Self.clampedTemporalScale(scale)))"
        case .datetimeOffset(let scale):
            "datetimeoffset(\(Self.clampedTemporalScale(scale)))"
        case .uniqueIdentifier:
            "uniqueidentifier"
        case .char(let maxBytes):
            "char(\(Self.normalizedFixedSingleByteMaxBytes(maxBytes)))"
        case .varchar(let maxBytes):
            if maxBytes == UInt16.max {
                "varchar(max)"
            } else {
                "varchar(\(Self.normalizedVarCharMaxBytes(maxBytes)))"
            }
        case .nchar(let maxBytes):
            "nchar(\(Int(Self.normalizedFixedNCharMaxBytes(maxBytes)) / 2))"
        case .nvarchar(let maxBytes):
            if maxBytes == UInt16.max {
                "nvarchar(max)"
            } else {
                "nvarchar(\(Int(Self.normalizedNVarCharMaxBytes(maxBytes)) / 2))"
            }
        case .binary(let maxBytes):
            "binary(\(Self.normalizedFixedSingleByteMaxBytes(maxBytes)))"
        case .varbinary(let maxBytes):
            if maxBytes == UInt16.max {
                "varbinary(max)"
            } else {
                "varbinary(\(Int(Self.normalizedVarBinaryMaxBytes(maxBytes))))"
            }
        case .xml:
            "xml"
        case .json:
            "nvarchar(max)"
        }
    }

    static func clampedPrecision(_ precision: UInt8) -> UInt8 {
        min(max(precision, 1), 38)
    }

    static func clampedScale(_ scale: UInt8, precision: UInt8) -> UInt8 {
        min(scale, Self.clampedPrecision(precision))
    }

    static func clampedTemporalScale(_ scale: UInt8) -> UInt8 {
        min(scale, 7)
    }

    static func normalizedNVarCharMaxBytes(_ maxBytes: UInt16) -> UInt16 {
        guard maxBytes != UInt16.max else {
            return UInt16.max
        }
        return max(2, maxBytes & ~1)
    }

    static func normalizedFixedNCharMaxBytes(_ maxBytes: UInt16) -> UInt16 {
        min(Self.normalizedNVarCharMaxBytes(maxBytes), 8_000)
    }

    static func normalizedVarCharMaxBytes(_ maxBytes: UInt16) -> UInt16 {
        guard maxBytes != UInt16.max else {
            return UInt16.max
        }
        return max(1, maxBytes)
    }

    static func normalizedFixedSingleByteMaxBytes(_ maxBytes: UInt16) -> UInt16 {
        min(max(1, maxBytes), 8_000)
    }

    static func normalizedVarBinaryMaxBytes(_ maxBytes: UInt16) -> UInt16 {
        guard maxBytes != UInt16.max else {
            return UInt16.max
        }
        return max(1, maxBytes)
    }
}
