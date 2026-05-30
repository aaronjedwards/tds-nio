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
public import NIOCore

public struct TDSDecodingError: Error, Sendable, Equatable {
    public enum Code: Sendable, Equatable {
        case missingColumn(String)
        case missingColumnIndex(Int)
        case missingOutputParameter(String)
        case nullValue
        case typeMismatch(expected: String, actual: TDSData)
        case valueOutOfRange(expected: String, actual: TDSData)
    }

    public var code: Code
    public var columnName: String?
    public var columnIndex: Int?
    public var dataType: TDSDataType?
    public var file: String
    public var line: Int

    public init(
        code: Code,
        columnName: String? = nil,
        columnIndex: Int? = nil,
        dataType: TDSDataType? = nil,
        file: String = #fileID,
        line: Int = #line
    ) {
        self.code = code
        self.columnName = columnName
        self.columnIndex = columnIndex
        self.dataType = dataType
        self.file = file
        self.line = line
    }

    static func missingColumn(
        _ column: String,
        file: String = #fileID,
        line: Int = #line
    ) -> Self {
        .init(code: .missingColumn(column), file: file, line: line)
    }

    static func missingColumnIndex(
        _ index: Int,
        file: String = #fileID,
        line: Int = #line
    ) -> Self {
        .init(code: .missingColumnIndex(index), columnIndex: index, file: file, line: line)
    }

    static func missingOutputParameter(
        _ name: String,
        file: String = #fileID,
        line: Int = #line
    ) -> Self {
        .init(code: .missingOutputParameter(name), columnName: name, file: file, line: line)
    }

    static func nullValue(expected: String) -> Self {
        .init(code: .nullValue)
    }

    static func typeMismatch(expected: String, actual: TDSData) -> Self {
        .init(code: .typeMismatch(expected: expected, actual: actual))
    }

    static func valueOutOfRange(expected: String, actual: TDSData) -> Self {
        .init(code: .valueOutOfRange(expected: expected, actual: actual))
    }
}

public protocol TDSDecodable: Sendable {
    static func decode(from value: TDSData) throws -> Self
}

extension Optional: TDSDecodable where Wrapped: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Self {
        switch value {
        case .null, .typedNull:
            return nil
        default:
            return try Wrapped.decode(from: value)
        }
    }
}

extension TDSData: TDSDecodable {
    public static func decode(from value: TDSData) throws -> TDSData {
        value
    }
}

extension Bool: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Bool {
        switch value {
        case .bool(let bool):
            return bool
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Bool")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Bool", actual: value)
        }
    }
}

extension UInt8: TDSDecodable {
    public static func decode(from value: TDSData) throws -> UInt8 {
        switch value {
        case .tinyInt(let int):
            return int
        case .smallInt(let int):
            guard let converted = UInt8(exactly: int) else {
                throw TDSDecodingError.valueOutOfRange(expected: "UInt8", actual: value)
            }
            return converted
        case .int32(let int):
            guard let converted = UInt8(exactly: int) else {
                throw TDSDecodingError.valueOutOfRange(expected: "UInt8", actual: value)
            }
            return converted
        case .int(let int):
            guard let converted = UInt8(exactly: int) else {
                throw TDSDecodingError.valueOutOfRange(expected: "UInt8", actual: value)
            }
            return converted
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "UInt8")
        default:
            throw TDSDecodingError.typeMismatch(expected: "UInt8", actual: value)
        }
    }
}

extension Int16: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Int16 {
        switch value {
        case .tinyInt(let int):
            return Int16(int)
        case .smallInt(let int):
            return int
        case .int32(let int):
            guard let converted = Int16(exactly: int) else {
                throw TDSDecodingError.valueOutOfRange(expected: "Int16", actual: value)
            }
            return converted
        case .int(let int):
            guard let converted = Int16(exactly: int) else {
                throw TDSDecodingError.valueOutOfRange(expected: "Int16", actual: value)
            }
            return converted
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Int16")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Int16", actual: value)
        }
    }
}

extension Int32: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Int32 {
        switch value {
        case .tinyInt(let int):
            return Int32(int)
        case .smallInt(let int):
            return Int32(int)
        case .int32(let int):
            return int
        case .int(let int):
            guard let converted = Int32(exactly: int) else {
                throw TDSDecodingError.valueOutOfRange(expected: "Int32", actual: value)
            }
            return converted
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Int32")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Int32", actual: value)
        }
    }
}

extension Int: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Int {
        switch value {
        case .tinyInt(let int):
            return Int(int)
        case .smallInt(let int):
            return Int(int)
        case .int32(let int):
            return Int(int)
        case .int(let int):
            guard let converted = Int(exactly: int) else {
                throw TDSDecodingError.valueOutOfRange(expected: "Int", actual: value)
            }
            return converted
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Int")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Int", actual: value)
        }
    }
}

extension Int64: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Int64 {
        switch value {
        case .tinyInt(let int):
            return Int64(int)
        case .smallInt(let int):
            return Int64(int)
        case .int32(let int):
            return Int64(int)
        case .int(let int):
            return int
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Int64")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Int64", actual: value)
        }
    }
}

extension Float: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Float {
        switch value {
        case .float(let float):
            return float
        case .double(let double):
            return Float(double)
        case .tinyInt(let int):
            return Float(int)
        case .smallInt(let int):
            return Float(int)
        case .int32(let int):
            return Float(int)
        case .int(let int):
            return Float(int)
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Float")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Float", actual: value)
        }
    }
}

extension Double: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Double {
        switch value {
        case .double(let double):
            return double
        case .float(let float):
            return Double(float)
        case .tinyInt(let int):
            return Double(int)
        case .smallInt(let int):
            return Double(int)
        case .int32(let int):
            return Double(int)
        case .int(let int):
            return Double(int)
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Double")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Double", actual: value)
        }
    }
}

extension Decimal: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Decimal {
        switch value {
        case .decimal(let string), .money(let string), .string(let string):
            guard let decimal = Decimal(string: string, locale: Locale(identifier: "en_US_POSIX")) else {
                throw TDSDecodingError.valueOutOfRange(expected: "Decimal", actual: value)
            }
            return decimal
        case .tinyInt(let int):
            return Decimal(int)
        case .smallInt(let int):
            return Decimal(int)
        case .int32(let int):
            return Decimal(int)
        case .int(let int):
            return Decimal(int)
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Decimal")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Decimal", actual: value)
        }
    }
}

extension String: TDSDecodable {
    public static func decode(from value: TDSData) throws -> String {
        switch value {
        case .string(let string), .decimal(let string), .money(let string):
            return string
        case .guid(let guid):
            return guid.stringValue
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "String")
        default:
            throw TDSDecodingError.typeMismatch(expected: "String", actual: value)
        }
    }
}

extension Array: TDSDecodable where Element == UInt8 {
    public static func decode(from value: TDSData) throws -> [UInt8] {
        switch value {
        case .bytes(let bytes), .xml(let bytes), .json(let bytes):
            return bytes
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "[UInt8]")
        default:
            throw TDSDecodingError.typeMismatch(expected: "[UInt8]", actual: value)
        }
    }
}

extension Data: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Data {
        switch value {
        case .bytes(let bytes), .xml(let bytes), .json(let bytes):
            return Data(bytes)
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Data")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Data", actual: value)
        }
    }
}

extension ByteBuffer: TDSDecodable {
    public static func decode(from value: TDSData) throws -> ByteBuffer {
        switch value {
        case .bytes(let bytes), .xml(let bytes), .json(let bytes):
            return ByteBuffer(bytes: bytes)
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "ByteBuffer")
        default:
            throw TDSDecodingError.typeMismatch(expected: "ByteBuffer", actual: value)
        }
    }
}

extension TDSGUID: TDSDecodable {
    public static func decode(from value: TDSData) throws -> TDSGUID {
        switch value {
        case .guid(let guid):
            return guid
        case .string(let string):
            return TDSGUID(string)
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "TDSGUID")
        default:
            throw TDSDecodingError.typeMismatch(expected: "TDSGUID", actual: value)
        }
    }
}

extension UUID: TDSDecodable {
    public static func decode(from value: TDSData) throws -> UUID {
        switch value {
        case .guid(let guid):
            guard let uuid = guid.uuidValue else {
                throw TDSDecodingError.valueOutOfRange(expected: "UUID", actual: value)
            }
            return uuid
        case .string(let string):
            guard let uuid = UUID(uuidString: string) else {
                throw TDSDecodingError.valueOutOfRange(expected: "UUID", actual: value)
            }
            return uuid
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "UUID")
        default:
            throw TDSDecodingError.typeMismatch(expected: "UUID", actual: value)
        }
    }
}

extension TDSDate: TDSDecodable {
    public static func decode(from value: TDSData) throws -> TDSDate {
        switch value {
        case .date(let date):
            return date
        case .datetime(let dateTime), .datetime2(let dateTime):
            return dateTime.date
        case .datetimeOffset(let dateTimeOffset):
            return dateTimeOffset.dateTime.date
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "TDSDate")
        default:
            throw TDSDecodingError.typeMismatch(expected: "TDSDate", actual: value)
        }
    }
}

extension TDSTime: TDSDecodable {
    public static func decode(from value: TDSData) throws -> TDSTime {
        switch value {
        case .time(let time):
            return time
        case .datetime(let dateTime), .datetime2(let dateTime):
            return dateTime.time
        case .datetimeOffset(let dateTimeOffset):
            return dateTimeOffset.dateTime.time
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "TDSTime")
        default:
            throw TDSDecodingError.typeMismatch(expected: "TDSTime", actual: value)
        }
    }
}

extension TDSDateTime: TDSDecodable {
    public static func decode(from value: TDSData) throws -> TDSDateTime {
        switch value {
        case .datetime(let dateTime), .datetime2(let dateTime):
            return dateTime
        case .datetimeOffset(let dateTimeOffset):
            return dateTimeOffset.dateTime
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "TDSDateTime")
        default:
            throw TDSDecodingError.typeMismatch(expected: "TDSDateTime", actual: value)
        }
    }
}

extension TDSDateTimeOffset: TDSDecodable {
    public static func decode(from value: TDSData) throws -> TDSDateTimeOffset {
        switch value {
        case .datetimeOffset(let dateTimeOffset):
            return dateTimeOffset
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "TDSDateTimeOffset")
        default:
            throw TDSDecodingError.typeMismatch(expected: "TDSDateTimeOffset", actual: value)
        }
    }
}

extension Date: TDSDecodable {
    public static func decode(from value: TDSData) throws -> Date {
        let decoded: Date?
        switch value {
        case .date(let date):
            decoded = date.dateValue()
        case .datetime(let dateTime), .datetime2(let dateTime):
            decoded = dateTime.dateValue()
        case .datetimeOffset(let dateTimeOffset):
            decoded = dateTimeOffset.dateValue()
        case .null, .typedNull:
            throw TDSDecodingError.nullValue(expected: "Date")
        default:
            throw TDSDecodingError.typeMismatch(expected: "Date", actual: value)
        }
        guard let decoded else {
            throw TDSDecodingError.valueOutOfRange(expected: "Date", actual: value)
        }
        return decoded
    }
}
