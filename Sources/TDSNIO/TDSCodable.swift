import Foundation
import NIOCore

/// A type that can represent itself as a TDS RPC/query bind value.
///
/// Conforming types can be used directly in ``TDSQuery`` string interpolation.
/// ``tdsSQLType`` is used when an optional value of the conforming type is `nil`.
public protocol TDSBindable: Sendable {
    static var tdsSQLType: TDSSQLType { get }

    var tdsData: TDSData { get }
}

/// A type that can be bound into a query and decoded from a row or output parameter.
public typealias TDSCodable = TDSBindable & TDSDecodable

extension Bool: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .bit }

    public var tdsData: TDSData { .bool(self) }
}

extension UInt8: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .tinyInt }

    public var tdsData: TDSData { .tinyInt(self) }
}

extension Int16: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .smallInt }

    public var tdsData: TDSData { .smallInt(self) }
}

extension Int32: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .int }

    public var tdsData: TDSData { .int32(self) }
}

extension Int: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .bigInt }

    public var tdsData: TDSData { .int(Int64(self)) }
}

extension Int64: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .bigInt }

    public var tdsData: TDSData { .int(self) }
}

extension Float: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .real }

    public var tdsData: TDSData { .float(self) }
}

extension Double: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .float }

    public var tdsData: TDSData { .double(self) }
}

extension Decimal: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .decimal() }

    public var tdsData: TDSData {
        .decimal(NSDecimalNumber(decimal: self).stringValue)
    }
}

extension String: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .nvarchar() }

    public var tdsData: TDSData { .string(self) }
}

extension Array: TDSBindable where Element == UInt8 {
    public static var tdsSQLType: TDSSQLType { .varbinary() }

    public var tdsData: TDSData { .bytes(self) }
}

extension Data: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .varbinary() }

    public var tdsData: TDSData { .bytes(Array(self)) }
}

extension ByteBuffer: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .varbinary() }

    public var tdsData: TDSData {
        .bytes(Array(self.readableBytesView))
    }
}

extension TDSGUID: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .uniqueIdentifier }

    public var tdsData: TDSData { .guid(self) }
}

extension UUID: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .uniqueIdentifier }

    public var tdsData: TDSData { .guid(TDSGUID(self)) }
}

extension TDSDate: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .date }

    public var tdsData: TDSData { .date(self) }
}

extension TDSTime: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .time() }

    public var tdsData: TDSData { .time(self) }
}

extension TDSDateTime: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .datetime2() }

    public var tdsData: TDSData { .datetime2(self) }
}

extension TDSDateTimeOffset: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .datetimeOffset() }

    public var tdsData: TDSData { .datetimeOffset(self) }
}

extension Date: TDSBindable {
    public static var tdsSQLType: TDSSQLType { .datetime2() }

    public var tdsData: TDSData { .datetime2(TDSDateTime(self)) }
}
