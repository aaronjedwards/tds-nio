import NIO

/// Bit
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/76425d61-416d-4c64-a60b-06072f83e180

extension TDSData {
    public init(bool: Bool) {
        var buffer = ByteBufferAllocator().buffer(capacity: 1)
        buffer.writeInteger(bool ? 1 : 0, as: UInt8.self)
        self.init(metadata: Bool.tdsMetadata, value: buffer)
    }

    public var bool: Bool? {
        guard var value = self.value else {
            return nil
        }
        guard value.readableBytes == 1 else {
            return nil
        }
        guard let byte = value.readInteger(as: UInt8.self) else {
            return nil
        }
        if byte == 0 {
            return false
        } else {
            return true
        }
    }
}

extension TDSData: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) {
        self.init(bool: value)
    }
}

extension Bool: TDSDataConvertible {
    public static var tdsMetadata: Metadata {
        return TypeMetadata(dataType: .bit)
    }

    public var tdsData: TDSData? {
        return .init(bool: self)
    }

    public init?(tdsData: TDSData) {
        guard let bool = tdsData.bool else {
            return nil
        }
        self = bool
    }
}
