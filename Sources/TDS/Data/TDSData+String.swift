extension TDSData {
    public init(string: String) {
        var buffer = ByteBufferAllocator().buffer(capacity: string.utf16.count)
        buffer.writeUTF16String(string)
        self.init(metadata: String.tdsMetadata, value: buffer)
    }
    
    public var string: String? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.metadata.dataType {
        case .charLegacy, .varcharLegacy, .char, .varchar, .nvarchar, .nchar, .text, .nText:
            let val = value.readUTF16String(length: value.readableBytes)
            return val
        default:
            return nil
        }
    }
}

extension TDSData: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(string: value)
    }
}

extension String: TDSDataConvertible {
    public static var tdsMetadata: Metadata {
        return TypeMetadata(dataType: .varchar)
    }
    
    public init?(tdsData: TDSData) {
        guard let string = tdsData.string else {
            return nil
        }
        self = string
    }

    public var tdsData: TDSData? {
        return .init(string: self)
    }
}
