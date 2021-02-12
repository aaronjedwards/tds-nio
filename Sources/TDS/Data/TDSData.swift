import NIO
import Foundation

public struct TDSData: CustomStringConvertible, CustomDebugStringConvertible {
    
    public static var null: TDSData {
        return .init(metadata: TypeMetadata(dataType: .null))
    }
    
    /// The object ID of the field's data type.
    public var metadata: Metadata

    public var value: ByteBuffer?

    public init(metadata: Metadata, value: ByteBuffer? = nil) {
        self.metadata = metadata
        self.value = value
    }

    public var description: String {
        guard let value = self.value else {
            return "<null>"
        }

        let description: String?

        switch self.metadata.dataType {
        case .bit, .bitn:
            description = self.bool?.description
        case .tinyInt:
            description = self.int8?.description
        case .smallInt:
            description = self.int16?.description
        case .int:
            description = self.int32?.description
        case .bigInt:
            description = self.int64?.description
        case .real:
            description = self.float?.description
        case .float, .floatn, .numeric, .numericLegacy, .decimal, .decimalLegacy, .smallMoney, .money, .moneyn:
            description = self.double?.description
        case .smallDateTime, .datetime, .datetimen, .date, .time, .datetime2, .datetimeOffset:
            fatalError("Unimplemented")
        case .charLegacy, .varcharLegacy, .char, .varchar, .nvarchar, .nchar, .text, .nText:
            description = self.string?.description
        case .binaryLegacy, .varbinaryLegacy, .varbinary, .binary:
            fatalError("Unimplemented")
        case .guid:
            fatalError("Unimplemented")
        case .xml, .image, .sqlVariant, .clrUdt:
            fatalError("Unimplemented")
        case .null:
            return "<null>"
        case .intn:
            switch value.readableBytes {
            case 1:
                description = self.int8?.description
            case 2:
                description = self.int16?.description
            case 4:
                description = self.int32?.description
            case 8:
                description = self.int64?.description
            default:
                fatalError("Unexpected number of readable bytes for INTNTYPE data type.")
            }
        }

        if let description = description {
            return description
        } else {
            return "0x" + value.readableBytesView.hexdigest()
        }
    }

    public var debugDescription: String {
        return self.description
    }
    
    public var declaration: String {
        return metadata.dataType.description
    }
}

extension TDSData: TDSDataConvertible {
    public static var tdsMetadata: Metadata {
        fatalError("TDSData cannot be statically represented as a single data type")
    }

    public init?(tdsData: TDSData) {
        self = tdsData
    }

    public var tdsData: TDSData? {
        return self
    }
}
