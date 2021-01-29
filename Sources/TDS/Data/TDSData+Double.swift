import NIO

extension TDSData {
    public init(double: Double) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        buffer.writeDouble(double)
        self.init(metadata: Double.tdsMetadata, value: buffer)
    }

    public var double: Double? {
        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .real:
            return value.readFloat().map {
                Double($0)
            }
        case .float:
            return value.readDouble()
        case .floatn:
            switch value.readableBytes {
            case 0:
                return nil
            case 4:
                return value.readFloat(endianness: .little).map {
                    Double($0)
                }
            case 8:
                return value.readDouble(endianness: .little)
            default:
                fatalError("Unexpected number of readable bytes for FLOATNTYPE data type.")
            }
        case .smallMoney:
            return value.readSmallMoney()
        case .money:
            return value.readMoney()
        case .moneyn:
            switch value.readableBytes {
            case 0:
                return nil
            case 4:
                return value.readSmallMoney()
            case 8:
                return value.readMoney()
            default:
                fatalError("Unexpected number of readable bytes for MONEYNTYPE data type.")
            }
        case .numeric, .decimal:
            print(UINT64_MAX)
            guard
                value.readableBytes != 0,
                let scale = metadata.scale,
                let signByte = value.readInteger(endianness: .little, as: UInt8.self)
            else {
                return nil
            }
            
            let sign = signByte == 1 ? 1 : -1

            switch value.readableBytes {
            case 4:
                guard let val = value.readInteger(endianness: .little, as: UInt32.self) else {
                    return nil
                }
                return Double(Int(val) * sign) / pow(10, Double(scale))
            case 8:
                guard let val = value.readInteger(endianness: .little, as: UInt64.self) else {
                    return nil
                }
                return Double(val) * Double(sign) / pow(10, Double(scale))
            case 12, 16:
                fatalError("Yikes! That is a big number.")
            default:
                fatalError("Unexpected number of readable bytes for DECIMALNTYPE or NUMERICNTYPE data type.")
            }
        default:
            return nil
        }
    }
}

extension Double: TDSDataConvertible {
    public static var tdsMetadata: Metadata {
        return TypeMetadata(dataType: .float)
    }

    public init?(tdsData: TDSData) {
        guard let double = tdsData.double else {
            return nil
        }
        self = double
    }

    public var tdsData: TDSData? {
        return .init(double: self)
    }
}
