import NIO

/// Integers
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/76425d61-416d-4c64-a60b-06072f83e180

extension TDSData {
//    public init(int value: Int) {
//        self.init(fwi: value)
//    }
//
//    public init(int8 value: Int8) {
//        self.init(fwi: value)
//    }
//
//    public init(int16 value: Int16) {
//        self.init(fwi: value)
//    }
//
//    public init(int32 value: Int32) {
//        self.init(fwi: value)
//    }
//
//    public init(int64 value: Int64) {
//        self.init(fwi: value)
//    }
//
//    public init(uint value: UInt) {
//        self.init(fwi: value)
//    }
//
//    public init(uint8 value: UInt8) {
//        self.init(fwi: value)
//    }
//
//    public init(uint16 value: UInt16) {
//        self.init(fwi: value)
//    }
//
//    public init(uint32 value: UInt32) {
//        self.init(fwi: value)
//    }
//
//    public init(uint64 value: UInt64) {
//        self.init(fwi: value)
//    }

    public var int: Int? {
        return fwi()
    }

    public var int8: Int8? {
        return fwi()
    }

    public var int16: Int16? {
        return fwi()
    }

    public var int32: Int32? {
        return fwi()
    }

    public var int64: Int64? {
        return fwi()
    }

    public var uint: UInt? {
        return fwi()
    }

    public var uint8: UInt8? {
        return fwi()
    }

    public var uint16: UInt16? {
        return fwi()
    }

    public var uint32: UInt32? {
        return fwi()
    }

    public var uint64: UInt64? {
        return fwi()
    }
}

private extension TDSData {
//    init<I>(fwi: I) where I: FixedWidthInteger {
//        let capacity: Int
//        let type: TDSDataType
//        switch I.bitWidth {
//        case 8:
//            capacity = 1
//            type = .tinyInt
//        case 16:
//            capacity = 2
//            type = .smallInt
//        case 32:
//            capacity = 3
//            type = .int
//        case 64:
//            capacity = 4
//            type = .bigInt
//        default:
//            fatalError("Cannot encode \(I.self) to TDSData")
//        }
//        var buffer = ByteBufferAllocator().buffer(capacity: capacity)
//        buffer.writeInteger(fwi, endianness: .little)
//        self.init(type: type, value: buffer)
//    }

    func fwi<I>(_ type: I.Type = I.self) -> I?
        where I: FixedWidthInteger
    {
        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .tinyInt:
            guard value.readableBytes == 1 else {
                return nil
            }
            guard let uint8 = value.getInteger(at: value.readerIndex, as: UInt8.self) else {
                return nil
            }
            return I(uint8)
        case .smallInt:
            assert(value.readableBytes == 2)
            guard let int16 = value.readInteger(endianness: .little, as: Int16.self) else {
                return nil
            }
            return I(int16)
        case .int:
            assert(value.readableBytes == 4)
            guard let int32 = value.getInteger(at: value.readerIndex, endianness: .little, as: Int32.self) else {
                return nil
            }
            return I(int32)
        case .bigInt:
            assert(value.readableBytes == 8)
            guard let int64 = value.getInteger(at: value.readerIndex, endianness: .little, as: Int64.self) else {
                return nil
            }
            return I(int64)
        case .intn:
            switch value.readableBytes {
            case 1:
                guard let uint8 = value.getInteger(at: value.readerIndex, as: UInt8.self) else {
                    return nil
                }
                return I(uint8)
            case 2:
                guard let int16 = value.readInteger(endianness: .little, as: Int16.self) else {
                    return nil
                }
                return I(int16)
            case 4:
                guard let int32 = value.getInteger(at: value.readerIndex, endianness: .little, as: Int32.self) else {
                    return nil
                }
                return I(int32)
            case 8:
                guard let int64 = value.getInteger(at: value.readerIndex, endianness: .little, as: Int64.self) else {
                    return nil
                }
                return I(int64)
            default:
                fatalError("Unexpected number of readable bytes for INTNTYPE data type.")
            }
        default:
            return nil
        }
    }
}

//extension FixedWidthInteger {
//    public static var tdsDataType: TDSDataType {
//        switch self.bitWidth {
//        case 8:
//            return .tinyInt
//        case 16:
//            return .smallInt
//        case 32:
//            return .int
//        case 64:
//            return .bigInt
//        default:
//            fatalError("\(self.bitWidth) not supported")
//        }
//    }
//
//    public var tdsData: TDSData? {
//        return .init(fwi: self)
//    }
//
//    public init?(tdsData: TDSData) {
//        guard let fwi = tdsData.fwi(Self.self) else {
//            return nil
//        }
//        self = fwi
//    }
//}

//extension Int: TDSDataConvertible { }
//extension Int8: TDSDataConvertible { }
//extension Int16: TDSDataConvertible { }
//extension Int32: TDSDataConvertible { }
//extension Int64: TDSDataConvertible { }
//extension UInt: TDSDataConvertible { }
//extension UInt8: TDSDataConvertible { }
//extension UInt16: TDSDataConvertible { }
//extension UInt32: TDSDataConvertible { }
//extension UInt64: TDSDataConvertible { }

//extension TDSData: ExpressibleByIntegerLiteral {
//    public init(integerLiteral value: Int) {
//        self.init(int: value)
//    }
//}

