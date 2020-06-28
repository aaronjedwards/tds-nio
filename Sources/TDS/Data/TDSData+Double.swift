import NIO

extension TDSData {
//    public init(double: Double) {
//        var buffer = ByteBufferAllocator().buffer(capacity: 0)
//        buffer.writeDouble(double)
//        self.init(type: .float, value: buffer)
//    }

    public var double: Double? {
        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .real:
            return value.readFloat()
                .flatMap { Double($0) }
        case .float:
            return value.readDouble()
        case .floatn, .numeric, .decimal, .numericLegacy, .decimalLegacy, .smallMoney, .money, .moneyn:
            fatalError("Unimplemented")
        default:
            return nil
        }
    }
}

//extension Double: TDSDataConvertible {
//    public static var tdsDataType: TDSDataType {
//        return .float
//    }
//
//    public init?(tdsData: TDSData) {
//        guard let double = tdsData.double else {
//            return nil
//        }
//        self = double
//    }
//
//    public var tdsData: TDSData? {
//        return .init(double: self)
//    }
//}
