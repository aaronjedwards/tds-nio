extension TDSData {
    public init(float: Float) {
        self.init(double: Double(float))
    }

    public var float: Float? {
        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .real:
            return value.readFloat()
        case .float:
            return value.readDouble()
                .flatMap { Float($0) }
        default:
            return nil
        }
    }
}

extension Float: TDSDataConvertible {
    public static var tdsMetadata: Metadata {
        return TypeMetadata(dataType: .real)
    }

    public init?(tdsData: TDSData) {
        guard let float = tdsData.float else {
            return nil
        }
        self = float
    }

    public var tdsData: TDSData? {
        return .init(float: self)
    }
}
