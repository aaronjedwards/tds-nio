public protocol TDSDataConvertible {
    static var tdsMetadata: Metadata { get }
    init?(tdsData: TDSData)
    var tdsData: TDSData? { get }
}
