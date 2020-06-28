public protocol TDSDataConvertible {
    static var tdsDataType: TDSDataType { get }
    init?(tdsData: TDSData)
    var tdsData: TDSData? { get }
}
