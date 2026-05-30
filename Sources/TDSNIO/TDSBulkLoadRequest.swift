/// A Bulk Load BCP packet body as defined by MS-TDS section 2.2.6.1.
public struct TDSBulkLoadRequest: Sendable, Hashable {
    public struct Column: Sendable, Hashable {
        public enum DataType: Sendable, Hashable {
            case int
            case bit
            case nVarChar(maxBytes: UInt16, collation: [UInt8] = [0x09, 0x04, 0xD0, 0x00, 0x34])
            case varBinary(maxBytes: UInt16)
        }

        public var name: String
        public var dataType: DataType
        public var userType: UInt32
        public var flags: UInt16

        public init(
            name: String,
            dataType: DataType,
            userType: UInt32 = 0,
            flags: UInt16 = 0
        ) {
            self.name = name
            self.dataType = dataType
            self.userType = userType
            self.flags = flags
        }
    }

    public var columns: [Column]
    public var rows: [[TDSData]]

    public init(columns: [Column], rows: [[TDSData]]) {
        self.columns = columns
        self.rows = rows
    }
}
