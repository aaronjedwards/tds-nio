import NIO


public enum TDSDataType: UInt8 {
    /// Fixed-Length Data Types
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/859eb3d2-80d3-40f6-a637-414552c9c552

    /// NULLTYPE / Null
    case null = 0x1F
    /// INT1TYPE / TinyInt
    case tinyInt = 0x30
    /// BITTYPE / Bit
    case bit = 0x32
    /// INT2TYPE / SmallInt
    case smallInt = 0x34
    /// INT4TYPE / Int
    case int = 0x38
    /// DATETIM4TYPE / SmallDateTime
    case smallDateTime = 0x3A
    /// FLT4TYPE / Real
    case real = 0x3B
    /// MONEYTYPE / Money
    case money = 0x3C
    /// DATETIMETYPE / DateTime
    case datetime = 0x3D
    /// FLT8TYPE / Float
    case float = 0x3E
    /// MONEY4TYPE / SmallMoney
    case smallMoney = 0x7A
    /// INT8TYPE / BigInt
    case bigInt = 0x7F

    /// Variable-Length Data Types
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/ce3183a6-9d89-47e8-a02f-de5a1a1303de

    /// GUIDTYPE / UniqueIdentifier
    case guid = 0x24
    /// INTNTYPE
    case intn = 0x26
    /// DECIMALTYPE / Decimal (Legacy)
    case decimalLegacy = 0x37
    /// NUMERICTYPE / Numeric (Legacy)
    case numericLegacy = 0x3F
    /// BITNTYPE
    case bitn = 0x68
    /// DECIMALNTYPE / Decimal
    case decimal = 0x6A
    /// NUMERICNTYPE / Numeric
    case numeric = 0x6C
    /// FLTNTYPE
    case floatn = 0x6D
    /// MONEYNTYPE
    case moneyn = 0x6E
    /// DATETIMNTYPE
    case datetimen = 0x6F
    /// DATENTYPE / Date
    case date = 0x28
    /// TIMENTYPE / Time
    case time = 0x29
    /// DATETIME2NTYPE / DateTime
    case datetime2 = 0x2A
    /// DATETIMEOFFSETNTYPE / DateTimeOffset
    case datetimeOffset = 0x2B
    /// CHARTYPE / Char (Legacy)
    case charLegacy = 0x2F
    /// VARCHARTYPE / VarChar(Legacy)
    case varcharLegacy = 0x27
    /// BINARYTYPE / Binary (Legacy)
    case binaryLegacy = 0x2D
    /// VARBINARYTYPE / VarBinary (Legacy)
    case varbinaryLegacy = 0x25
    /// BIGVARBINARYTYPE / VarBinary
    case varbinary = 0xA5
    /// BIGVARCHARTYPE / VarChar
    case varchar = 0xA7
    /// BIGBINARYTYPE / Binary
    case binary = 0xAD
    /// BIGCHARTYPE / Char
    case char = 0xAF
    /// NVARCHARTYPE / NVarChar
    case nvarchar = 0xE7
    /// NCHARTYPE / NChar
    case nchar = 0xEF
    /// XMLTYPE / XML
    case xml = 0xF1
    /// UDTTYPE / CLR UDT
    case clrUdt = 0xF0
    /// TEXTTYPE / Text
    case text = 0x23
    /// IMAGETYPE / Image
    case image = 0x22
    /// NTEXTTYPE / NText
    case nText = 0x63
    /// SSVARIANTTYPE / Sql_Variant
    case sqlVariant = 0x62
    
    var description: String {
        switch self {
        case .null:
            return "null"
        case .tinyInt:
            return "tinyInt"
        case .bit:
            return "bit"
        case .smallInt:
            return "smallInt"
        case .int:
            return "int"
        case .smallDateTime:
            return "smalldatetime"
        case .real:
            return "real"
        case .money:
            return "money"
        case .datetime:
            return "datetime"
        case .float:
            return "float"
        case .smallMoney:
            return "smallmoney"
        case .bigInt:
            return "bigint"
        case .guid:
            return "guid"
        case .intn:
            return "int"
        case .decimalLegacy:
            return "decimal"
        case .numericLegacy:
            return "numeric"
        case .bitn:
            return "bit"
        case .decimal:
            return "decimal"
        case .numeric:
            return "numeric"
        case .floatn:
            return "float"
        case .moneyn:
            return "money"
        case .datetimen:
            return "datetime"
        case .date:
            return "date"
        case .time:
            return "time"
        case .datetime2:
            return "datetime2"
        case .datetimeOffset:
            return "datetimeoffset"
        case .charLegacy:
            return "char"
        case .varcharLegacy:
            return "varchar"
        case .binaryLegacy:
            return "binary"
        case .varbinaryLegacy:
            return "varbinary"
        case .varbinary:
            return "varbinary"
        case .varchar:
            return "varchar"
        case .binary:
            return "binary"
        case .char:
            return "char"
        case .nvarchar:
            return "nvarchar"
        case .nchar:
            return "nchar"
        case .xml:
            return "xml"
        case .clrUdt:
            return "clrudt"
        case .text:
            return "text"
        case .image:
            return "image"
        case .nText:
            return "ntext"
        case .sqlVariant:
            return "sql_variant"
        }
    }

    func isCollationType() -> Bool {
        switch self {
        case .char, .varchar, .text, .nText, .nchar, .nvarchar:
            return true
        default:
            return false
        }
    }

    func isPrecisionType() -> Bool {
        switch self {
        case .numericLegacy, .numeric, .decimalLegacy, .decimal:
            return true
        default:
            return false
        }
    }

    func isScaleType() -> Bool {
        switch self {
        case .numericLegacy, .numeric, .decimalLegacy, .decimal, .time, .datetime2, .datetimeOffset:
            return true
        default:
            return false
        }
    }
}

