import NIO

extension TDSMessages {
    enum DataType: UInt8 {
        /// NULLTYPE / Null
        case nullType = 0x1F
        /// INT1TYPE / TinyInt
        case tinyInt = 0x30
        /// BITTYPE / Bit
        case bit = 0x32
        /// INT2TYPE / SmallInt
        case smallInt = 0x34
        /// INT4TYPE / Int
        case intType = 0x38
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
        /// GUIDTYPE / UniqueIdentifier
        case guid = 0x24
        /// DECIMALTYPE / Decimal (Legacy)
        case decimalLegacy = 0x37
        /// NUMERICTYPE / Numeric (Legacy)
        case numericLegacy = 0x3F
        /// DECIMALNTYPE / Decimal
        case decimal = 0x6A
        /// NUMERICNTYPE / Numeric
        case numeric = 0x6C
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
}
