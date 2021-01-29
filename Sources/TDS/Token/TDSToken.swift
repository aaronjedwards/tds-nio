import NIO

public protocol TDSToken {
    var type: TDSTokens.TokenType { get set }
}

public protocol Metadata {
    var userType: ULong { get set }
    var flags: UShort { get set }
    var dataType: TDSDataType { get set }
    var collation: [Byte] { get set }
    var precision: Int? { get set }
    var scale: Int? { get set }
}

public struct TypeMetadata: Metadata {
    public var userType: ULong
    public var flags: UShort
    public var dataType: TDSDataType
    public var collation: [Byte]
    public var precision: Int?
    public var scale: Int?
    
    init(userType: ULong = 0, flags: UShort = 0, dataType: TDSDataType, collation: [Byte] = [], precision: Int? = nil, scale: Int? = nil) {
        self.userType = userType
        self.flags = flags
        self.dataType = dataType
        self.collation = collation
        self.precision = precision
        self.scale = scale
    }
}

public enum TDSTokens {

    public enum TokenType: UInt8 {
        /// ALTMETADATA
        case altMetadata = 0x88
        /// ALTROW
        case altRow = 0xD3
        /// COLINFO
        case colInfo = 0xA5
        /// COLMETADATA
        case colMetadata = 0x81
        /// DATACLASSIFICATION
        case dataClassification = 0xA3
        /// DONE
        case done = 0xFD
        /// DONEINPROC
        case doneInProc = 0xFF
        /// DONEPROC
        case doneProc = 0xFE
        /// ENVCHANGE
        case envchange = 0xE3
        /// ERROR
        case error = 0xAA
        /// FEATUREEXTACK
        case featureExtAck = 0xAE
        /// FEDAUTHINFO
        case fedAuthInfo = 0xEE
        /// INFO
        case info = 0xAB
        /// LOGINACK
        case loginAck = 0xAD
        /// NBCROW
        case nbcRow = 0xD2
        /// OFFSET
        case offset = 0x78
        /// ORDER
        case order = 0xA9
        /// RETURNSTATUS
        case returnStatus = 0x79
        /// RETURNVALUE
        case returnValue = 0xAC
        /// ROW
        case row = 0xD1
        /// SESSIONSTATE
        case sessionState = 0xE4
        /// SSPI
        case sspi = 0xED
        /// TABNAME
        case tabName = 0xA4
        /// TVP_ROW
        case tvpRow = 0x01
    }

    public struct LoginAckToken: TDSToken {
        public var type: TokenType = .loginAck
        var interface: Byte
        var tdsVersion: DWord
        var progName: String
        var majorVer: Byte
        var minorVer: Byte
        var buildNumHi: Byte
        var buildNumLow: Byte
    }

    public struct ColMetadataToken: TDSToken {
        public var type: TokenType = .colMetadata
        var count: UShort
        var colData: [ColumnData]

        public struct ColumnData: Metadata {
            public var userType: ULong
            public var flags: UShort
            public var dataType: TDSDataType
            public var length: Int
            public var collation: [Byte]
            public var tableName: String?
            public var colName: String
            public var precision: Int?
            public var scale: Int?
        }
    }

    public struct RowToken: TDSToken {
        public var type: TokenType = .row
        var colData: [ColumnData]

        struct ColumnData {
            var textPointer: [Byte]
            var timestamp: [Byte]
            var data: ByteBuffer
        }
    }

    public struct DoneToken: TDSToken {
        public var type: TokenType = .done
        var status: UShort
        var curCmd: UShort
        var doneRowCount: ULongLong
    }

    public struct ErrorInfoToken: TDSToken {
        public var type: TokenType = .error
        var number: Int
        var state: Byte
        var classValue: Byte
        var messageText: String
        var serverName: String
        var procedureName: String
        var lineNumber: Int
    }

    public struct EnvchangeToken<T>: TDSToken {
        public var type: TokenType = .envchange
        var envchangeType: EnvchangeType
        var newValue: T
        var oldValue: T
    }

    public struct RoutingEnvchangeToken: TDSToken {

        struct RoutingData {
            var port: Int
            var alternateServer: String
        }

        public var type: TokenType = .envchange
        var envchangeType: EnvchangeType
        var newValue: RoutingData
        var oldValue: [Byte]
    }

    enum EnvchangeType: Byte {
        case database = 1
        case language = 2
        case characterSet = 3 // TDS 7.0 or ealier
        case packetSize = 4
        case unicodeSortingLocalId = 5 // TDS 7.0 or ealier
        case unicodeSortingFlags = 6
        case sqlCollation = 7
        case beingTransaction = 8
        case commitTransaction = 9 // TDS 7.0 or ealier
        case rollbackTransaction = 10
        case enlistDTCTransaction = 11
        case defectTransaction = 12
        case realTimeLogShipping = 13
        case promoteTransaction = 15
        case transactionManagerAddress = 16
        case transactionEnded = 17
        case resetConnectionAck = 18
        case userInstanceStarted = 19
        case routingInfo = 20
    }
}
