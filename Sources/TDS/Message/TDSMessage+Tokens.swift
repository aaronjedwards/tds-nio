import NIO

protocol Token {
    var type: TokenType { get set }
}

enum TokenType: UInt8 {
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

extension TDSMessages {

    struct LoginAckToken: Token {
        var type: TokenType = .loginAck
        var interface: Byte
        var tdsVersion: DWord
        var progName: String
        var majorVer: Byte
        var minorVer: Byte
        var buildNumHi: Byte
        var buildNumLow: Byte
    }

    struct ColMetadataToken: Token {
        var type: TokenType = .colMetadata
        var count: UShort
        var colData: [ColumnData]

        struct ColumnData {
            var userType: ULong
            var flags: UShort
            var dataType: DataType
            var length: Int
            var collation: [Byte]
            var tableName: String?
            var colName: String
            var precision: Int?
            var scale: Int?
        }
    }

    struct RowToken: Token {
        var type: TokenType = .row
        var colData: [ColumnData]

        struct ColumnData {
            var textPointer: [Byte]
            var timestamp: [Byte]
            var data: [Byte]
        }
    }

    struct DoneToken: Token {
        var type: TokenType = .done
        var status: UShort
        var curCmd: UShort
        var doneRowCount: ULongLong
    }

    struct ErrorInfoToken: Token {
        var type: TokenType = .error
        var number: Int
        var state: Byte
        var classValue: Byte
        var messageText: String
        var serverName: String
        var procedureName: String
        var lineNumber: Int
    }

    struct BVarcharEnvchangeToken: Token {
        var type: TokenType = .envchange
        var envchangeType: EnvchangeType
        var newValue: String
        var oldValue: String
    }

    struct BVarbyteEnvchangeToken: Token {
        var type: TokenType = .envchange
        var envchangeType: EnvchangeType
        var newValue: [Byte]
        var oldValue: [Byte]
    }

    struct RoutingEnvchangeToken: Token {

        struct RoutingData {
            var port: Int
            var alternateServer: String
        }

        var type: TokenType = .envchange
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

    public static func parseTokenDataStream(messageBuffer: inout ByteBuffer) throws -> [Token] {

        enum State {
            case parsing
            case recievedColMetadata(ColMetadataToken)
        }

        var state: State = .parsing

        var tokens: [Token] = []
        while messageBuffer.readableBytes > 0 {
            guard
                let token = messageBuffer.readByte(),
                let tokenType = TokenType(rawValue: token)
            else {
                throw TDSError.protocolError("Invalid token type in Login7 response")
            }

            switch tokenType {
            case .error, .info:
                let token = try TDSMessages.parseErrorInfoTokenStream(type: tokenType, messageBuffer: &messageBuffer)
                tokens.append(token)
            case .loginAck:
                let token = try TDSMessages.parseLoginAckTokenStream(messageBuffer: &messageBuffer)
                tokens.append(token)
            case .envchange:
                let token = try TDSMessages.parseEnvChangeTokenStream(messageBuffer: &messageBuffer)
                tokens.append(token)
            case .done, .doneInProc, .doneProc :
                let token = try TDSMessages.parseDoneTokenStream(messageBuffer: &messageBuffer)
                tokens.append(token)
            case .colMetadata:
                let token = try TDSMessages.parseColMetadataTokenStream(messageBuffer: &messageBuffer)
                tokens.append(token)
                state = .recievedColMetadata(token)
            case .row:
                switch state {
                case .recievedColMetadata(let colMetadata):
                    let token = try TDSMessages.parseRowTokenStream(colMetadata: colMetadata, messageBuffer: &messageBuffer)
                    tokens.append(token)
                default:
                    throw TDSError.protocolError("Error while parsing row data: no COLMETADATA recieved")
                }
            default:
                throw TDSError.protocolError("Parsing implementation incomplete")
            }
        }
        return tokens
    }

    public static func parseLoginAckTokenStream(messageBuffer: inout ByteBuffer) throws -> LoginAckToken {
        guard
            let _ = messageBuffer.readUShort(),
            let interface = messageBuffer.readByte(),
            let tdsVersion = messageBuffer.readDWord(),
            let progNameLength = messageBuffer.readByte(),
            let progName = messageBuffer.readUTF16String(length: Int(progNameLength)),
            let majorVer = messageBuffer.readByte(),
            let minorVer = messageBuffer.readByte(),
            let buildNumHi = messageBuffer.readByte(),
            let buildNumLow = messageBuffer.readByte()
        else {
            throw TDSError.protocolError("Invalid loginack token")
        }

        let token = LoginAckToken(interface: interface, tdsVersion: tdsVersion, progName: progName, majorVer: majorVer, minorVer: minorVer, buildNumHi: buildNumHi, buildNumLow: buildNumLow)

        return token
    }

    public static func parseColMetadataTokenStream(messageBuffer: inout ByteBuffer) throws -> ColMetadataToken {
        guard
            let count = messageBuffer.readUShort()
        else {
            throw TDSError.protocolError("Invalid COLMETADATA token: Error while reading COUNT of columns")
        }

        var colData: [ColMetadataToken.ColumnData] = []
        for _ in 0...count - 1 {
            guard
                let userType = messageBuffer.readULong(),
                let flags = messageBuffer.readUShort(),
                let dataTypeVal = messageBuffer.readByte(),
                let dataType = DataType.init(rawValue: dataTypeVal)
            else {
                throw TDSError.protocolError("Invalid COLMETADATA token")
            }
            var length: Int
            switch dataType {
            case .sqlVariant, .nText, .text, .image:
                guard let len = messageBuffer.readLongLen() else {
                    throw TDSError.protocolError("Error while reading length")
                }
                length = Int(len)
            case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
                guard let len = messageBuffer.readUShortCharBinLen() else {
                    throw TDSError.protocolError("Error while reading length")
                }
                length = Int(len)
            case .date:
                length = 3
            case .tinyInt, .bit:
                length = 1
            case .smallInt:
                length = 2
            case .intType, .smallDateTime, .real, .smallMoney:
                length = 4
            case .money, .datetime, .float, .bigInt:
                length = 8
            case .nullType:
                length = 0
            default:
                guard let len = messageBuffer.readByteLen() else {
                    throw TDSError.protocolError("Error while reading length.")
                }
                length = Int(len)
            }

            var collationData: [UInt8] = []
            if (dataType.isCollationType()) {
                guard let collationBytes = messageBuffer.readBytes(length: 5) else {
                    throw TDSError.protocolError("Error while reading COLLATION.")
                }
                collationData = collationBytes
            }

            var precision: Int?
            if (dataType.isPrecisionType()) {
                guard
                    let p = messageBuffer.readByte(),
                    p <= 38
                else {
                    throw TDSError.protocolError("Error while reading PRECISION.")
                }
                precision = Int(p)
            }

            var scale: Int?
            if (dataType.isScaleType()) {
                guard let s = messageBuffer.readByte() else {
                    throw TDSError.protocolError("Error while reading SCALE.")
                }

                if let p = precision {
                    guard s <= p else {
                        throw TDSError.protocolError("Invalid SCALE value. Must be less than or equal to precision value.")
                    }
                }

                scale = Int(s)
            }

            // TODO: Read [TableName] and [CryptoMetaData]
            var tableName: String?
            switch dataType {
            case .text, .nText, .image:
                var parts: [String] = []
                guard let numParts = messageBuffer.readByte() else {
                    throw TDSError.protocolError("Error while reading NUMPARTS.")
                }

                for _ in 0...numParts - 1 {
                    guard let partName = messageBuffer.readUSVarchar() else {
                        throw TDSError.protocolError("Error while reading NUMPARTS.")
                    }
                    parts.append(partName)
                }

                tableName = parts.joined(separator: ".")
            default:
                break
            }

            guard let colName = messageBuffer.readBVarchar() else {
                throw TDSError.protocolError("Error while reading column name")
            }

            colData.append(ColMetadataToken.ColumnData(userType: userType, flags: flags, dataType: dataType, length: length, collation: collationData, tableName: tableName, colName: colName, precision: precision, scale: scale))
        }

        let token = ColMetadataToken(count: count, colData: colData)
        return token
    }

    public static func parseRowTokenStream(colMetadata: ColMetadataToken, messageBuffer: inout ByteBuffer) throws -> RowToken {
        var colData: [RowToken.ColumnData] = []

        // TODO: Handle textpointer and timestamp for certain types
        for col in colMetadata.colData {

            var length: Int
            switch col.dataType {
            case .sqlVariant, .nText, .text, .image:
                guard let len = messageBuffer.readLongLen() else {
                    throw TDSError.protocolError("Error while reading length")
                }
                length = Int(len)
            case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
                guard let len = messageBuffer.readUShortCharBinLen() else {
                    throw TDSError.protocolError("Error while reading length")
                }
                length = Int(len)
            case .date:
                length = 3
            case .tinyInt, .bit:
                length = 1
            case .smallInt:
                length = 2
            case .intType, .smallDateTime, .real, .smallMoney:
                length = 4
            case .money, .datetime, .float, .bigInt:
                length = 8
            case .nullType:
                length = 0
            default:
                guard let len = messageBuffer.readByteLen() else {
                    throw TDSError.protocolError("Error while reading length.")
                }
                length = Int(len)
            }

            guard
                let data = messageBuffer.readBytes(length: Int(length))
            else {
                throw TDSError.protocolError("Error while reading row data")
            }

            colData.append(RowToken.ColumnData(textPointer: [], timestamp: [], data: data))
        }

        let token = RowToken(colData: colData)
        return token
    }

    public static func parseDoneTokenStream(messageBuffer: inout ByteBuffer) throws -> DoneToken {
        guard
            let status = messageBuffer.readUShort(),
            let curCmd = messageBuffer.readUShort(),
            let doneRowCount = messageBuffer.readULongLong()
        else {
            throw TDSError.protocolError("Invalid done token")
        }

        let token = DoneToken(status: status, curCmd: curCmd, doneRowCount: doneRowCount)
        return token
    }

    public static func parseEnvChangeTokenStream(messageBuffer: inout ByteBuffer) throws -> Token {
        guard
            let _ = messageBuffer.readUShort(),
            let type = messageBuffer.readByte(),
            let changeType = TDSMessages.EnvchangeType(rawValue: type)
        else {
            throw TDSError.protocolError("Invalid envchange token")
        }

        switch changeType {
        case .database, .language, .characterSet, .packetSize, .realTimeLogShipping, .unicodeSortingLocalId, .unicodeSortingFlags, .userInstanceStarted:
            guard
                let newValue = messageBuffer.readBVarchar(),
                let oldValue = messageBuffer.readBVarchar()
            else {
                throw TDSError.protocolError("Invalid token stream.")
            }

            let token = BVarcharEnvchangeToken(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token
        case .sqlCollation, .beingTransaction, .commitTransaction, .defectTransaction, .rollbackTransaction, .enlistDTCTransaction, .resetConnectionAck, .transactionEnded:
            guard
               let newValue = messageBuffer.readBVarbyte(),
               let oldValue = messageBuffer.readBVarbyte()
           else {
               throw TDSError.protocolError("Invalid token stream.")
           }

           let token = BVarbyteEnvchangeToken(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
           return token
        case .promoteTransaction:
            guard
                let newValue = messageBuffer.readLVarbyte(),
                let _ = messageBuffer.readBytes(length: 1)
            else {
                throw TDSError.protocolError("Invalid token stream.")
            }

            let token = BVarbyteEnvchangeToken(envchangeType: changeType, newValue: newValue, oldValue: [])
            return token
        case .transactionManagerAddress:
                throw TDSError.protocolError("Received unexpected ENVCHANGE Token Type 16: Transaction Manager Address is not used by SQL Server.")
        case .routingInfo:
            guard
                let _ = messageBuffer.readUShort(),
                let protocolByte = messageBuffer.readByte(),
                protocolByte == 0,
                let portNumber = messageBuffer.readUShort(),
                let alternateServer = messageBuffer.readUSVarchar(),
                let oldValue = messageBuffer.readBytes(length: 2)
            else {
                throw TDSError.protocolError("Invalid token stream.")
            }

            let newValue = RoutingEnvchangeToken.RoutingData(port: Int(portNumber), alternateServer: alternateServer)

            let token = RoutingEnvchangeToken(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token
        }
    }

    public static func parseErrorInfoTokenStream(type: TokenType, messageBuffer: inout ByteBuffer) throws -> ErrorInfoToken {
        guard
            let _ = messageBuffer.readUShort(),
            let number = messageBuffer.readLong(),
            let state = messageBuffer.readByte(),
            let classValue = messageBuffer.readByte(),
            let msgText = messageBuffer.readUSVarchar(),
            let serverName = messageBuffer.readBVarchar(),
            let procName = messageBuffer.readBVarchar(),
            let lineNumber = messageBuffer.readLong()
        else {
            throw TDSError.protocolError("Invalid error/info token")
        }

        let token = ErrorInfoToken(type: type, number: Int(number), state: state, classValue: classValue, messageText: msgText, serverName: serverName, procedureName: procName, lineNumber: Int(lineNumber))

        return token
        
    }
}
