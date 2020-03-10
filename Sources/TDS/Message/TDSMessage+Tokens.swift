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
        var interface: UInt8
        var tdsVersion: DWord
        var progName: String
        var majorVer: UInt8
        var minorVer: UInt8
        var buildNumHi: UInt8
        var buildNumLow: UInt8
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
            var collation: [UInt8]
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
            var textPointer: [UInt8]
            var timestamp: [UInt8]
            var data: [UInt8]
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
        var state: UInt8
        var classValue: UInt8
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
        var newValue: String
        var oldValue: String
    }

    enum EnvchangeType: UInt8 {
        case database = 1
        case language = 2
        case characterSet = 3
        case packetSize = 4
        case unicodeSortingLocalId = 5
        case unicodeSortingFlags = 6
        case sqlCollation = 7
        case beingTransaction = 8
        case commitTransaction = 9
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
                let token = messageBuffer.readInteger(as: UInt8.self),
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
                if let token = try TDSMessages.parseEnvChangeTokenStream(messageBuffer: &messageBuffer) {
                    tokens.append(token)
                }
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
            let _ = messageBuffer.readInteger(endianness: .little, as: UInt16.self),
            let interface = messageBuffer.readInteger(as: UInt8.self),
            let tdsVersion = messageBuffer.readInteger(as: DWord.self),
            let progNameLength = messageBuffer.readInteger(as: UInt8.self),
            let progNameBytes = messageBuffer.readBytes(length: Int(progNameLength * 2)),
            let progName = String(bytes: progNameBytes, encoding: .utf16LittleEndian),
            let majorVer = messageBuffer.readInteger(as: UInt8.self),
            let minorVer = messageBuffer.readInteger(as: UInt8.self),
            let buildNumHi = messageBuffer.readInteger(as: UInt8.self),
            let buildNumLow = messageBuffer.readInteger(as: UInt8.self)
        else {
            throw TDSError.protocolError("Invalid loginack token")
        }

        let token = LoginAckToken(interface: interface, tdsVersion: tdsVersion, progName: progName, majorVer: majorVer, minorVer: minorVer, buildNumHi: buildNumHi, buildNumLow: buildNumLow)

        return token
    }

    public static func parseColMetadataTokenStream(messageBuffer: inout ByteBuffer) throws -> ColMetadataToken {
        guard
            let count = messageBuffer.readInteger(endianness: .little, as: UShort.self)
        else {
            throw TDSError.protocolError("Invalid COLMETADATA token")
        }

        var colData: [ColMetadataToken.ColumnData] = []
        for _ in 0...count - 1 {
            guard
                let userType = messageBuffer.readInteger(as: ULong.self),
                let flags = messageBuffer.readInteger(as: UShort.self),
                let dataTypeVal = messageBuffer.readInteger(as: UInt8.self),
                let dataType = DataType.init(rawValue: dataTypeVal)
            else {
                throw TDSError.protocolError("Invalid COLMETADATA token")
            }
            var length: Int
            switch dataType {
            case .sqlVariant, .nText, .text, .image:
                guard let len = messageBuffer.readInteger(endianness: .little, as: LongLen.self) else {
                    throw TDSError.protocolError("Error while reading length")
                }
                length = Int(len)
            case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
                guard let len = messageBuffer.readInteger(endianness: .little, as: UShortCharBinLen.self) else {
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
                guard let len = messageBuffer.readInteger(endianness: .little, as: ByteLen.self) else {
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
                    let p = messageBuffer.readInteger(as: UInt8.self),
                    p <= 38
                else {
                    throw TDSError.protocolError("Error while reading PRECISION.")
                }
                precision = Int(p)
            }

            var scale: Int?
            if (dataType.isScaleType()) {
                guard let s = messageBuffer.readInteger(as: UInt8.self) else {
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
                guard let numParts = messageBuffer.readInteger(as: UInt8.self) else {
                    throw TDSError.protocolError("Error while reading NUMPARTS.")
                }

                for _ in 0...numParts - 1 {
                    guard
                        let partNameLen = messageBuffer.readInteger(as: UShort.self),
                        let partNameBytes = messageBuffer.readBytes(length: Int(partNameLen * 2)),
                        let partName = String(bytes: partNameBytes, encoding: .utf16LittleEndian)
                    else {
                        throw TDSError.protocolError("Error while reading NUMPARTS.")
                    }
                    parts.append(partName)
                }
                tableName = parts.joined(separator: ".")
            default:
                break
            }

            guard
                let colNameLength = messageBuffer.readInteger(as: UInt8.self),
                let colNameBytes = messageBuffer.readBytes(length: Int(colNameLength * 2)),
                let colName = String(bytes: colNameBytes, encoding: .utf16LittleEndian)
            else {
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
                guard let len = messageBuffer.readInteger(endianness: .little, as: LongLen.self) else {
                    throw TDSError.protocolError("Error while reading length")
                }
                length = Int(len)
            case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
                guard let len = messageBuffer.readInteger(endianness: .little, as: UShortCharBinLen.self) else {
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
                guard let len = messageBuffer.readInteger(endianness: .little, as: ByteLen.self) else {
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
            let status = messageBuffer.readInteger(as: UShort.self),
            let curCmd = messageBuffer.readInteger(as: UShort.self),
            let doneRowCount = messageBuffer.readInteger(as: ULongLong.self)
        else {
            throw TDSError.protocolError("Invalid done token")
        }

        let token = DoneToken(status: status, curCmd: curCmd, doneRowCount: doneRowCount)
        return token
    }

    public static func parseEnvChangeTokenStream(messageBuffer: inout ByteBuffer) throws -> Token? {
        guard
            let length = messageBuffer.readInteger(endianness: .little, as: UInt16.self),
            let type = messageBuffer.readInteger(as: UInt8.self),
            let changeType = TDSMessages.EnvchangeType(rawValue: type)
        else {
            throw TDSError.protocolError("Invalid envchange token")
        }

        switch changeType {
        case .database, .language, .characterSet, .packetSize, .unicodeSortingLocalId, .unicodeSortingFlags:
            guard
                let newValueLength = messageBuffer.readInteger(as: UInt8.self),
                let newValueBytes = messageBuffer.readBytes(length: Int(newValueLength * 2)),
                let newValue = String(bytes: newValueBytes, encoding: .utf16LittleEndian),
                let oldValueLength = messageBuffer.readInteger(as: UInt8.self),
                let oldValueBytes = messageBuffer.readBytes(length: Int(oldValueLength * 2)),
                let oldValue = String(bytes: oldValueBytes, encoding: .utf16LittleEndian)
            else {
                throw TDSError.protocolError("Invalid token stream.")
            }

            let token = BVarcharEnvchangeToken(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token

        default:
            messageBuffer.moveReaderIndex(forwardBy: Int(length - 1))
            return nil
        }
    }

    public static func parseErrorInfoTokenStream(type: TokenType, messageBuffer: inout ByteBuffer) throws -> ErrorInfoToken {
        guard
            let _ = messageBuffer.readInteger(endianness: .little, as: UInt16.self),
            let number = messageBuffer.readInteger(as: Long.self),
            let state = messageBuffer.readInteger(as: UInt8.self),
            let classValue = messageBuffer.readInteger(as: UInt8.self),
            let msgTextLength = messageBuffer.readInteger(endianness: .little, as: UShortLen.self),
            let msgTextBytes = messageBuffer.readBytes(length: Int(msgTextLength * 2)),
            let msgText = String(bytes: msgTextBytes, encoding: .utf16LittleEndian),
            let serverNameLength = messageBuffer.readInteger(as: UInt8.self),
            let serverNameBytes = messageBuffer.readBytes(length: Int(serverNameLength * 2)),
            let serverName = String(bytes: serverNameBytes, encoding: .utf16LittleEndian),
            let procNameLength = messageBuffer.readInteger(as: UInt8.self),
            let procNameBytes = messageBuffer.readBytes(length: Int(procNameLength * 2)),
            let procName = String(bytes: procNameBytes, encoding: .utf16LittleEndian),
            let lineNumber = messageBuffer.readInteger(as: Long.self)
        else {
            throw TDSError.protocolError("Invalid error/info token")
        }

        let token = ErrorInfoToken(type: type, number: Int(number), state: state, classValue: classValue, messageText: msgText, serverName: serverName, procedureName: procName, lineNumber: Int(lineNumber))

        return token
        
    }
}
