import NIO

extension TDSMessage {

    public static func parseTokenDataStream(messageBuffer: inout ByteBuffer) throws -> [TDSToken] {

        enum State {
            case parsing
            case recievedColMetadata(TDSTokens.ColMetadataToken)
        }

        var state: State = .parsing

        var tokens: [TDSToken] = []
        while messageBuffer.readableBytes > 0 {
            guard
                let token = messageBuffer.readByte(),
                let tokenType = TDSTokens.TokenType(rawValue: token)
                else {
                    throw TDSError.protocolError("Invalid token type in Login7 response")
            }

            switch tokenType {
            case .error, .info:
                let token = try TDSMessage.parseErrorInfoTokenStream(type: tokenType, messageBuffer: &messageBuffer)
                tokens.append(token)
            case .loginAck:
                let token = try TDSMessage.parseLoginAckTokenStream(messageBuffer: &messageBuffer)
                tokens.append(token)
            case .envchange:
                let token = try TDSMessage.parseEnvChangeTokenStream(messageBuffer: &messageBuffer)
                tokens.append(token)
            case .done, .doneInProc, .doneProc :
                let token = try TDSMessage.parseDoneTokenStream(messageBuffer: &messageBuffer)
                tokens.append(token)
            case .colMetadata:
                let token = try TDSMessage.parseColMetadataTokenStream(messageBuffer: &messageBuffer)
                tokens.append(token)
                state = .recievedColMetadata(token)
            case .row:
                switch state {
                case .recievedColMetadata(let colMetadata):
                    let token = try TDSMessage.parseRowTokenStream(colMetadata: colMetadata, messageBuffer: &messageBuffer)
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

    public static func parseLoginAckTokenStream(messageBuffer: inout ByteBuffer) throws -> TDSTokens.LoginAckToken {
        guard
            let _ = messageBuffer.readUShort(),
            let interface = messageBuffer.readByte(),
            let tdsVersion = messageBuffer.readDWord(),
            let progNameLength = messageBuffer.readByte(),
            let progName = messageBuffer.readUTF16String(length: Int(progNameLength) * 2),
            let majorVer = messageBuffer.readByte(),
            let minorVer = messageBuffer.readByte(),
            let buildNumHi = messageBuffer.readByte(),
            let buildNumLow = messageBuffer.readByte()
            else {
                throw TDSError.protocolError("Invalid loginack token")
        }

        let token = TDSTokens.LoginAckToken(interface: interface, tdsVersion: tdsVersion, progName: progName, majorVer: majorVer, minorVer: minorVer, buildNumHi: buildNumHi, buildNumLow: buildNumLow)

        return token
    }

    public static func parseColMetadataTokenStream(messageBuffer: inout ByteBuffer) throws -> TDSTokens.ColMetadataToken {
        guard
            let count = messageBuffer.readUShort()
            else {
                throw TDSError.protocolError("Invalid COLMETADATA token: Error while reading COUNT of columns")
        }

        var colData: [TDSTokens.ColMetadataToken.ColumnData] = []
        for _ in 0...count - 1 {
            guard
                let userType = messageBuffer.readULong(),
                let flags = messageBuffer.readUShort(),
                let dataTypeVal = messageBuffer.readByte(),
                let dataType = TDSDataType.init(rawValue: dataTypeVal)
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
                guard let len = messageBuffer.readUShortLen() else {
                    throw TDSError.protocolError("Error while reading length")
                }
                length = Int(len)
            case .date:
                length = 3
            case .tinyInt, .bit:
                length = 1
            case .smallInt:
                length = 2
            case .int, .smallDateTime, .real, .smallMoney:
                length = 4
            case .money, .datetime, .float, .bigInt:
                length = 8
            case .null:
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

            colData.append(TDSTokens.ColMetadataToken.ColumnData(userType: userType, flags: flags, dataType: dataType, length: length, collation: collationData, tableName: tableName, colName: colName, precision: precision, scale: scale))
        }

        let token = TDSTokens.ColMetadataToken(count: count, colData: colData)
        return token
    }

    public static func parseRowTokenStream(colMetadata: TDSTokens.ColMetadataToken, messageBuffer: inout ByteBuffer) throws -> TDSTokens.RowToken {
        var colData: [TDSTokens.RowToken.ColumnData] = []

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
            case .int, .smallDateTime, .real, .smallMoney:
                length = 4
            case .money, .datetime, .float, .bigInt:
                length = 8
            case .null:
                length = 0
            default:
                guard let len = messageBuffer.readByteLen() else {
                    throw TDSError.protocolError("Error while reading length.")
                }
                length = Int(len)
            }

            guard
                let data = messageBuffer.readSlice(length: Int(length))
                else {
                    throw TDSError.protocolError("Error while reading row data")
            }

            colData.append(TDSTokens.RowToken.ColumnData(textPointer: [], timestamp: [], data: data))
        }

        let token = TDSTokens.RowToken(colData: colData)
        return token
    }

    public static func parseDoneTokenStream(messageBuffer: inout ByteBuffer) throws -> TDSTokens.DoneToken {
        guard
            let status = messageBuffer.readUShort(),
            let curCmd = messageBuffer.readUShort(),
            let doneRowCount = messageBuffer.readULongLong()
            else {
                throw TDSError.protocolError("Invalid done token")
        }

        let token = TDSTokens.DoneToken(status: status, curCmd: curCmd, doneRowCount: doneRowCount)
        return token
    }

    public static func parseEnvChangeTokenStream(messageBuffer: inout ByteBuffer) throws -> TDSToken {
        guard
            let _ = messageBuffer.readUShort(),
            let type = messageBuffer.readByte(),
            let changeType = TDSTokens.EnvchangeType(rawValue: type)
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

            let token = TDSTokens.EnvchangeToken<String>(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token
        case .sqlCollation, .beingTransaction, .commitTransaction, .defectTransaction, .rollbackTransaction, .enlistDTCTransaction, .resetConnectionAck, .transactionEnded:
            guard
                let newValue = messageBuffer.readBVarbyte(),
                let oldValue = messageBuffer.readBVarbyte()
                else {
                    throw TDSError.protocolError("Invalid token stream.")
            }

            let token = TDSTokens.EnvchangeToken<[Byte]>(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token
        case .promoteTransaction:
            guard
                let newValue = messageBuffer.readLVarbyte(),
                let _ = messageBuffer.readBytes(length: 1)
                else {
                    throw TDSError.protocolError("Invalid token stream.")
            }

            let token = TDSTokens.EnvchangeToken<[Byte]>(envchangeType: changeType, newValue: newValue, oldValue: [])
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

            let newValue = TDSTokens.RoutingEnvchangeToken.RoutingData(port: Int(portNumber), alternateServer: alternateServer)

            let token = TDSTokens.RoutingEnvchangeToken(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token
        }
    }

    public static func parseErrorInfoTokenStream(type: TDSTokens.TokenType, messageBuffer: inout ByteBuffer) throws -> TDSTokens.ErrorInfoToken {
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

        let token = TDSTokens.ErrorInfoToken(type: type, number: Int(number), state: state, classValue: classValue, messageText: msgText, serverName: serverName, procedureName: procName, lineNumber: Int(lineNumber))

        return token

    }
}
