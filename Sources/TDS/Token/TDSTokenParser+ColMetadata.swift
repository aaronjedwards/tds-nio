extension TDSTokenParser {
    public static func parseColMetadataToken(from buffer: inout ByteBuffer) throws -> TDSTokens.ColMetadataToken {
        guard
            let count = buffer.readUShort()
            else {
                throw TDSError.protocolError("Invalid COLMETADATA token: Error while reading COUNT of columns")
        }

        var colData: [TDSTokens.ColMetadataToken.ColumnData] = []
        for _ in 0...count - 1 {
            guard
                let userType = buffer.readULong(),
                let flags = buffer.readUShort(),
                let dataTypeVal = buffer.readByte(),
                let dataType = TDSDataType.init(rawValue: dataTypeVal)
                else {
                    throw TDSError.protocolError("Invalid COLMETADATA token")
            }
            var length: Int
            switch dataType {
            case .sqlVariant, .nText, .text, .image:
                guard let len = buffer.readLongLen() else {
                    throw TDSError.protocolError("Error while reading length")
                }
                length = Int(len)
            case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
                guard let len = buffer.readUShortLen() else {
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
                guard let len = buffer.readByteLen() else {
                    throw TDSError.protocolError("Error while reading length.")
                }
                length = Int(len)
            }

            var collationData: [UInt8] = []
            if (dataType.isCollationType()) {
                guard let collationBytes = buffer.readBytes(length: 5) else {
                    throw TDSError.protocolError("Error while reading COLLATION.")
                }
                collationData = collationBytes
            }

            var precision: Int?
            if (dataType.isPrecisionType()) {
                guard
                    let p = buffer.readByte(),
                    p <= 38
                    else {
                        throw TDSError.protocolError("Error while reading PRECISION.")
                }
                precision = Int(p)
            }

            var scale: Int?
            if (dataType.isScaleType()) {
                guard let s = buffer.readByte() else {
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
                guard let numParts = buffer.readByte() else {
                    throw TDSError.protocolError("Error while reading NUMPARTS.")
                }

                for _ in 0...numParts - 1 {
                    guard let partName = buffer.readUSVarchar() else {
                        throw TDSError.protocolError("Error while reading NUMPARTS.")
                    }
                    parts.append(partName)
                }

                tableName = parts.joined(separator: ".")
            default:
                break
            }

            guard let colName = buffer.readBVarchar() else {
                throw TDSError.protocolError("Error while reading column name")
            }

            colData.append(TDSTokens.ColMetadataToken.ColumnData(userType: userType, flags: flags, dataType: dataType, length: length, collation: collationData, tableName: tableName, colName: colName, precision: precision, scale: scale))
        }

        let token = TDSTokens.ColMetadataToken(count: count, colData: colData)
        return token
    }
        
}
