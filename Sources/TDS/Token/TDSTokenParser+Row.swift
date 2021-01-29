extension TDSTokenParser {
    public static func parseRowToken(from buffer: inout ByteBuffer, with colMetadata: TDSTokens.ColMetadataToken) throws -> TDSTokens.RowToken {
        var colData: [TDSTokens.RowToken.ColumnData] = []

        // TODO: Handle textpointer and timestamp for certain types
        for col in colMetadata.colData {

            var length: Int
            switch col.dataType {
            case .sqlVariant, .nText, .text, .image:
                guard let len = buffer.readLongLen() else {
                    throw TDSError.protocolError("Error while reading length")
                }
                length = Int(len)
            case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
                guard let len = buffer.readUShortCharBinLen() else {
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

            guard
                let data = buffer.readSlice(length: Int(length))
                else {
                    throw TDSError.protocolError("Error while reading row data")
            }

            colData.append(TDSTokens.RowToken.ColumnData(textPointer: [], timestamp: [], data: data))
        }

        let token = TDSTokens.RowToken(colData: colData)
        return token
    }
}
