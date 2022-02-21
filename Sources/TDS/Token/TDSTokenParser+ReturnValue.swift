extension TDSTokenParser {
    public static func parseReturnValueToken(from buffer: inout ByteBuffer) throws -> TDSTokens.ReturnValueToken {
//        print(Data(buffer: buffer).hexEncodedString(options: .upperCase))
        
        guard
            let paramOrdinal = buffer.readUShortCharBinLen(),
            // UTF16 Parameter name
            let parameterName = buffer.readUSVarchar(),
            let status = buffer.readByte(),
            let userType = buffer.readULong(),
            let flags = buffer.readUShort(),
            let type = buffer.readByte(), // Data type
            let dataType = TDSDataType(rawValue: type), // Convert to TDSDataType
            let _ = buffer.readUShort(), // Maximal Length
            let _ = buffer.readSlice(length: 5), // Collation
            let dataLength = buffer.readUShortCharBinLen(), // dataLength
            var data = buffer.readSlice(length: Int(dataLength))
        else {
            throw TDSError.protocolError("Invalid Return Value token")
        }
        
        var decodedData: Any?
        
        switch dataType {
        case .charLegacy, .varcharLegacy, .char, .varchar, .text:// UTF-8 Encoding
            decodedData = data.readUTF8String(length: data.readableBytes)
        case .nvarchar, .nchar, .nText:// UTF-16 Encoding
            decodedData = data.readUTF16String(length: data.readableBytes)
        case .null:
            break
        case .tinyInt:
            decodedData = data.readByte()
        case .smallInt:
            decodedData = data.readUShort()
        case .int, .intn:
            decodedData = data.readULong()
        case .bigInt:
            decodedData = data.readULongLong()
        
        default:
            throw TDSError.protocolError("TDSData Type \(dataType) decoding not implemented")
        }
        
        
        let token = TDSTokens.ReturnValueToken(paramName: parameterName,
                                               paramOrdinal: UShort(paramOrdinal),
                                               status: Byte(status),
                                               userType: UInt32(userType),
                                               flags: Byte(flags),
                                               dataType: dataType,
                                               data: decodedData
        )
        return token
    }
}
