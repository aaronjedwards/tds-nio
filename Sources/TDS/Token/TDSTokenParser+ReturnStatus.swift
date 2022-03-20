extension TDSTokenParser {
    public static func parseReturnStatusToken(from buffer: inout ByteBuffer) throws -> TDSTokens.ReturnStatusToken {
        guard
            let valueLong = buffer.readLong()
        else {
            throw TDSError.protocolError("Invalid DoneInProc token")
        }
        let value = Int(valueLong)
        let token = TDSTokens.ReturnStatusToken(value: value)
        return token
    }
}
