extension TDSTokenParser {
    public static func parseDoneToken(from buffer: inout ByteBuffer) throws -> TDSTokens.DoneToken {
        guard
            let status = buffer.readUShort(),
            let curCmd = buffer.readUShort(),
            let doneRowCount = buffer.readULongLong()
            else {
                throw TDSError.protocolError("Invalid done token")
        }

        let token = TDSTokens.DoneToken(status: status, curCmd: curCmd, doneRowCount: doneRowCount)
        return token
    }
}
