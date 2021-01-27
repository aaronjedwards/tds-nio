extension TDSTokenParser {
    public static func parseLoginAckToken(from buffer: inout ByteBuffer) throws -> TDSTokens.LoginAckToken {
        guard
            let _ = buffer.readUShort(),
            let interface = buffer.readByte(),
            let tdsVersion = buffer.readDWord(),
            let progNameLength = buffer.readByte(),
            let progName = buffer.readUTF16String(length: Int(progNameLength) * 2),
            let majorVer = buffer.readByte(),
            let minorVer = buffer.readByte(),
            let buildNumHi = buffer.readByte(),
            let buildNumLow = buffer.readByte()
            else {
                throw TDSError.protocolError("Invalid loginack token")
        }

        let token = TDSTokens.LoginAckToken(interface: interface, tdsVersion: tdsVersion, progName: progName, majorVer: majorVer, minorVer: minorVer, buildNumHi: buildNumHi, buildNumLow: buildNumLow)

        return token
    }
}
