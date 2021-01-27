extension TDSTokenParser {
    public static func parseEnvChangeToken(from buffer: inout ByteBuffer) throws -> TDSToken {
        guard
            let _ = buffer.readUShort(),
            let type = buffer.readByte(),
            let changeType = TDSTokens.EnvchangeType(rawValue: type)
            else {
                throw TDSError.protocolError("Invalid envchange token")
        }

        switch changeType {
        case .database, .language, .characterSet, .packetSize, .realTimeLogShipping, .unicodeSortingLocalId, .unicodeSortingFlags, .userInstanceStarted:
            guard
                let newValue = buffer.readBVarchar(),
                let oldValue = buffer.readBVarchar()
                else {
                    throw TDSError.protocolError("Invalid token stream.")
            }

            let token = TDSTokens.EnvchangeToken<String>(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token
        case .sqlCollation, .beingTransaction, .commitTransaction, .defectTransaction, .rollbackTransaction, .enlistDTCTransaction, .resetConnectionAck, .transactionEnded:
            guard
                let newValue = buffer.readBVarbyte(),
                let oldValue = buffer.readBVarbyte()
                else {
                    throw TDSError.protocolError("Invalid token stream.")
            }

            let token = TDSTokens.EnvchangeToken<[Byte]>(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token
        case .promoteTransaction:
            guard
                let newValue = buffer.readLVarbyte(),
                let _ = buffer.readBytes(length: 1)
                else {
                    throw TDSError.protocolError("Invalid token stream.")
            }

            let token = TDSTokens.EnvchangeToken<[Byte]>(envchangeType: changeType, newValue: newValue, oldValue: [])
            return token
        case .transactionManagerAddress:
            throw TDSError.protocolError("Received unexpected ENVCHANGE Token Type 16: Transaction Manager Address is not used by SQL Server.")
        case .routingInfo:
            guard
                let _ = buffer.readUShort(),
                let protocolByte = buffer.readByte(),
                protocolByte == 0,
                let portNumber = buffer.readUShort(),
                let alternateServer = buffer.readUSVarchar(),
                let oldValue = buffer.readBytes(length: 2)
                else {
                    throw TDSError.protocolError("Invalid token stream.")
            }

            let newValue = TDSTokens.RoutingEnvchangeToken.RoutingData(port: Int(portNumber), alternateServer: alternateServer)

            let token = TDSTokens.RoutingEnvchangeToken(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token
        }
    }
}
