import NIOCore

/// A session environment change sent by SQL Server.
///
/// SQL Server sends ENVCHANGE tokens when connection state changes, for example after
/// changing database, language, packet size, collation, or transaction state.
public struct TDSEnvChangeMessage: Sendable, Hashable {
    public enum Value: Sendable, Hashable {
        case string(new: String, old: String)
        case bytes(new: [UInt8], old: [UInt8])
        case routing(protocolByte: UInt8, port: UInt16, server: String)
        case unknown([UInt8])
    }

    public let type: UInt8
    public let value: Value

    init(_ envChange: TDSBackendMessage.EnvChange) {
        self.type = envChange.type
        switch envChange.value {
        case .string(let new, let old):
            self.value = .string(new: new, old: old)
        case .bytes(let new, let old):
            self.value = .bytes(new: new, old: old)
        case .routing(let routing):
            self.value = .routing(
                protocolByte: routing.protocolByte,
                port: routing.port,
                server: routing.server
            )
        case .unknown(var data):
            self.value = .unknown(data.readBytes(length: data.readableBytes) ?? [])
        }
    }
}
