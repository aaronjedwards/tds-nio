public enum TDSProtocolVersion: CustomStringConvertible, Sendable {
    case v7_4
    case v8_0

    public var description: String {
        switch self {
        case .v7_4:
            return "7.4"
        case .v8_0:
            return "8.0"
        }
    }
}

extension TDSProtocolVersion {
    init(loginAck: TDSBackendMessage.LoginAck) {
        switch loginAck.tdsVersion {
        case 0x7400_0004:
            self = .v7_4
        case 0x0800_0000:
            self = .v8_0
        default:
            self = .v7_4
        }
    }
}
