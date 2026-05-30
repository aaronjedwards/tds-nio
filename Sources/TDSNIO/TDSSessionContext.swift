struct TDSSessionContext: Sendable, Hashable {
    private(set) var capabilities: Capabilities
    private(set) var loginAck: TDSBackendMessage.LoginAck?
    private(set) var packetSize: Int?
    private(set) var transactionDescriptor: [UInt8] = []

    init(requestedProtocolVersion: TDSProtocolVersion = .v7_4) {
        self.capabilities = Capabilities(requestedProtocolVersion: requestedProtocolVersion)
    }

    var requestedProtocolVersion: TDSProtocolVersion {
        self.capabilities.requestedProtocolVersion
    }

    var negotiatedProtocolVersion: TDSProtocolVersion? {
        self.capabilities.negotiatedProtocolVersion
    }

    mutating func receiveLoginAck(_ loginAck: TDSBackendMessage.LoginAck) {
        self.loginAck = loginAck
        self.capabilities.adjustForLoginAck(loginAck)
    }

    mutating func receiveFeatureExtAck(_ featureExtAck: TDSBackendMessage.FeatureExtAck) {
        self.capabilities.adjustForFeatureExtAck(featureExtAck)
    }

    mutating func receiveEnvChange(_ envChange: TDSBackendMessage.EnvChange) -> EnvChangeAction {
        switch envChange.value {
        case .bytes(let new, _) where Self.isTransactionDescriptorEnvChange(envChange.type):
            self.transactionDescriptor = new
            return .transactionDescriptorChanged(new)
        case .string(let new, _) where envChange.type == 4:
            guard let packetSize = Int(new) else {
                return .none
            }
            self.packetSize = packetSize
            return .packetSizeChanged(packetSize)
        default:
            return .none
        }
    }

    func wasAcknowledged(_ feature: Capabilities.FeatureID) -> Bool {
        self.capabilities.wasAcknowledged(feature)
    }

    private static func isTransactionDescriptorEnvChange(_ type: UInt8) -> Bool {
        switch type {
        case 8, 9, 10, 11, 12, 17:
            return true
        default:
            return false
        }
    }
}

extension TDSSessionContext {
    enum EnvChangeAction: Equatable {
        case none
        case transactionDescriptorChanged([UInt8])
        case packetSizeChanged(Int)
    }
}
