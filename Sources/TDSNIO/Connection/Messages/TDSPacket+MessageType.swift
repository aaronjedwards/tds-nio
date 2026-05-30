import NIO

extension TDSPacket {
    enum MessageType: Byte {
        case sqlBatch = 0x01
        case preTDS7Login = 0x02
        case rpc = 0x03
        case preloginLoginOrTablularResponse = 0x04
        case attentionSignal = 0x06
        case bulkLoadData = 0x07
        case federatedAuthenticationToken = 0x08
        case transactionManagerRequest = 0x0E
        case tds7Login = 0x10
        case sspi = 0x11
        case prelogin = 0x12
        case sslKickoff = 0x99
    }
}
