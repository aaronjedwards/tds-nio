import NIO

extension TDSMessages {
    /// Authentication request returned by the server.
    public struct SSLKickoff: TDSPacketType {
        public static var headerType: TDSPacket.HeaderType {
            return .sslKickoff
        }
        
        public func serialize(into buffer: inout ByteBuffer) throws {}
    }
}
