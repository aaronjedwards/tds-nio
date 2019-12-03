import NIO

extension TDSMessage {
    /// Authentication request returned by the server.
    public struct PreloginSSLHandshakeMessage: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType {
            return .prelogin
        }
        
        public var sslPayload: ByteBuffer
        
        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> PreloginSSLHandshakeMessage {
            return PreloginSSLHandshakeMessage(sslPayload: buffer)
        }
    }
}
