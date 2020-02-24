import NIO

extension TDSMessage {
    /// Authentication request returned by the server.
    public struct PreloginSSLHandshakeMessage: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType {
            return .prelogin
        }
        
        public var sslPayload: ByteBuffer
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            var payloadBuffer = sslPayload
            var packetNumber = 0
            while(sslPayload.readableBytes >= TDSPacket.maximumPacketDataLength) {
                guard var packetData = payloadBuffer.readSlice(length: TDSPacket.maximumPacketDataLength) else {
                    throw TDSError.protocol("Serialization Error: Expected")
                }
                
                buffer.writeBytes([
                    PreloginSSLHandshakeMessage.headerType.value,   // Type
                    0x00,                                           // Status
                ])
                
                buffer.writeInteger(UInt16(TDSPacket.defaultPacketLength)) // Length
                
                buffer.writeBytes([
                    0x00, 0x00,                                     // SPID
                    UInt8(packetNumber),                            // PacketID (Unused)
                    0x00                                            // Window (Unused)
                ])
                
                buffer.writeBuffer(&packetData)
                packetNumber += 1
            }
            
            buffer.writeBytes([
                PreloginSSLHandshakeMessage.headerType.value,   // Type
                0x01                                            // Status
            ])
            
            buffer.writeInteger(UInt16(payloadBuffer.readableBytes + 8)) // Length
            
            buffer.writeBytes([
                0x00, 0x00,                                     // SPID
                UInt8(packetNumber),                            // PacketID (Unused)
                0x00                                            // Window (Unused)
            ])
            
            buffer.writeBuffer(&payloadBuffer)
            
        }
        
        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> PreloginSSLHandshakeMessage {
            return PreloginSSLHandshakeMessage(sslPayload: buffer)
        }
    }
}
