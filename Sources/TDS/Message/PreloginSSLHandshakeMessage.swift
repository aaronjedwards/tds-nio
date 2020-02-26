import NIO

extension TDSMessage {
    init(packingSSLPayloadWith sslPayload: inout ByteBuffer, allocator: ByteBufferAllocator) throws {
        var packets = [TDSPacket]()
        
        var packetId: UInt8 = 0
        while sslPayload.readableBytes >= TDSPacket.maximumPacketDataLength {
            guard var packetData = sslPayload.readSlice(length: TDSPacket.maximumPacketDataLength) else {
                throw TDSError.protocolError("Serialization Error: Expected")
            }
            
            packets.append(TDSPacket(message: &packetData, headerType: .prelogin, isLastPacket: false, packetId: packetId, allocator: allocator))
            packetId = packetId &+ 1
        }
        
        var lastPacket = sslPayload.slice()
        packets.append(TDSPacket(message: &lastPacket, headerType: .prelogin, isLastPacket: true, packetId: packetId, allocator: allocator))
        
        self.init(packets: packets)
    }
}

extension ByteBuffer {
    init(unpackingSSLPayloadFrom message: TDSMessage, allocator: ByteBufferAllocator) throws {
        let size = message.packets.reduce(0, { $0 + $1.messageBuffer.readableBytes })
        var buffer = allocator.buffer(capacity: size)
        
        for packet in message.packets {
            var messageBuffer = packet.messageBuffer
            buffer.writeBuffer(&messageBuffer)
        }
        
        self = buffer
    }
}
