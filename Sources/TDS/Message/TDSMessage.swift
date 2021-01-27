import NIO

// Use this as a namespace
public enum TDSMessages {}

/// Client or Server Message
public struct TDSMessage {
    public var packetType: TDSPacket.HeaderType {
        packets[0].type
    }
    
    public internal(set) var packets: [TDSPacket]
    
    init(packets: [TDSPacket]) {
        assert(!packets.isEmpty, "Invalid message")
        self.packets = packets
    }

    init<P: TDSMessagePayload>(payload: P, allocator: ByteBufferAllocator) throws {
        var buffer = allocator.buffer(capacity: TDSPacket.maximumPacketDataLength)
        try payload.serialize(into: &buffer)
        self = try TDSMessage(from: &buffer, ofType: P.packetType, allocator: allocator)
    }
    
    public func writeToByteBuffer(_ data: inout ByteBuffer) {
        for var packet in packets {
            data.writeBuffer(&packet.buffer)
        }
    }
    
    public func makeByteBuffer(allocator: ByteBufferAllocator) -> ByteBuffer {
        let size = packets.reduce(0, { $0 + $1.buffer.readableBytes })
        
        var data = allocator.buffer(capacity: size)
        
        for var packet in packets {
            data.writeBuffer(&packet.buffer)
        }
        
        return data
    }
}
