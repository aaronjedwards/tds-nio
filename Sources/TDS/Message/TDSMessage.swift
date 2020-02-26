import NIO

// Use this as a namespace
enum TDSMessages {}

/// Client or Server Message
public struct TDSMessage {
    public var headerType: TDSPacket.HeaderType {
        packets[0].headerType
    }
    
    public internal(set) var packets: [TDSPacket]
    var firstPacket: TDSPacket { packets[0] }
    
    init(packets: [TDSPacket]) {
        assert(!packets.isEmpty, "Invalid message")
        self.packets = packets
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
