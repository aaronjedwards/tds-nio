import NIO

import NIO

extension TDSMessage {
    init(from buffer: inout ByteBuffer, ofType type: TDSPacket.HeaderType, allocator: ByteBufferAllocator) throws {
        var packets = [TDSPacket]()
        
        var packetId: UInt8 = 0
        while buffer.readableBytes >= TDSPacket.maximumPacketDataLength {
            guard var packetData = buffer.readSlice(length: TDSPacket.maximumPacketDataLength) else {
                throw TDSError.protocolError("Serialization Error: Expected")
            }
            
            packets.append(TDSPacket(from: &packetData, ofType: type, isLastPacket: false, packetId: packetId, allocator: allocator))
            packetId = packetId &+ 1
        }
        
        var lastPacket = buffer.slice()
        packets.append(TDSPacket(from: &lastPacket, ofType: type, isLastPacket: true, packetId: packetId, allocator: allocator))
        
        self.init(packets: packets)
    }
}
