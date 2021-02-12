import NIO

import NIO

extension TDSMessage {
    init(from buffer: inout ByteBuffer, ofType type: TDSPacket.HeaderType, allocator: ByteBufferAllocator) throws {
        var packets = [TDSPacket]()
        
        var packetId: UInt8 = 0
        while buffer.readableBytes >= TDSPacket.maximumPacketDataLength {
            guard var packetData = buffer.readSlice(length: TDSPacket.maximumPacketDataLength) else {
                throw TDSError.protocolError("Unable to read packet of size: \(TDSPacket.maximumPacketDataLength)")
            }
            let packet = TDSPacket(from: &packetData, ofType: type, status: .normal, packetId: packetId, allocator: allocator)
            packets.append(packet)
            packetId = packetId &+ 1
        }
        
        var lastPacket = buffer.slice()
        packets.append(TDSPacket(from: &lastPacket, ofType: type, status: .eom, packetId: packetId, allocator: allocator))
        
        self.init(packets: packets)
    }
}
