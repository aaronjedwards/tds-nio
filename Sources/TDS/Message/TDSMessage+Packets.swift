import NIO

import NIO

extension TDSMessage {
    init(packingDataWith buffer: inout ByteBuffer, headerType: TDSPacket.HeaderType, allocator: ByteBufferAllocator) throws {
        var packets = [TDSPacket]()
        
        var packetId: UInt8 = 0
        while buffer.readableBytes >= TDSPacket.maximumPacketDataLength {
            guard var packetData = buffer.readSlice(length: TDSPacket.maximumPacketDataLength) else {
                throw TDSError.protocol("Serialization Error: Expected")
            }
            
            packets.append(TDSPacket(message: &packetData, headerType: headerType, isLastPacket: false, packetId: packetId, allocator: allocator))
            packetId = packetId &+ 1
        }
        
        var lastPacket = buffer.slice()
        packets.append(TDSPacket(message: &lastPacket, headerType: headerType, isLastPacket: true, packetId: packetId, allocator: allocator))
        
        self.init(packets: packets)
    }
}

extension ByteBuffer {
    init(unpackingDataFrom message: TDSMessage, allocator: ByteBufferAllocator) throws {
        let size = message.packets.reduce(0, { $0 + $1.messageBuffer.readableBytes })
        var buffer = allocator.buffer(capacity: size)
        
        for packet in message.packets {
            var messageBuffer = packet.messageBuffer
            buffer.writeBuffer(&messageBuffer)
        }
        
        self = buffer
    }
}
