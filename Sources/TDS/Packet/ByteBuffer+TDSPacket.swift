extension ByteBuffer {
    init(from packets: [TDSPacket], allocator: ByteBufferAllocator) {
        let size = packets.reduce(0, { $0 + $1.messageBuffer.readableBytes })
        var buffer = allocator.buffer(capacity: size)
        
        for packet in packets {
            var messageBuffer = packet.messageBuffer
            buffer.writeBuffer(&messageBuffer)
        }
        
        self = buffer
    }
}
