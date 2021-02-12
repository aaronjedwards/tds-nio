import NIO

/// Packet
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/e5ea8520-1ea3-4a75-a2a9-c17e63e9ee19
public struct TDSPacket {
    /// Packet Header
    var header: Header! {
        Header(from: buffer)
    }
    
    let type: HeaderType
    
    public var messageBuffer: ByteBuffer {
        buffer.getSlice(at: Header.length, length: buffer.readableBytes - Header.length)!
    }
    
    /// Packet Data
    internal var buffer: ByteBuffer
    
    public var isEom: Bool {
        return header.status == .eom
    }
    
    init?(from buffer: inout ByteBuffer) {
        guard
            buffer.readableBytes >= Header.length,
            let typeByte: UInt8 = buffer.getInteger(at: 0),
            let length: UInt16 = buffer.getInteger(at: 2), // After type and status
            length <= buffer.readableBytes,
            let slice = buffer.readSlice(length: Int(length))
        else {
            return nil
        }
        
        self.type = .init(integerLiteral: typeByte)
        self.buffer = slice
    }
    
    init<M: TDSMessagePayload>(message: M, allocator: ByteBufferAllocator) throws {
        var buffer = allocator.buffer(capacity: 4_096)
        
        buffer.writeInteger(M.packetType.value)
        buffer.writeInteger(0x00 as UInt8) // status
        
        // Skip length, it will be set later
        buffer.moveWriterIndex(forwardBy: 2)
        buffer.writeInteger(0x00 as UInt16) // SPID
        buffer.writeInteger(0x00 as UInt8)
        buffer.writeInteger(0x00 as UInt8) // Window
        
        try message.serialize(into: &buffer)
        
        // Update length
        buffer.setInteger(UInt16(buffer.writerIndex), at: 2)
        
        self.type = M.packetType
        self.buffer = buffer
    }
    
    init(from inputBuffer: inout ByteBuffer, ofType type: HeaderType, status: TDSPacket.Status, packetId: UInt8 = 0, allocator: ByteBufferAllocator) {
        var buffer = allocator.buffer(capacity: inputBuffer.readableBytes + TDSPacket.headerLength)
        
        buffer.writeInteger(type.value)
        buffer.writeInteger(status.value) // status
        
        // Skip length, it will be set later
        buffer.moveWriterIndex(forwardBy: 2)
        buffer.writeInteger(0x00 as UInt16) // SPID
        buffer.writeInteger(packetId) // PacketID
        buffer.writeInteger(0x00 as UInt8) // Window
        
        buffer.writeBuffer(&inputBuffer)
        
        // Update length
        buffer.setInteger(UInt16(buffer.writerIndex), at: 2)
        
        self.type = type
        self.buffer = buffer
    }
}

extension TDSPacket {
    public static let defaultPacketLength = 1000
    public static let headerLength = 8
    public static let maximumPacketDataLength = TDSPacket.defaultPacketLength - 8
}
