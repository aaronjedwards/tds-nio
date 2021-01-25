import NIO

/// Packet
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/e5ea8520-1ea3-4a75-a2a9-c17e63e9ee19
public struct TDSPacket {
    /// Packet Header
    var header: Header! {
        Header(from: buffer)
    }
    
    let headerType: HeaderType
    
    public var messageBuffer: ByteBuffer {
        buffer.getSlice(at: Header.length, length: buffer.readableBytes - Header.length)!
    }
    
    /// Packet Data
    internal var buffer: ByteBuffer
    
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
        
        self.headerType = .init(integerLiteral: typeByte)
        self.buffer = slice
    }
    
    init<M: TDSMessageType>(message: M, allocator: ByteBufferAllocator) throws {
        var buffer = allocator.buffer(capacity: 4_096)
        
        buffer.writeInteger(M.headerType.value)
        buffer.writeInteger(0x00 as UInt8) // status
        
        // Skip length, it will be set later
        buffer.moveWriterIndex(forwardBy: 2)
        buffer.writeInteger(0x00 as UInt16) // SPID
        buffer.writeInteger(0x00 as UInt8)
        buffer.writeInteger(0x00 as UInt8) // Window
        
        try message.serialize(into: &buffer)
        
        // Update length
        if buffer.writerIndex >= Int(UInt16.max) {
            fatalError("This shouldn't happen, crash for now")
        }
        
        buffer.setInteger(UInt16(buffer.writerIndex), at: 2)
        
        self.headerType = M.headerType
        self.buffer = buffer
    }
    
    init(message: inout ByteBuffer, headerType: HeaderType, isLastPacket: Bool, packetId: UInt8 = 0, allocator: ByteBufferAllocator) {
        var buffer = allocator.buffer(capacity: 4_096)
        
        buffer.writeInteger(headerType.value)
        buffer.writeInteger(isLastPacket ? 0x01 : 0x00 as UInt8) // status
        
        // Skip length, it will be set later
        buffer.moveWriterIndex(forwardBy: 2)
        buffer.writeInteger(0x00 as UInt16) // SPID
        buffer.writeInteger(packetId) // PacketID
        buffer.writeInteger(0x00 as UInt8) // Window
        
        buffer.writeBuffer(&message)
        
        // Update length
        if buffer.writerIndex >= Int(UInt16.max) {
            fatalError("This shouldn't happen, crash for now")
        }
        
        buffer.setInteger(UInt16(buffer.writerIndex), at: 2)
        
        self.headerType = headerType
        self.buffer = buffer
    }
}

extension TDSPacket {
    public static func empty(type: HeaderType, allocator: ByteBufferAllocator) -> TDSPacket {
        let header = Header(type: type, status: .eom)
        var buffer = allocator.buffer(capacity: 100)
        header.writeToByteBuffer(buffer: &buffer)
        let packet = TDSPacket.init(from: &buffer)!
        return packet
    }
}

extension TDSPacket {
    public static let defaultPacketLength = 1000
    public static let maximumPacketDataLength = TDSPacket.defaultPacketLength - 8
}
