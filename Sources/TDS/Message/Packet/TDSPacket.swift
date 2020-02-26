import NIO

/// Packet
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/e5ea8520-1ea3-4a75-a2a9-c17e63e9ee19
public struct TDSPacket {
    /// Packet Header
    var header: Header! {
        Header(from: buffer)
    }
    
    var headerType: HeaderType {
        // This could become a stored constant so we don't parse the header unnecessarily
        header.type
    }
    
    public var messageBuffer: ByteBuffer! {
        buffer.getSlice(at: Header.length, length: buffer.readableBytes - Header.length)
    }
    
    /// Packet Data
    private var buffer: ByteBuffer
    
    init?(from buffer: inout ByteBuffer) {
        guard
            buffer.readableBytes >= Header.length,
            let length: UInt16 = buffer.getInteger(at: 2), // After type and status
            length <= buffer.readableBytes,
            let slice = buffer.readSlice(length: Int(length))
        else {
            return nil
        }
        
        self.buffer = slice
    }
    
    init<M: TDSMessage>(message: M, allocator: ByteBufferAllocator) throws {
        var buffer = allocator.buffer(capacity: 4_096)
        
        buffer.writeInteger(M.headerType.value)
        buffer.writeInteger(0 as UInt8)
        
        // Skip length, it will be set later
        buffer.moveWriterIndex(forwardBy: 2)
        buffer.writeInteger(0x00 as UInt16) // SPID
        buffer.writeInteger(0x00 as UInt8) // TODO: PacketID is incremental
        buffer.writeInteger(0x00 as UInt8) // Window
        
        buffer.moveWriterIndex(forwardBy: Header.length)
        try message.serialize(into: &buffer)
        
        // Update length
        if buffer.writerIndex >= Int(UInt16.max) {
            fatalError("This shouldn't happen, crash for now")
        }
        buffer.setInteger(UInt16(buffer.writerIndex), at: 2)
        
        self.buffer = buffer
    }
}

extension TDSPacket {
    public static let defaultPacketLength = 1000
    public static let maximumPacketDataLength = TDSPacket.defaultPacketLength - 8
}
