import NIO

/// Packet Header
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/7af53667-1b72-4703-8258-7984e838f746
extension TDSPacket {
    public struct Header {
        static let length = 8
        
        /// Type
        public var type: HeaderType
        
        /// Status
        public var status: Status
        
        /// Length
        public var length: UInt16
        
        /// SPID
        public var spid: UInt16
        
        /// PacketID
        public var packetId: UInt8
        
        /// Window
        public var window: UInt8 = 0x00
        
        init(type: HeaderType, status: Status, length: UInt16 = 0) {
            self.type = type
            self.status = status
            self.length = length
            self.spid = 0x0000
            self.packetId = 0x00
            self.window = 0x00
        }
        
        init?(from buffer: ByteBuffer) {
            guard
                let typeByte: UInt8 = buffer.getInteger(at: 0),
                let statusByte: UInt8 = buffer.getInteger(at: 1),
                let length: UInt16 = buffer.getInteger(at: 2),
                let spid: UInt16 = buffer.getInteger(at: 4),
                let packetId: UInt8 = buffer.getInteger(at: 6),
                let window: UInt8 = buffer.getInteger(at: 7)
            else {
                return nil
            }
            
            self.type = HeaderType(integerLiteral: typeByte)
            self.status = Status(integerLiteral: statusByte)
            self.length = length
            self.spid = spid
            self.packetId = packetId
            self.window = window
        }
        
        func writeToByteBuffer(buffer: inout ByteBuffer) {
            buffer.writeInteger(type.value)
            buffer.writeInteger(status.value)
            buffer.writeInteger(length)
            buffer.writeInteger(spid)
            buffer.writeInteger(packetId)
            buffer.writeInteger(window)
        }
    }
}
