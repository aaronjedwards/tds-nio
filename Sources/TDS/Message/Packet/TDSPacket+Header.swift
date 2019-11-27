/// Packet Header
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/7af53667-1b72-4703-8258-7984e838f746
extension TDSPacket {
    public struct Header {
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
    }
}
