import NIO

/// Packet
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/e5ea8520-1ea3-4a75-a2a9-c17e63e9ee19
public struct TDSPacket {
    /// Packet Header
    var header: Header
    
    /// Packet Data
    var data: ByteBuffer
}

extension TDSPacket {
    public static let defaultPacketLength = 1000
    public static let maximumPacketDataLength = TDSPacket.defaultPacketLength - 8
}
