import NIO

/// Client or Server Message
public struct TDSMessage {
    public var headerType: TDSPacket.HeaderType
    
    public var data: ByteBuffer
    
    public init(headerType: TDSPacket.HeaderType, data: ByteBuffer) {
        self.headerType = headerType
        self.data = data
    }
    
    public init?(packets: [TDSPacket]) {
        guard let packet = packets.first else {
            return nil
        }
        
        var data = packet.data
        for index in packets.indices.dropFirst() {
            var packet = packets[index]
            data.writeBuffer(&packet.data)
        }

        self.init(headerType: packet.header.type, data: data)
    }
}
