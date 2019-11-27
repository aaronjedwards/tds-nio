import NIO

/// Client or Server Message
public struct TDSMessage {
    public var headerType: TDSPacket.HeaderType
    
    public var data: ByteBuffer
    
    public init(headerType: TDSPacket.HeaderType, data: ByteBuffer) {
        self.headerType = headerType
        self.data = data
    }
}
