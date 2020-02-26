import NIO

public final class TDSMessageDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    public typealias InboundOut = [TDSPacket]
    
    /// See `ByteToMessageDecoder`.
    private var storedPackets = [TDSPacket]()
    
    /// If `true`, the server has asked for authentication.
    public var hasSeenFirstMessage: Bool
    
    /// Creates a new `TDSMessageDecoder`.
    public init() {
        self.hasSeenFirstMessage = false
    }
    
    /// See `ByteToMessageDecoder`.
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        while let packet = TDSPacket(from: &buffer) {
            self.storedPackets.append(packet)
            
            if packet.header.status == .eom {
                context.fireChannelRead(wrapInboundOut(storedPackets))
                storedPackets.removeAll(keepingCapacity: true)

                // Don't check, just set. It's faster that way
                self.hasSeenFirstMessage = true
                
                return .continue
            }
        }
        
        return .needMoreData
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        try decode(context: context, buffer: &buffer)
    }
}

extension ByteBuffer {
    fileprivate mutating func readPacketData() -> [Byte]? {
        return []
    }
}

