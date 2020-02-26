import NIO

public final class TDSMessageDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    public typealias InboundOut = TDSPacket
    
    /// See `ByteToMessageDecoder`.
    public var cumulationBuffer: ByteBuffer?
    
    /// If `true`, the server has asked for authentication.
    public var hasSeenFirstMessage: Bool
    
    /// Creates a new `TDSMessageDecoder`.
    public init() {
        self.hasSeenFirstMessage = false
    }
    
    /// See `ByteToMessageDecoder`.
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard let packet = TDSPacket(from: &buffer) else {
            return .needMoreData
        }
        
        context.fireChannelRead(wrapInboundOut(packet))
        
        // Don't check, just set. It's faster that way
        self.hasSeenFirstMessage = true
        return .continue
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        while let packet = TDSPacket(from: &buffer) {
            context.fireChannelRead(wrapInboundOut(packet))
        }
        
        return .needMoreData
    }
}

extension ByteBuffer {
    fileprivate mutating func readPacketData() -> [Byte]? {
        return []
    }
}

