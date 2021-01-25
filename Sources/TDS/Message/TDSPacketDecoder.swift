import NIO
import Logging

public final class TDSPacketDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    public typealias InboundOut = TDSPacket

    let logger: Logger
    
    /// Creates a new `TDSPacketDecoder`.
    public init(logger: Logger) {
        self.logger = logger
    }
    
    /// See `ByteToMessageDecoder`.
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        while let packet = TDSPacket(from: &buffer) {
            context.fireChannelRead(wrapInboundOut(packet))
            logger.debug("Decoded TDSPacket with type: \(packet.headerType)")
            return .continue
        }
        
        return .needMoreData
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        logger.debug("Decoding last")
        return .needMoreData
    }
}
