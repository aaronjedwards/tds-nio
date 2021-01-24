import NIO
import Logging

public final class TDSMessageDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    public typealias InboundOut = TDSMessage
    
    /// See `ByteToMessageDecoder`.
    private var storedPackets = [TDSPacket]()

    let logger: Logger
    
    /// Creates a new `TDSMessageDecoder`.
    public init(logger: Logger) {
        self.logger = logger
    }
    
    /// See `ByteToMessageDecoder`.
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        while let packet = TDSPacket(from: &buffer) {
            self.storedPackets.append(packet)

            if packet.header.status == .eom {
                let message = TDSMessage(packets: storedPackets)
                context.fireChannelRead(wrapInboundOut(message))
                storedPackets.removeAll(keepingCapacity: true)
                logger.debug("Decoded message with type: \(message.headerType)")
                return .continue
            }
            
            return .continue
        }
        
        return .needMoreData
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        logger.debug("Decoding last")
        return .needMoreData
    }
}

extension ByteBuffer {
    fileprivate mutating func readPacketData() -> [Byte]? {
        return []
    }
}

