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
            logger.debug("Received TDS Packet - type: \(packet.headerType.description), status: \(packet.header.status.description), length: \(packet.header.length)")
            self.storedPackets.append(packet)
            if packet.header.status == .eom {
                let message = TDSMessage(packets: storedPackets)
                context.fireChannelRead(wrapInboundOut(message))
                storedPackets.removeAll(keepingCapacity: true)

                logger.debug("Received complete TDS Message - type: \(message.headerType.description), packet count: \(message.packets.count)")
                
                return .continue
            }

            logger.debug("TDS Message incomplete, reading more data.")
            return .continue
        }
        
        return .needMoreData
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        logger.debug("Decoding last")
        return try decode(context: context, buffer: &buffer)
    }
}

extension ByteBuffer {
    fileprivate mutating func readPacketData() -> [Byte]? {
        return []
    }
}

