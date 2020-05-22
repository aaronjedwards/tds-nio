import NIO
import Logging

public final class TDSMessageEncoder: MessageToByteEncoder {
    /// See `MessageToByteEncoder`.
    public typealias OutboundIn = TDSMessage

    let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }
    
    /// See `MessageToByteEncoder`.
    public func encode(data message: TDSMessage, out: inout ByteBuffer) throws {
        message.writeToByteBuffer(&out)
        logger.debug("Encoding TDSMessage - type: \(message.headerType.description), packet count: \(message.packets.count)")
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}

extension MessageToByteHandler: RemovableChannelHandler {}

