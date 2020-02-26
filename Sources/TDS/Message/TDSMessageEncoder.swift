import NIO

public final class TDSMessageEncoder: MessageToByteEncoder {
    /// See `MessageToByteEncoder`.
    public typealias OutboundIn = [TDSPacket]
    
    /// See `MessageToByteEncoder`.
    public func encode(data messages: [TDSPacket], out: inout ByteBuffer) throws {
        for var message in messages {
            out.writeBuffer(&message.buffer)
        }
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}

extension MessageToByteHandler: RemovableChannelHandler {}

