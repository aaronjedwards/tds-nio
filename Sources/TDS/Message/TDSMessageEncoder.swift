import NIO

public final class TDSMessageEncoder: MessageToByteEncoder {
    /// See `MessageToByteEncoder`.
    public typealias OutboundIn = TDSMessage
    
    /// See `MessageToByteEncoder`.
    public func encode(data messages: TDSMessage, out: inout ByteBuffer) throws {
        messages.writeToByteBuffer(&out)
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}

extension MessageToByteHandler: RemovableChannelHandler {}

