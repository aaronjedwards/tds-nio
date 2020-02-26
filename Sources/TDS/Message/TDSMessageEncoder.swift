import NIO

public final class TDSMessageEncoder: MessageToByteEncoder {
    /// See `MessageToByteEncoder`.
    public typealias OutboundIn = TDSPacket
    
    /// See `MessageToByteEncoder`.
    public func encode(data message: TDSPacket, out: inout ByteBuffer) throws {
        // serialize the message data
        var buffer = message.messageBuffer
        out.writeBuffer(&buffer)
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}

extension MessageToByteHandler: RemovableChannelHandler {}

