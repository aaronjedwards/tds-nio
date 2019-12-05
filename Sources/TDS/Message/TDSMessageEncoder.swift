import NIO

public final class TDSMessageEncoder: MessageToByteEncoder {
    /// See `MessageToByteEncoder`.
    public typealias OutboundIn = TDSMessage
    
    /// See `MessageToByteEncoder`.
    public func encode(data message: TDSMessage, out: inout ByteBuffer) throws {
        // print("TDSMessage.ChannelEncoder.encode(\(message))")
        var message = message
        
        // serialize the message data
        out.writeBuffer(&message.data)
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}

extension MessageToByteHandler: RemovableChannelHandler {}

