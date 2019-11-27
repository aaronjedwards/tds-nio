import NIO

public final class TDSMessageEncoder: MessageToByteEncoder {
    /// See `MessageToByteEncoder`.
    public typealias OutboundIn = TDSMessage
    
    /// See `MessageToByteEncoder`.
    public func encode(data message: TDSMessage, out: inout ByteBuffer) throws {
        
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}

