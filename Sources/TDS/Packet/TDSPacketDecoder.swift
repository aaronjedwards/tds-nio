import NIO
import Logging

public final class TDSPacketDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    public typealias InboundOut = TDSPacket

    let logger: Logger
    private var fragments: [ByteBuffer] = []
    var lastHeaderReceived: TDSPacket.Header!
    
    /// Creates a new `TDSPacketDecoder`.
    public init(logger: Logger) {
        self.logger = logger
    }
    
    /// See `ByteToMessageDecoder`.
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard
            let length: UInt16 = buffer.getInteger(at: buffer.readerIndex + 2),
            buffer.readableBytes >= length, // Check to see if complete first packet has been sent through
            let header = TDSPacket.Header(from: buffer), // Keep record of first packet header
            let slice = buffer.readSlice(length: Int(length))
        else {
            return .needMoreData
        }
        
        // Intialise vars used for creation of struct
        self.lastHeaderReceived = header
        self.fragments.append(slice)
        
        // Process additionnal data in TCP Packet if present
        while buffer.readerIndex < buffer.writerIndex {
            guard
                let type: UInt8 = buffer.getInteger(at: buffer.readerIndex),
                let status: UInt8 = buffer.getInteger(at: buffer.readerIndex + 1),
                let length: UInt16 = buffer.getInteger(at: buffer.readerIndex + 2),
                buffer.readableBytes >= length,
                let slice = buffer.readSlice(length: Int(length))
            else {
                return .needMoreData
            }
            
            // Keep copy of latest header received
            self.lastHeaderReceived = TDSPacket.Header(type: TDSPacket.HeaderType(integerLiteral: type),
                                                       status: TDSPacket.Status(integerLiteral: status),
                                                       length: length)
            
            // Append fragment to array
            let newBuffer = ByteBufferAllocator().buffer(buffer: slice)
            fragments.append(newBuffer)
        }

        // Check if first packet is also last packet
        if lastHeaderReceived.status == .eom {
            return try flushFragmentsAndRead(context, self.fragments, {
                // Clear fragment buffer
                self.fragments = [ByteBuffer]()
            })
        } else {
            return .needMoreData
        }
    }
    
    fileprivate func flushFragmentsAndRead(_ context: ChannelHandlerContext, _ fragments: [ByteBuffer], _ completion: @escaping ()->()) throws -> DecodingState{
        guard fragments.count > 0 else { throw TDSError.protocolError("No fragments found in buffer") }
        // Initilase new buffer to allocate fragments too. Will set header at the end.
        var tempBuffer = ByteBufferAllocator().buffer(capacity: 8)
        tempBuffer.writeBytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,])
        
        // Parse remaining fragments for data
        for (index, fragment) in fragments.enumerated() {
            guard
                let length: UInt16 = fragment.getInteger(at: fragment.readerIndex + 2),
                //Discard header of tabular data packet
                let fragBuffer = fragment.getSlice(at: 8, length: Int(length) - 8)
            else {
                throw TDSError.protocolError("Failed to parse fragment #\(index)")
            }
            
            //Append fragment to reconstruction
            let count = tempBuffer.setBuffer(fragBuffer, at: tempBuffer.writerIndex)
            tempBuffer.moveWriterIndex(forwardBy: count)
        }
        // Modify header to reflect being last packet
        tempBuffer.setInteger(lastHeaderReceived.type.value, at: 0)
        tempBuffer.setInteger(lastHeaderReceived.status.value, at: 1)
        
        guard let packet = TDSPacket(from: &tempBuffer) else {
            throw TDSError.protocolError("Failed to parse TDSPacket")
        }
        
        // Send packet to parse tokens
        context.fireChannelRead(wrapInboundOut(packet))
        completion()
        return .continue
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        logger.debug("Decoding last")
        return .needMoreData
    }
}
