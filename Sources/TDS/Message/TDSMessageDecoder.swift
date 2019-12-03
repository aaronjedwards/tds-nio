import NIO

public final class TDSMessageDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    public typealias InboundOut = TDSMessage
    
    /// See `ByteToMessageDecoder`.
    public var cumulationBuffer: ByteBuffer?
    
    /// If `true`, the server has asked for authentication.
    public var hasSeenFirstMessage: Bool
    
    /// Creates a new `PostgresMessageDecoder`.
    public init() {
        self.hasSeenFirstMessage = false
    }
    
    private func parsePackets(buffer: inout ByteBuffer) -> [TDSPacket]? {
        var packets: [TDSPacket] = []
        
        var readBytes = true
        // Try and read a complete message worth of packets
        while(readBytes) {
            // Read packet header
            guard let header = buffer.readPacketHeader() else {
                return nil
            }
            
            if header.status.value == TDSPacket.Status.eom {
                readBytes = false
            }
            
            let packetSize = Int(header.length)
            
            // ensure message is large enough (skipping message type) or reject
            guard let data = buffer.readSlice(length: packetSize - 8) else {
                return nil
            }
            
            let packet = TDSPacket(header: header, data: data)
            packets.append(packet)
        }
        
        return packets
    }
    
    /// See `ByteToMessageDecoder`.
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        var bufferCopy = buffer
        
        // Parse a complete se of packets to make up a TDSMessage
        guard let packets = parsePackets(buffer: &bufferCopy) else {
            return .needMoreData
        }
        
        guard let message = TDSMessage(packets: packets) else {
            throw TDSError.protocol("Message Decoding Error: Unable to create a copmlete message from packets.")
        }
        
        // there is sufficient data, use this buffer
        buffer = bufferCopy
        
        context.fireChannelRead(wrapInboundOut(message))
        return .continue
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        // ignore
        return .needMoreData
    }
}

extension ByteBuffer {
    fileprivate mutating func readPacketHeader() -> TDSPacket.Header? {
        guard
            let headerType = self.readInteger(as: UInt8.self).map(TDSPacket.HeaderType.init),
            let status = self.readInteger(as: UInt8.self).map(TDSPacket.Status.init),
            let length = self.readInteger(as: UInt16.self),
            let spid = self.readInteger(as: UInt16.self),
            let packetId = self.readInteger(as: UInt8.self),
            let window = self.readInteger(as: UInt8.self)
            else {
                return nil
        }
        
        let header = TDSPacket.Header(
            type: headerType,
            status: status,
            length: length,
            spid: spid,
            packetId: packetId,
            window: window
        )
        
        return header
    }
    
    fileprivate mutating func readPacketData() -> [Byte]? {
        return []
    }
}

