import NIO

public protocol TDSMessagePayload {
    /// Type of all packets that are contained in a given message
    static var packetType: TDSPacket.HeaderType { get }
    
    /// Buffer supplied here contains the raw message data extracted from the packets (ie. no packet header data)
    static func parse(from buffer: inout ByteBuffer) throws -> Self
    
    /// Serializes the message payload into the supplied buffer
    func serialize(into buffer: inout ByteBuffer) throws
}

extension TDSMessagePayload {
    public static func parse(from buffer: inout ByteBuffer) throws -> Self {
        fatalError("\(Self.self) does not support parsing.")
    }
    
    public func serialize(into buffer: inout ByteBuffer) throws {
        fatalError("\(Self.self) does not support serializing.")
    }
}
