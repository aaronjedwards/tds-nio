import NIO

public protocol TDSMessageType {
    /// Type of all packets that are contained in a given message
    static var headerType: TDSPacket.HeaderType { get }
    /// Buffer supplied here contains the raw message data extracted from the packets (ie. no packet header data)
    static func parse(from buffer: inout ByteBuffer) throws -> Self
    func serialize(into buffer: inout ByteBuffer) throws
}

extension TDSMessageType {

    public static func parse(from buffer: inout ByteBuffer) throws -> Self {
        fatalError("\(Self.self) does not support parsing.")
    }
    
    public func serialize(into buffer: inout ByteBuffer) throws {
        fatalError("\(Self.self) does not support serializing.")
    }
}
