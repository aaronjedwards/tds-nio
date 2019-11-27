import NIO

public protocol TDSMessageType {
    static var headerType: TDSPacket.HeaderType { get }
    static func parse(from buffer: inout ByteBuffer) throws -> Self
    func serialize(into buffer: inout ByteBuffer) throws
}

extension TDSMessageType {
    func message() throws -> TDSMessage {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        try self.serialize(into: &buffer)
        return .init(headerType: Self.headerType, data: buffer)
    }
    
    public init(message: TDSMessage) throws {
        var message = message
        self = try Self.parse(from: &message.data)
    }
    
    public static func parse(from buffer: inout ByteBuffer) throws -> Self {
        fatalError("\(Self.self) does not support parsing.")
    }
    
    public func serialize(into buffer: inout ByteBuffer) throws {
        fatalError("\(Self.self) does not support serializing.")
    }
}
