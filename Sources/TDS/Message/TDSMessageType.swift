import NIO

public protocol TDSMessage {
    static var headerType: TDSPacket.HeaderType { get }
    static func parse(from buffer: inout ByteBuffer) throws -> Self
    func serialize(into buffer: inout ByteBuffer) throws
}

extension TDSMessage {
    public init(packet: TDSPacket) throws {
        var messageBuffer = packet.messageBuffer!
        self = try Self.parse(from: &messageBuffer)
    }
    
    public static func parse(from buffer: inout ByteBuffer) throws -> Self {
        fatalError("\(Self.self) does not support parsing.")
    }
    
    public func serialize(into buffer: inout ByteBuffer) throws {
        fatalError("\(Self.self) does not support serializing.")
    }
}
