import Logging
import NIO
extension TDSConnection {
    public func sslKickoff() -> EventLoopFuture<Void> {
        let auth = SSLKickoffRequest()
        return self.send(auth)
    }
}

// MARK: Private

private final class SSLKickoffRequest: TDSRequest {
    init() {}
    
    func log(to logger: Logger) {
        logger.debug("Kicking off Prelogin SSL Handshake.")
    }
    
    func respond(to message: [TDSPacket], allocator: ByteBufferAllocator) throws -> [TDSPacket]? {
        return nil
    }
    
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let packet = try TDSPacket(message: TDSMessages.SSLKickoff(), isLastPacket: true, allocator: allocator)
        return [packet]
    }
}
