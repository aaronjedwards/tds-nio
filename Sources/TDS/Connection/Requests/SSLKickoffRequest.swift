import Logging
import NIO
extension TDSConnection {
    private func sslKickoff() -> EventLoopFuture<Void> {
        let auth = SSLKickoffRequest()
        return self.send(auth, logger: logger)
    }
}

// MARK: Private

private final class SSLKickoffRequest: TDSRequest {
    init() {}
    
    func log(to logger: Logger) {
        logger.debug("Kicking off Prelogin SSL Handshake.")
    }
    
    func respond(to packet: TDSPacket, allocator: ByteBufferAllocator) throws -> [TDSPacket]? {
        return nil
    }
    
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let message = try TDSMessage(packetType: TDSMessage.SSLKickoff(), allocator: allocator)
        return message.packets
    }
}
