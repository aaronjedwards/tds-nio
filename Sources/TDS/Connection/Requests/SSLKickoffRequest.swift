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
    
    func respond(to message: TDSMessage) throws -> TDSMessage? {
        return nil
    }
    
    func start() throws -> TDSMessage {
        return try TDSMessage.SSLKickoff().message()
    }
}
