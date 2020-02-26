import Logging
import NIO
import Foundation

extension TDSConnection {
    public func prelogin() -> EventLoopFuture<Void> {
        let auth = PreloginRequest()
        return self.send(auth)
    }
}

// MARK: Private

private final class PreloginRequest: TDSRequest {
    enum State {
        case start
    }
    
    init() {}
    
    func log(to logger: Logger) {
        logger.debug("Sending Prelogin Packet)")
    }
    
    func respond(to message: TDSMessage) throws -> TDSMessage? {
        switch message.headerType {
        case .preloginResponse:
            let message = try TDSMessage.PreloginResponse.init(message: message)
            print("Prelogin Response Version: \(message.body.version)")
            print("Prelogin Response Encryption: \(message.body.encryption)")
            if let enc = message.body.encryption {
                switch enc {
                case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
                    return try TDSMessage.SSLKickoff().message()
                default:
                    throw TDSError.protocol("PRELOGIN Error: Server does not supprt encryption.")
                }
            }
        default:
            break
        }
        return nil
    }
    
    func start() throws -> TDSMessage {
        return try TDSMessage.PreloginMessage(version: "9.0.0", encryption: .encryptOn).message()
    }
}
