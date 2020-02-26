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
    
    func respond(to packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacket? {
        var messageBuffer = packet.messageBuffer
        switch packet.headerType {
        case .preloginResponse:
            let message = try TDSMessages.PreloginResponse.parse(from: &messageBuffer)
            print("Prelogin Response Version: \(message.body.version)")
            print("Prelogin Response Encrytion: \(message.body.encryption)")
            if let enc = message.body.encryption {
                switch enc {
                case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
                    return try TDSPacket(message: TDSMessages.SSLKickoff(), allocator: allocator)
                default:
                    throw TDSError.protocol("PRELOGIN Error: Server does not supprt encryption.")
                }
            }
        default:
            break
        }
        return nil
    }
    
    func start(allocator: ByteBufferAllocator) throws -> TDSPacket {
        let message = TDSMessages.PreloginMessage(version: "9.0.0", encryption: .encryptOn)
        return try TDSPacket(message: message, allocator: allocator)
    }
}
