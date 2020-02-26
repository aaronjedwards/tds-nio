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
    
    func respond(to message: [TDSPacket], allocator: ByteBufferAllocator) throws -> [TDSPacket]? {
        let inbound = message[0]
        var messageBuffer = inbound.messageBuffer
        switch inbound.headerType {
        case .preloginResponse:
            let message = try TDSMessages.PreloginResponse.parse(from: &messageBuffer)
            print("Prelogin Response Version: \(message.body.version)")
            print("Prelogin Response Encrytion: \(message.body.encryption)")
            if let enc = message.body.encryption {
                switch enc {
                case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
                    let outbound = try TDSPacket(message: TDSMessages.SSLKickoff(), isLastPacket: true, allocator: allocator)
                    return [outbound]
                default:
                    throw TDSError.protocol("PRELOGIN Error: Server does not supprt encryption.")
                }
            }
        default:
            break
        }
        
        return nil
    }
    
    func start( allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let message = TDSMessages.PreloginPacket(version: "9.0.0", encryption: .encryptOn)
        let packet = try TDSPacket(message: message, isLastPacket: true, allocator: allocator)
        
        return [packet]
    }
}
