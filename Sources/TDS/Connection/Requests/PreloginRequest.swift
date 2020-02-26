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
    
    func respond(to message: TDSMessage, allocator: ByteBufferAllocator) throws -> TDSMessage? {
        switch message.headerType {
        case .preloginResponse:
            var messageBuffer = try ByteBuffer(unpackingDataFrom: message, allocator: allocator)
            let message = try TDSMessages.PreloginResponse.parse(from: &messageBuffer)

            print("Prelogin Response Version: \(message.body.version)")
            print("Prelogin Response Encrytion: \(message.body.encryption)")

            if let enc = message.body.encryption {
                switch enc {
                case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
                    let message = try TDSMessage.init(packetType: TDSMessages.SSLKickoff(), allocator: allocator)
                    return message
                default:
                    throw TDSError.protocol("PRELOGIN Error: Server does not supprt encryption.")
                }
            }
        default:
            break
        }
        
        return nil
    }
    
    func start(allocator: ByteBufferAllocator) throws -> TDSMessage {
        let prelogin = TDSMessages.PreloginPacket(version: "9.0.0", encryption: .encryptOn)
        let message = try TDSMessage.init(packetType: prelogin, allocator: allocator)
        return message
    }
}
