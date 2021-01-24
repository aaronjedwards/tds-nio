import Logging
import NIO
import Foundation

extension TDSConnection {
    internal func prelogin(shouldNegotiateEncryption: Bool) -> EventLoopFuture<Void> {
        let auth = PreloginRequest(shouldNegotiateEncryption)
        return self.send(auth, logger: logger)
    }
}

// MARK: Private

internal final class PreloginRequest: TDSRequest {
    private let clientEncryption: TDSMessage.PreloginEncryption
    
    init(_ shouldNegotiateEncryption: Bool) {
        self.clientEncryption = shouldNegotiateEncryption ? .encryptOn : .encryptNotSup
    }
    
    func log(to logger: Logger) {
        logger.debug("Sending Prelogin Packet")
    }
    
    func respond(to message: TDSMessage, allocator: ByteBufferAllocator) throws -> TDSMessage? {
        switch message.headerType {
        case .preloginResponse:
            var messageBuffer = try ByteBuffer(unpackingDataFrom: message, allocator: allocator)
            let parsedMessage = try TDSMessage.PreloginResponse.parse(from: &messageBuffer)
            
            // Encryption Negotiation - Supports all or nothing encryption
            if let serverEncryption = parsedMessage.body.encryption {
                switch (serverEncryption, clientEncryption) {
                case (.encryptReq, .encryptOn),
                     (.encryptOn, .encryptOn):
                    // encrypt connection
                    return try TDSMessage(packetType: TDSMessage.SSLKickoff(), allocator: allocator)
                case (.encryptNotSup, .encryptNotSup):
                    // no encryption
                    return nil
                default:
                    throw TDSError.protocolError("PRELOGIN Error: Incompatible client/server encyption configuration. Client: \(clientEncryption), Server: \(serverEncryption)")
                }
            }
        default:
            break
        }
        
        return nil
    }
    
    func start(allocator: ByteBufferAllocator) throws -> TDSMessage {
        let prelogin = TDSMessage.PreloginMessage(version: "9.0.0", encryption: clientEncryption)
        let message = try TDSMessage(packetType: prelogin, allocator: allocator)
        return message
    }
}
