import Logging
import NIO
import Foundation

extension TDSConnection {
    func prelogin(shouldNegotiateEncryption: Bool) -> EventLoopFuture<Void> {
        let auth = PreloginRequest(shouldNegotiateEncryption)
        return self.send(auth, logger: logger)
    }
}

// MARK: Private

private final class PreloginRequest: TDSRequest {
    private let clientEncryption: TDSMessages.PreloginEncryption
    
    init(_ shouldNegotiateEncryption: Bool) {
        self.clientEncryption = shouldNegotiateEncryption ? .encryptReq : .encryptNotSup
    }
    
    func log(to logger: Logger) {
        logger.debug("Sending Prelogin message.")
    }
    
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let prelogin = TDSMessages.PreloginMessage(version: "9.0.0", encryption: clientEncryption)
        let message = try TDSMessage(payload: prelogin, allocator: allocator)
        return message.packets
    }
    
    func complete(message: inout ByteBuffer, allocator: ByteBufferAllocator) throws -> TDSRequestResponse {
        guard let parsedMessage = try? TDSMessages.PreloginResponse.parse(from: &message) else {
            throw TDSError.protocolError("Unable to parse prelogin response from message contents.")
        }
        
        // Encryption Negotiation - Supports all or nothing encryption
        if let serverEncryption = parsedMessage.encryption {
            switch (serverEncryption, clientEncryption) {
            case (.encryptOn, .encryptReq):
                // encrypt connection
                return .kickoffSSL
            case (.encryptNotSup, .encryptNotSup):
                // no encryption
                return .done
            default:
                throw TDSError.protocolError("PRELOGIN Error: Incompatible client/server encyption configuration. Client: \(clientEncryption), Server: \(serverEncryption)")
            }
        }
        
        return .done
    }
}
