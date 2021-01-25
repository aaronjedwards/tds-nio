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
    
    private var storedPackets = [TDSPacket]()
    
    init(_ shouldNegotiateEncryption: Bool) {
        self.clientEncryption = shouldNegotiateEncryption ? .encryptOn : .encryptNotSup
    }
    
    func log(to logger: Logger) {
        logger.debug("Sending Prelogin Packet")
    }
    
    func respond(to packet: TDSPacket, allocator: ByteBufferAllocator) throws -> [TDSPacket]? {
        storedPackets.append(packet)
        
        guard packet.header.status == .eom else {
            return []
        }
        
        switch packet.headerType {
        case .preloginResponse:
            var messageBuffer = ByteBuffer(from: storedPackets, allocator: allocator)
            guard let parsedMessage = try? TDSMessage.PreloginResponse.parse(from: &messageBuffer) else {
                throw TDSError.protocolError("Unable to parse prelogin response from message contents.")
            }
            
            // Encryption Negotiation - Supports all or nothing encryption
            if let serverEncryption = parsedMessage.encryption {
                switch (serverEncryption, clientEncryption) {
                case (.encryptReq, .encryptOn),
                     (.encryptOn, .encryptOn):
                    // encrypt connection
                    let packet = TDSPacket.empty(type: .sslKickoff, allocator: allocator)
                    return [packet]
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
    
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let prelogin = TDSMessage.PreloginMessage(version: "9.0.0", encryption: clientEncryption)
        let message = try TDSMessage(packetType: prelogin, allocator: allocator)
        return message.packets
    }
}
