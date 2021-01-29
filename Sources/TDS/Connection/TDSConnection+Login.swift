import Logging
import NIO
import Foundation

extension TDSConnection {
    public func login(username: String, password: String, server: String, database: String) -> EventLoopFuture<Void> {
        let payload = TDSMessages.Login7Message(
            username: username,
            password: password,
            serverName: server,
            database: database
        )
        return self.send(LoginRequest(payload: payload, logger: logger), logger: logger)
    }
}

class LoginRequest: TDSRequest {
    private let payload: TDSMessages.Login7Message
    private let logger: Logger
    
    private let tokenParser: TDSTokenParser
    
    init(payload: TDSMessages.Login7Message, logger: Logger) {
        self.payload = payload
        self.logger = logger
        self.tokenParser = TDSTokenParser(logger: logger)
    }

    func handle(packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        // Add packet to token parser stream
        let tokens = tokenParser.writeAndParseTokens(packet.messageBuffer)
        
        guard packet.header.status == .eom else {
            return .continue
        }
        
        // TODO: Set logged in ready state
        // TODO: React to envchange request from server
        
        return .done
    }

    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let message = try TDSMessage(payload: payload, allocator: allocator)
        return message.packets
    }

    func log(to logger: Logger) {
        logger.debug("Logging in as user: \(payload.username) to database: \(payload.database) and server: \(payload.serverName)")
    }
}
