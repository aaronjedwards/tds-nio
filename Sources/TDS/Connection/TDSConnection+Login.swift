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

class LoginRequest: TDSTokenStreamRequest {
    
    private let payload: TDSMessages.Login7Message
    private let logger: Logger
    
    private let tokenParser: TDSTokenParser
    
    init(payload: TDSMessages.Login7Message, logger: Logger) {
        self.payload = payload
        self.logger = logger
        self.tokenParser = TDSTokenParser(logger: logger)
    }

    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let message = try TDSMessage(payload: payload, allocator: allocator)
        return message.packets
    }
    
    func handle(token: TDSToken) throws {
        return
    }
    
    func complete(message: inout ByteBuffer, allocator: ByteBufferAllocator) throws -> TDSRequestResponse {
        return .done
    }

    func log(to logger: Logger) {
        logger.debug("Logging in as user: \(payload.username) to database: \(payload.database) and server: \(payload.serverName)")
    }
}
