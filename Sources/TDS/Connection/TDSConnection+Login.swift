import Logging
import NIO
import Foundation

extension TDSConnection {
    public func login(username: String, password: String? = nil, server: String? = nil, database: String? = nil) -> EventLoopFuture<Void> {
        let auth = TDSMessage.Login7Message(
            hostname: Host.current().name ?? "",
            username: username,
            password: password ?? "",
            appName: "",
            serverName: server ?? "",
            clientInterfaceName: "SwiftTDS",
            language: "",
            database: database ?? "master",
            sspiData: ""
        )
        return self.send(LoginRequest(login: auth, logger: logger), logger: logger)
    }
}

class LoginRequest: TDSRequest {
    private let login: TDSMessage.Login7Message
    private let logger: Logger
    
    private let tokenParser: TDSTokenParser
    
    init(login: TDSMessage.Login7Message, logger: Logger) {
        self.login = login
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
        let message = try TDSMessage(payload: login, allocator: allocator)
        return message.packets
    }

    func log(to logger: Logger) {
        logger.debug("Logging in as \(login.username)")
    }
}
