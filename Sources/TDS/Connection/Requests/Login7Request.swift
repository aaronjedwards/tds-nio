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
        return self.send(Login7Request(login: auth), logger: logger)
    }
}

class Login7Request: TDSRequest {
    let login: TDSMessage.Login7Message
    
    private var storedPackets = [TDSPacket]()
    
    init(login: TDSMessage.Login7Message) {
        self.login = login
    }

    func respond(to packet: TDSPacket, allocator: ByteBufferAllocator) throws -> [TDSPacket]? {
        storedPackets.append(packet)
        
        guard packet.header.status == .eom else {
            return []
        }
        
        var messageBuffer = ByteBuffer(from: storedPackets, allocator: allocator)
        let _ = try TDSMessage.LoginResponse.parse(from: &messageBuffer)
        // TODO: Set logged in ready state
        // TODO: React to envchange request from server
        return nil
    }

    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let message = try TDSMessage(packetType: login, allocator: allocator)
        return message.packets
    }

    func log(to logger: Logger) {
        logger.debug("Logging in as \(login.username)")
    }
}
