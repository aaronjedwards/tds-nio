import Logging
import NIO
import Foundation

extension TDSConnection {
    public func login(hostname: String? = nil, username: String, password: String? = nil, serverName: String? = nil, database: String? = nil) -> EventLoopFuture<Void> {
        let auth = TDSMessage.Login7Message(
            hostname: hostname ?? "",
            username: username,
            password: password ?? "",
            appName: "",
            serverName: serverName ?? "",
            clientInterfaceName: "SwiftTDS",
            language: "",
            database: database ?? "master",
            sspiData: ""
        )
        return self.send(Login7Request(login: auth), logger: logger)
    }
}

struct Login7Request: TDSRequest {
    let login: TDSMessage.Login7Message

    func respond(to message: TDSMessage, allocator: ByteBufferAllocator) throws -> TDSMessage? {
        var messageBuffer = try ByteBuffer(unpackingDataFrom: message, allocator: allocator)
        let _ = try TDSMessage.LoginResponse.parse(from: &messageBuffer)
        // TODO: Set logged in ready state
        // TODO: React to envchange request from server
        return nil
    }

    func start(allocator: ByteBufferAllocator) throws -> TDSMessage {
        let message = try TDSMessage(packetType: login, allocator: allocator)
        return message
    }

    func log(to logger: Logger) {
        logger.debug("Logging in as \(login.username)")
    }
}
