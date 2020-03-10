import Logging
import NIO
import Foundation

extension TDSConnection {
    public func login(username: String, password: String, database: String = "master") -> EventLoopFuture<Void> {
        let auth = TDSMessages.Login7Message(
            hostname: "localhost",
            username: username,
            password: password,
            appName: "TDSTester",
            serverName: "",
            clientInterfaceName: "SwiftTDS",
            language: "",
            database: database,
            sspiData: "")
        return self.send(Login7Request(login: auth))
    }
}

struct Login7Request: TDSRequest {
    let login: TDSMessages.Login7Message

    func respond(to message: TDSMessage, allocator: ByteBufferAllocator) throws -> TDSMessage? {
        var messageBuffer = try ByteBuffer(unpackingDataFrom: message, allocator: allocator)
        _ = try TDSMessages.LoginResponse.parse(from: &messageBuffer)
        // TODO: Set logged in ready state
        // TODO: React to envchange request from server
        return nil
    }

    func start(allocator: ByteBufferAllocator) throws -> TDSMessage {
        let message = try TDSMessage(packetType: login, allocator: allocator)
        return message
    }

    func log(to logger: Logger) {
        logger.log(level: .debug, "Logging in as \(login.username)")
    }
}
