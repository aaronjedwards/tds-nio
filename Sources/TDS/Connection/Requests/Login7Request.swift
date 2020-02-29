import Logging
import NIO
import Foundation

extension TDSConnection {
    public func login(username: String, password: String, database: String = "master") -> EventLoopFuture<Void> {
        let auth = TDSMessages.Login7Request(
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
    let login: TDSMessages.Login7Request

    func respond(to message: TDSMessage, allocator: ByteBufferAllocator) throws -> TDSMessage? {
        var messageBuffer = message.firstPacket.messageBuffer

        guard
            let token = messageBuffer.readInteger(as: UInt8.self),
            let tokenType = TDSMessages.TokenType(rawValue: token)
        else {
            throw TDSError.protocolError("Invalid token type in Login7 response")
        }

        switch tokenType {
        case .error:
            throw TDSError.invalidCredentials
        case .info:
            throw TDSError.protocolError("Unsupported INFO TokenType")
        case .envchange:
            try TDSMessages.parseEnvChangeTokenStream(messageBuffer: &messageBuffer)
            return nil
        case .done:
            print("Authenticated as user \(login.username)")
            return nil
        }
    }

    func start(allocator: ByteBufferAllocator) throws -> TDSMessage {
        let packet = try TDSPacket(message: login, isLastPacket: true, allocator: allocator)
        return TDSMessage(packets: [packet])
    }

    func log(to logger: Logger) {
        logger.log(level: .debug, "Logging in as \(login.username)")
    }
}
