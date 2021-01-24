import Logging
import TDS
import XCTest

extension TDSConnection {
    static func address() throws -> SocketAddress {
        try .makeAddressResolvingHost(env("TDS_HOSTNAME") ?? "swift-tds.database.windows.net", port: 1433)
    }
    
    static func testUnauthenticated(on eventLoop: EventLoop) -> EventLoopFuture<TDSConnection> {
        do {
            return connect(to: try address(), serverHostname: env("TDS_HOSTNAME") ?? "swift-tds.database.windows.net", on: eventLoop)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    static func test(on eventLoop: EventLoop) -> EventLoopFuture<TDSConnection> {
        return testUnauthenticated(on: eventLoop).flatMap { conn in
            return conn.login(
                username: env("TDS_USERNAME") ?? "swift_tds_user",
                password: env("TDS_PASSWORD") ?? "RP9f7PVffK6U8b9ek@Q9eH-8",
                server: env("TDS_SEVERNAME") ?? "swift-tds.database.windows.net",
                database: env("TDS_DATABASE") ?? "swift-tds"
            ).map {
                return conn
            }.flatMapError { error in
                conn.close().flatMapThrowing {
                    throw error
                }
            }
        }
    }
}

extension NSRegularExpression {
    convenience init(_ pattern: String) {
        do {
            try self.init(pattern: pattern)
        } catch {
            preconditionFailure("Illegal regular expression: \(pattern).")
        }
    }
    
    func matches(_ string: String?) -> Bool {
        guard let str = string else { return false }
        let range = NSRange(location: 0, length: str.utf16.count)
        return firstMatch(in: str, options: [], range: range) != nil
    }
}

let sqlServerVersionPattern = "[0-9]{2}\\.[0-9]{1}\\.[0-9]{4}\\.[0-9]{1}"
