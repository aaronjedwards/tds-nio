import XCTest
import Logging
import NIOTestUtils
@testable import TDS

final class TDSTests: XCTestCase {
    
    private var group: EventLoopGroup!
    
    private var eventLoop: EventLoop { self.group.next() }
    
    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }
    
    override func tearDownWithError() throws {
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }
    
    // MARK: Tests
    func testConnectAndClose() throws {
        let conn = try TDSConnection.test(on: eventLoop).wait()
        try conn.close().wait()
    }
    
    func testRawSqlVersion() throws {
        let conn = try TDSConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.rawSql("SELECT @@VERSION AS version").wait()
        XCTAssertEqual(rows.count, 1)
        
        let version = rows[0].column("version")?.string
        let regex = try NSRegularExpression(pattern: sqlServerVersionPattern)
        XCTAssertEqual(regex.matches(version), true)
    }
    
    func testRawSqlGetDate() throws {
        let conn = try TDSConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.rawSql("SELECT GETDATE() AS timestamp").wait()
        XCTAssertEqual(rows.count, 1)
        
        let date = rows[0].column("timestamp")?.date
        XCTAssertEqual(date != nil, true)
    }
    
    func testRawSqlLargeResultSet() throws {
        let conn = try TDSConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.rawSql("""
            SELECT TOP(10000)
                  [id]
                  , [first_name]
                  , [last_name]
                  , [email]
                  , [gender]
                  , [ip_address]
            FROM [dbo].[applicationUsers]
        """)
        .wait()
        XCTAssertEqual(rows.count, 10000)
    }
    
    func testLocalTLSServerWithSelfSignedCertificate() throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try! elg.syncShutdownGracefully() }

        let currentPathComponents = #file.split(separator: "/")
        let directory = currentPathComponents.dropLast(1).map(String.init).joined(separator: "/")
        let relativePathToCert = "/\(directory)/../../scripts/certificate.pem"
        let conn = try TDSConnection.connect(
            to: try SocketAddress.makeAddressResolvingHost("localhost", port: 1433),
            tlsConfiguration: .forClient(trustRoots: .file(relativePathToCert)),
            serverHostname: "localhost",
            on: eventLoop
        ).wait()

        try conn.login(
            username: "sa",
            password: "<YourStrong@Passw0rd>",
            server: "localhost",
            database: "master"
        ).wait()

        defer { try! conn.close().wait() }
        
        let rows = try conn.rawSql("SELECT @@VERSION AS version").wait()
        XCTAssertEqual(rows.count, 1)
        
        let version = rows[0].column("version")?.string
        let regex = try NSRegularExpression(pattern: sqlServerVersionPattern)
        XCTAssertEqual(regex.matches(version), true)
    }
    
    func testRemoteTLSServer() throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try! elg.syncShutdownGracefully() }

        let conn = try TDSConnection.connect(
            to: try SocketAddress.makeAddressResolvingHost("swift-tds.database.windows.net", port: 1433),
            serverHostname: "swift-tds.database.windows.net",
            on: eventLoop
        ).wait()

        try conn.login(
            username: "swift_tds_user",
            password: "RP9f7PVffK6U8b9ek@Q9eH-8",
            server: "swift-tds.database.windows.net",
            database: "swift-tds"
        ).wait()

        defer { try? conn.close().wait() }
        
        let rows = try conn.rawSql("SELECT @@VERSION AS version").wait()
        XCTAssertEqual(rows.count, 1)
        
        let version = rows[0].column("version")?.string
        let regex = try NSRegularExpression(pattern: sqlServerVersionPattern)
        XCTAssertEqual(regex.matches(version), true)
    }
}

func env(_ name: String) -> String? {
    getenv(name).flatMap { String(cString: $0) }
}

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .debug
        return handler
    }
    return true
}()
