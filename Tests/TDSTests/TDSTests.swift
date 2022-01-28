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
        print(rows)
        XCTAssertEqual(rows.count, 1)
        
        let version = rows[0].column("version")?.string
        let regex = try NSRegularExpression(pattern: sqlServerVersionPattern)
        XCTAssertEqual(regex.matches(version), true)
    }
    
    func testRawSqlGetDate() throws {
        let conn = try TDSConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.rawSql("SELECT GETUTCDATE() AS timestamp").wait()
        XCTAssertEqual(rows.count, 1)
        
        let date = rows[0].column("timestamp")?.date
        XCTAssertEqual(date != nil, true)
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


final class TDSProcTests: XCTestCase {
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
    
    func testRawMockData() throws {
        let conn = try TDSConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.rawSql("SELECT TOP(60) * FROM MOCK_DATA").wait()
        print(rows.count)
    }
    
    func testTop10Mock() throws {
        let conn = try TDSConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.rpc("getTop10MockData", nil, nil).wait()
        XCTAssertEqual(rows.count, 10)
    }
    
    func testTopAllMock() throws {
        let conn = try TDSConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.rpc("getAllMockData", nil, nil).wait()
        XCTAssertEqual(rows.count, 1000)
    }
    
//    func testStoredProcParams() throws {
//
//        let conn = try TDSConnection.test(on: eventLoop).wait()
//        defer { try! conn.close().wait() }
//        let input = RPCInputParameter(name: "inputVal", data: RPCParamData(data: "hello world", dataType: .varchar))
//        let output = RPCOutputParameter(name: "returnValue")
//        let rows = try conn.rpc("inOutProc", [input], [output])
//        print(rows)
//
//    }
    
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
