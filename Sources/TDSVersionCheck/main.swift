import Foundation
import TDS
import NIO
import NIOSSL

let queryTestTable = "SELECT * FROM [dbo].[applicationUsers]"

func testPrelogin() throws {
    let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    defer { try! elg.syncShutdownGracefully() }
    let hostname = "swift-tds.database.windows.net"

    // Establish connnection
    let conn = try TDSConnection.connect(
        to: SocketAddress.makeAddressResolvingHost(hostname, port: 1433),
        tlsConfiguration: .forClient(),
        serverHostname: hostname,
        on: elg.next()
    ).wait()

    // Prelogin
    try conn.prelogin().wait()

    // Login
    try conn.login(hostname: hostname, username: "swifttds", password: "CQwEdRn$xh9BVprGUy)", serverName: "swift-tds", database: "swift-tds").wait()

    // Fetch records and view results
    let results = try conn.rawSql(queryTestTable).wait()
    print(results.count)

    try conn.close().wait()
}

do {
    try testPrelogin()
} catch let error {
    print(error)
}

