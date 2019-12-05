import Foundation
import TDS
import NIO

func testRemoteServer() throws {
    let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    defer { try! elg.syncShutdownGracefully() }
    let hostname = "ajedwards.database.windows.net"
    let conn = try TDSConnection.connect(
        to: SocketAddress.makeAddressResolvingHost(hostname, port: 1433),
        tlsConfiguration: .forClient(),
        serverHostname: hostname,
        on: elg.next()
    ).wait()
    try conn.prelogin().wait()
    try conn.close().wait()
}

do {
    try testRemoteServer()
} catch let error {
    print(error)
}
