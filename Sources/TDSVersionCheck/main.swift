import Foundation
import TDS
import NIO

func testRemoteServer() throws {
    let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    defer { try! elg.syncShutdownGracefully() }
    
    let conn = try TDSConnection.connect(
        to: SocketAddress.makeAddressResolvingHost("carbondb.twc.systems", port: 1433),
        serverHostname: "carbondb.twc.systems",
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
