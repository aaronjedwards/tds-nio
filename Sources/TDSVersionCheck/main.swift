import Foundation
import TDS
import NIO
import NIOSSL

func testRemoteServer() throws {
    let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    defer { try! elg.syncShutdownGracefully() }
    let hostname = "localhost"
    let conn = try TDSConnection.connect(
        to: SocketAddress.makeAddressResolvingHost(hostname, port: 1433),
        tlsConfiguration: .forClient(trustRoots: .file("/Users/aaronjedwards/Desktop/certificate.pem")),
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
