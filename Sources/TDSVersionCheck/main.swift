import Foundation
import TDS
import NIO
import NIOSSL

func testPrelogin() throws {
    let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    defer { try! elg.syncShutdownGracefully() }
    let hostname = "localhost"

    let currentPathComponents = #file.split(separator: "/")
    let directory = currentPathComponents.dropLast(1).map(String.init).joined(separator: "/")
    let relativePathToCert = "/\(directory)/../../scripts/certificate.pem"

    let conn = try TDSConnection.connect(
        to: SocketAddress.makeAddressResolvingHost(hostname, port: 1433),
        tlsConfiguration: .forClient(trustRoots: .file(relativePathToCert)),
        serverHostname: hostname,
        on: elg.next()
    ).wait()
    try conn.prelogin().wait()
    try conn.login(username: "sa", password: "<YourStrong@Passw0rd>").wait()
    try conn.close().wait()
}

do {
    try testPrelogin()
} catch let error {
    print(error)
}
