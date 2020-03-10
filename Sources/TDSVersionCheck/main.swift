import Foundation
import TDS
import NIO
import NIOSSL

let createTestDB = """
IF NOT EXISTS (
    SELECT [name]
        FROM sys.databases
        WHERE [name] = N'swiftTDSTest'
)
CREATE DATABASE swiftTDSTest
"""

let dropTestTable = """
IF OBJECT_ID('[swiftTDSTest].[dbo].[userList]', 'U') IS NOT NULL
DROP TABLE [swiftTDSTest].[dbo].[userList]
"""

let createTestTable = """
CREATE TABLE [swiftTDSTest].[dbo].[userList]
(
    [id] NVARCHAR(50) NOT NULL PRIMARY KEY, -- Primary Key column
    [name] NVARCHAR(50) NOT NULL
);
"""

let seedTestTable = """
INSERT INTO [swiftTDSTest].[dbo].[userList]
(
 [Id], [name]
)
VALUES
(
 '1', 'John Doe'
),
(
 '2', 'Jane Doe'
)
"""

let queryTestTable = "SELECT * FROM [swiftTDSTest].[dbo].[userList]"

func testPrelogin() throws {
    let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    defer { try! elg.syncShutdownGracefully() }
    let hostname = "localhost"

    // Use local certificate for TLS
    let currentPathComponents = #file.split(separator: "/")
    let directory = currentPathComponents.dropLast(1).map(String.init).joined(separator: "/")
    let relativePathToCert = "/\(directory)/../../scripts/certificate.pem"

    // Establish connnection
    let conn = try TDSConnection.connect(
        to: SocketAddress.makeAddressResolvingHost(hostname, port: 1433),
        tlsConfiguration: .forClient(trustRoots: .file(relativePathToCert)),
        serverHostname: hostname,
        on: elg.next()
    ).wait()

    // Prelogin
    try conn.prelogin().wait()

    // Login
    try conn.login(username: "SA", password: "<YourStrong@Passw0rd>").wait()

    // SQL Execution
    _ = try conn.rawSql(createTestDB).wait()
    _ = try conn.rawSql(dropTestTable).wait()
    _ = try conn.rawSql(createTestTable).wait()
    _ = try conn.rawSql(seedTestTable).wait()

    // Fetch records and view results
    let results = try conn.rawSql(queryTestTable).wait()
    print(results)

    try conn.close().wait()
}

do {
    try testPrelogin()
} catch let error {
    print(error)
}
