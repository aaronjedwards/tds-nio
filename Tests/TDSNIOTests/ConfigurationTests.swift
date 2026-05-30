import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOSSL
import NIOTestUtils
import XCTest

@testable import TDSNIO

extension TDSTests {
    func testConfigurationCanApplyRoutingRedirect() throws {
        var configuration = TDSConnection.Configuration(
            host: "original.sql.example.test",
            port: 1433,
            username: "sa",
            password: "Secret123!",
            database: "master"
        )
        configuration.options.routingRedirectLimit = 2

        let redirected = try configuration.redirected(
            to: .init(
                protocolByte: 0,
                port: 1444,
                server: "redirect.sql.example.test"
            ))

        XCTAssertEqual(redirected.host, "redirect.sql.example.test")
        XCTAssertEqual(redirected.port, 1444)
        XCTAssertEqual(redirected.username, configuration.username)
        XCTAssertEqual(redirected.password, configuration.password)
        XCTAssertEqual(redirected.database, configuration.database)
        XCTAssertEqual(redirected.options.routingRedirectLimit, 2)
        XCTAssertEqual(redirected.serverNameForTLS, "redirect.sql.example.test")
    }

    func testConfigurationParsesSQLServerConnectionString() throws {
        let configuration = try TDSConnection.Configuration(
            connectionString: """
                Server=tcp:sql.example.test,1444;\
                Database=appdb;\
                User ID=sa;\
                Password={Secret;123!};\
                Application Name=App;\
                Language=us_english;\
                Packet Size=32768;\
                Encrypt=no;\
                Application Intent=ReadOnly
                """)

        XCTAssertEqual(configuration.host, "sql.example.test")
        XCTAssertEqual(configuration.port, 1444)
        XCTAssertEqual(configuration.database, "appdb")
        XCTAssertEqual(configuration.username, "sa")
        XCTAssertEqual(configuration.password, "Secret;123!")
        XCTAssertEqual(configuration.applicationName, "App")
        XCTAssertEqual(configuration.language, "us_english")
        XCTAssertEqual(configuration.packetSize, TDSPacket.maximumNegotiatedPacketLength)
        XCTAssertFalse(configuration.tls.isSupported)
        XCTAssertEqual(configuration.applicationIntent, .readOnly)
        XCTAssertEqual(configuration.authentication, .sqlServer)
    }

    func testConfigurationParsesIntegratedSecurityConnectionString() throws {
        let configuration = try TDSConnection.Configuration(
            connectionString: """
                Data Source=sql.example.test;\
                Initial Catalog=appdb;\
                Integrated Security=SSPI
                """)

        XCTAssertEqual(configuration.host, "sql.example.test")
        XCTAssertEqual(configuration.port, 1433)
        XCTAssertEqual(configuration.database, "appdb")
        XCTAssertEqual(configuration.username, "")
        XCTAssertEqual(configuration.password, "")
        XCTAssertEqual(configuration.authentication, .sspi(initialToken: []))
    }

    func testConfigurationRejectsInvalidConnectionStringValues() throws {
        XCTAssertThrowsError(try TDSConnection.Configuration(connectionString: "Database=appdb")) { error in
            XCTAssertEqual(error as? TDSConnectionStringError, .missingServer)
        }
        XCTAssertThrowsError(try TDSConnection.Configuration(connectionString: "Server=sql.example.test;User ID=sa")) {
            error in
            XCTAssertEqual(error as? TDSConnectionStringError, .missingPassword)
        }
        XCTAssertThrowsError(
            try TDSConnection.Configuration(connectionString: "Server=sql.example.test,nope;Integrated Security=true")
        ) { error in
            XCTAssertEqual(error as? TDSConnectionStringError, .invalidPort("nope"))
        }
    }
}
