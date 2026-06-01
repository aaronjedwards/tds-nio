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

    func testConfigurationRoutingRedirectIgnoresInstanceName() throws {
        let configuration = TDSConnection.Configuration(
            host: "original.sql.example.test",
            port: 1433,
            username: "sa",
            password: "Secret123!",
            database: "master"
        )

        let redirected = try configuration.redirected(
            to: .init(
                protocolByte: 0,
                port: 1444,
                server: #"redirect.sql.example.test\instanceNameA"#
            ))

        XCTAssertEqual(redirected.host, "redirect.sql.example.test")
        XCTAssertEqual(redirected.port, 1444)
        XCTAssertEqual(redirected.serverNameForTLS, "redirect.sql.example.test")
    }

    func testConfigurationRetriesOnlyTransientLoginErrors() throws {
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!"
        )

        XCTAssertTrue(
            configuration.shouldRetryConnection(
                after: TDSSQLError.server(Self.infoError(number: 40613)),
                remainingRetries: 1
            )
        )
        XCTAssertFalse(
            configuration.shouldRetryConnection(
                after: TDSSQLError.server(Self.infoError(number: 18456)),
                remainingRetries: 1
            )
        )
        XCTAssertFalse(
            configuration.shouldRetryConnection(
                after: TDSSQLError.connectionError(underlying: ChannelError.ioOnClosedChannel),
                remainingRetries: 1
            )
        )
        XCTAssertFalse(
            configuration.shouldRetryConnection(
                after: TDSSQLError.server(Self.infoError(number: 40613)),
                remainingRetries: 0
            )
        )
    }

    func testInitialSessionSettingsGenerateDriverDefaultSQL() throws {
        let sql = TDSConnection.Configuration.Options.InitialSessionSettings.driverDefaults.sqlBatch

        XCTAssertEqual(
            sql,
            """
            set ansi_nulls on
            set ansi_null_dflt_on on
            set ansi_padding on
            set ansi_warnings on
            set arithabort on
            set concat_null_yields_null on
            set datefirst 7
            set dateformat mdy
            set implicit_transactions off
            set language us_english
            set numeric_roundabort off
            set quoted_identifier on
            set textsize 2147483647
            set transaction isolation level read committed
            set xact_abort off
            """
        )
    }

    func testInitialSQLOverridesInitialSessionSettings() throws {
        var configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!"
        )
        configuration.options.initialSQL = "set language British"
        configuration.options.initialSessionSettings = .driverDefaults

        XCTAssertEqual(configuration.options.startupInitialSQL, "set language British")
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

    private static func infoError(number: Int32) -> TDSBackendMessage.InfoError {
        .init(
            number: number,
            state: 1,
            severity: 16,
            message: "Login failed",
            serverName: "",
            procedureName: "",
            lineNumber: 1
        )
    }
}
