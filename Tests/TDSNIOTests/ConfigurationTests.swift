import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOSSL
import NIOTestUtils
import Testing

@testable import TDSNIO

extension TDSTests {
    @Test func configurationCanApplyRoutingRedirect() throws {
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

        expectEqual(redirected.host, "redirect.sql.example.test")
        expectEqual(redirected.port, 1444)
        expectEqual(redirected.username, configuration.username)
        expectEqual(redirected.password, configuration.password)
        expectEqual(redirected.database, configuration.database)
        expectEqual(redirected.options.routingRedirectLimit, 2)
        expectEqual(redirected.serverNameForTLS, "redirect.sql.example.test")
    }

    @Test func configurationRoutingRedirectIgnoresInstanceName() throws {
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

        expectEqual(redirected.host, "redirect.sql.example.test")
        expectEqual(redirected.port, 1444)
        expectEqual(redirected.serverNameForTLS, "redirect.sql.example.test")
    }

    @Test func configurationRetriesOnlyTransientLoginErrors() throws {
        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!"
        )

        expectTrue(
            configuration.shouldRetryConnection(
                after: TDSSQLError.server(Self.infoError(number: 40613)),
                remainingRetries: 1
            )
        )
        expectFalse(
            configuration.shouldRetryConnection(
                after: TDSSQLError.server(Self.infoError(number: 18456)),
                remainingRetries: 1
            )
        )
        expectFalse(
            configuration.shouldRetryConnection(
                after: TDSSQLError.connectionError(underlying: ChannelError.ioOnClosedChannel),
                remainingRetries: 1
            )
        )
        expectFalse(
            configuration.shouldRetryConnection(
                after: TDSSQLError.server(Self.infoError(number: 40613)),
                remainingRetries: 0
            )
        )
    }

    @Test func initialSessionSettingsGenerateDriverDefaultSQL() throws {
        let sql = TDSConnection.Configuration.Options.InitialSessionSettings.driverDefaults.sqlBatch

        expectEqual(
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

    @Test func initialSQLOverridesInitialSessionSettings() throws {
        var configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "sa",
            password: "Secret123!"
        )
        configuration.options.initialSQL = "set language British"
        configuration.options.initialSessionSettings = .driverDefaults

        expectEqual(configuration.options.startupInitialSQL, "set language British")
    }

    @Test func configurationParsesSQLServerConnectionString() throws {
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

        expectEqual(configuration.host, "sql.example.test")
        expectEqual(configuration.port, 1444)
        expectEqual(configuration.database, "appdb")
        expectEqual(configuration.username, "sa")
        expectEqual(configuration.password, "Secret;123!")
        expectEqual(configuration.applicationName, "App")
        expectEqual(configuration.language, "us_english")
        expectEqual(configuration.packetSize, TDSPacket.maximumNegotiatedPacketLength)
        expectFalse(configuration.tls.isSupported)
        expectEqual(configuration.applicationIntent, .readOnly)
        expectEqual(configuration.authentication, .sqlServer)
    }

    @Test func configurationParsesIntegratedSecurityConnectionString() throws {
        let configuration = try TDSConnection.Configuration(
            connectionString: """
                Data Source=sql.example.test;\
                Initial Catalog=appdb;\
                Integrated Security=SSPI
                """)

        expectEqual(configuration.host, "sql.example.test")
        expectEqual(configuration.port, 1433)
        expectEqual(configuration.database, "appdb")
        expectEqual(configuration.username, "")
        expectEqual(configuration.password, "")
        expectEqual(configuration.authentication, .sspi(initialToken: []))
    }

    @Test func configurationRejectsInvalidConnectionStringValues() throws {
        expectThrowsError(try TDSConnection.Configuration(connectionString: "Database=appdb")) { error in
            expectEqual(error as? TDSConnectionStringError, .missingServer)
        }
        expectThrowsError(try TDSConnection.Configuration(connectionString: "Server=sql.example.test;User ID=sa")) {
            error in
            expectEqual(error as? TDSConnectionStringError, .missingPassword)
        }
        expectThrowsError(
            try TDSConnection.Configuration(connectionString: "Server=sql.example.test,nope;Integrated Security=true")
        ) { error in
            expectEqual(error as? TDSConnectionStringError, .invalidPort("nope"))
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
