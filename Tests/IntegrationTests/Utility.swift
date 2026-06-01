//===----------------------------------------------------------------------===//
//
// This source file is part of the TDSNIO open source project
//
// Copyright (c) 2026 Aaron Edwards and the TDSNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
// See CONTRIBUTORS.md for the list of TDSNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//
import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOSSL
import TDSNIO

extension TDSConnection {
    static func testConfig() throws -> TDSConnection.Configuration {
        let tls: TDSConnection.Configuration.TLS
        switch env("TDS_TLS")?.lowercased() {
        case nil, "disable", "disabled", "false", "no":
            tls = .disable
        case "prefer", "preferred", "true", "yes":
            var configuration = TLSConfiguration.makeClientConfiguration()
            configuration.certificateVerification = .none
            tls = try .prefer(NIOSSLContext(configuration: configuration))
        case "require", "required":
            var configuration = TLSConfiguration.makeClientConfiguration()
            configuration.certificateVerification = .none
            tls = try .require(NIOSSLContext(configuration: configuration))
        case let value?:
            throw TestConfigurationError.invalidTLSMode(value)
        }

        var configuration = TDSConnection.Configuration(
            host: env("TDS_HOSTNAME") ?? "127.0.0.1",
            port: env("TDS_PORT").flatMap(Int.init) ?? 11433,
            username: env("TDS_USERNAME") ?? "sa",
            password: env("TDS_PASSWORD") ?? "TDSNio_Strong_Password_123",
            database: env("TDS_DATABASE"),
            tls: tls
        )
        configuration.options.connectTimeout = .seconds(env("TDS_CONNECT_TIMEOUT").flatMap(Int64.init) ?? 10)
        return configuration
    }

    static func test(
        on eventLoop: EventLoop,
        config: TDSConnection.Configuration? = nil,
        logLevel: Logger.Level = Logger.getLogLevel()
    ) async throws -> TDSConnection {
        var logger = Logger(label: "tds.connection.test")
        logger.logLevel = logLevel

        return try await TDSConnection.connect(
            on: eventLoop,
            configuration: config ?? self.testConfig(),
            id: nextTDSConnectionID(),
            logger: logger
        )
    }
}

extension Logger {
    static var tdsTest: Logger {
        var logger = Logger(label: "tds.test")
        logger.logLevel = self.getLogLevel()
        return logger
    }

    static func getLogLevel() -> Logger.Level {
        let ghActionsDebug = env("ACTIONS_STEP_DEBUG")
        if ghActionsDebug == "true" || ghActionsDebug == "TRUE" {
            return .trace
        }

        return env("LOG_LEVEL").flatMap {
            Logger.Level(rawValue: $0)
        } ?? .info
    }
}

enum TestConfigurationError: Error, CustomStringConvertible {
    case invalidTLSMode(String)

    var description: String {
        switch self {
        case .invalidTLSMode(let value):
            return "Invalid TDS_TLS value '\(value)'. Use disable, prefer, or require."
        }
    }
}

func env(_ name: String) -> String? {
    getenv(name).flatMap { String(cString: $0) }
}

private let connectionIDGenerator = NIOLockedValueBox<TDSConnection.ID>(0)

func nextTDSConnectionID() -> TDSConnection.ID {
    connectionIDGenerator.withLockedValue {
        $0 += 1
        return $0
    }
}

func withTDSConnection(
    on eventLoop: EventLoop,
    _ body: (TDSConnection) async throws -> Void
) async throws {
    let connection = try await TDSConnection.test(on: eventLoop)
    do {
        try await body(connection)
        try await connection.close()
    } catch {
        try? await connection.close()
        throw error
    }
}

func uniqueTableName(_ suffix: String) -> String {
    "tds_nio_\(suffix)_\(UUID().uuidString.replacingOccurrences(of: "-", with: "_"))"
}

func sqlIdentifier(_ name: String) -> String {
    "[\(name.replacingOccurrences(of: "]", with: "]]"))]"
}

func dropTableIfExists(_ tableName: String, on connection: TDSConnection) async throws {
    let identifier = sqlIdentifier(tableName)
    _ = try await connection.execute("DROP TABLE IF EXISTS \(unescaped: identifier)")
}
