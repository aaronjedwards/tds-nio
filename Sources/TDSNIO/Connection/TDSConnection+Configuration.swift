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

public import Foundation
public import NIOCore
import NIOPosix
public import NIOSSL

#if DistributedTracingSupport
    public import Tracing
#endif

public enum TDSConnectionStringError: Error, Sendable, Equatable {
    case missingServer
    case missingUsername
    case missingPassword
    case invalidPort(String)
    case invalidPacketSize(String)
    case invalidEncryptValue(String)
    case invalidApplicationIntent(String)
}

extension TDSConnection {
    /// A configuration object for a connection.
    public struct Configuration: Sendable {
        /// The possible modes of operation for TLS encapsulation of a connection.
        public struct TLS: Sendable {

            /// Do not try to create a TLS connection to the server.
            public static var disable: Self { .init(base: .disable) }

            /// Try to create a TLS connection to the server. If the server supports TLS, create a TLS connection.
            /// If the server does not support TLS, create an insecure connection.
            public static func prefer(_ sslContext: NIOSSLContext) -> Self {
                self.init(base: .prefer(sslContext))
            }

            /// Try to create a TLS connection to the server.
            ///
            /// If the server supports TLS, create a TLS connection. If the server does not support TLS,
            /// fail the connection creation.
            public static func require(_ sslContext: NIOSSLContext) -> Self {
                self.init(base: .require(sslContext))
            }

            // MARK: Accessors

            /// Whether TLS will be attempted on the connection (`false` only when mode is ``disable``).
            public var isSupported: Bool {
                if case .disable = self.base { return false } else { return true }
            }

            /// Whether TLS will be required on the connection (`true` only when mode is ``require(_:)``).
            public var isRequired: Bool {
                if case .require(_) = self.base { return true } else { return false }
            }

            /// The `NIOSSLContext` that will be used. `nil` when TLS is disabled.
            public var sslContext: NIOSSLContext? {
                switch self.base {
                case .disable: return nil
                case .prefer(let context), .require(let context): return context
                }
            }

            var preloginEncryption: TDSFrontendMessageEncoder.PreloginEncryption {
                switch self.base {
                case .disable:
                    return .encryptNotSup
                case .prefer:
                    return .encryptOn
                case .require:
                    return .encryptReq
                }
            }

            func isCompatible(
                with serverEncryption: TDSFrontendMessageEncoder.PreloginEncryption?
            ) -> Bool {
                switch (self.base, serverEncryption) {
                case (.disable, .encryptReq), (.disable, .encryptOn),
                    (.disable, .encryptClientCertOn), (.disable, .encryptClientCertReq):
                    return false
                case (.require, .encryptNotSup), (.require, nil):
                    return false
                default:
                    return true
                }
            }

            enum Base {
                case disable
                case prefer(NIOSSLContext)
                case require(NIOSSLContext)
            }
            let base: Base
            private init(base: Base) { self.base = base }
        }

        public struct Options: Sendable {
            /// Session defaults that can be applied with a post-login SQL batch.
            public struct InitialSessionSettings: Sendable, Hashable {
                public var ansiNulls: Bool?
                public var ansiNullDefault: Bool?
                public var ansiPadding: Bool?
                public var ansiWarnings: Bool?
                public var arithAbort: Bool?
                public var concatNullYieldsNull: Bool?
                public var cursorCloseOnCommit: Bool?
                public var dateFirst: Int?
                public var dateFormat: String?
                public var implicitTransactions: Bool?
                public var language: String?
                public var numericRoundAbort: Bool?
                public var quotedIdentifier: Bool?
                public var textSize: Int?
                public var isolationLevel: TDSTransactionManagerRequest.IsolationLevel?
                public var xactAbort: Bool?

                public init(
                    ansiNulls: Bool? = nil,
                    ansiNullDefault: Bool? = nil,
                    ansiPadding: Bool? = nil,
                    ansiWarnings: Bool? = nil,
                    arithAbort: Bool? = nil,
                    concatNullYieldsNull: Bool? = nil,
                    cursorCloseOnCommit: Bool? = nil,
                    dateFirst: Int? = nil,
                    dateFormat: String? = nil,
                    implicitTransactions: Bool? = nil,
                    language: String? = nil,
                    numericRoundAbort: Bool? = nil,
                    quotedIdentifier: Bool? = nil,
                    textSize: Int? = nil,
                    isolationLevel: TDSTransactionManagerRequest.IsolationLevel? = nil,
                    xactAbort: Bool? = nil
                ) {
                    self.ansiNulls = ansiNulls
                    self.ansiNullDefault = ansiNullDefault
                    self.ansiPadding = ansiPadding
                    self.ansiWarnings = ansiWarnings
                    self.arithAbort = arithAbort
                    self.concatNullYieldsNull = concatNullYieldsNull
                    self.cursorCloseOnCommit = cursorCloseOnCommit
                    self.dateFirst = dateFirst
                    self.dateFormat = dateFormat
                    self.implicitTransactions = implicitTransactions
                    self.language = language
                    self.numericRoundAbort = numericRoundAbort
                    self.quotedIdentifier = quotedIdentifier
                    self.textSize = textSize
                    self.isolationLevel = isolationLevel
                    self.xactAbort = xactAbort
                }

                /// Common session defaults emitted by the driver after login.
                public static var driverDefaults: Self {
                    .init(
                        ansiNulls: true,
                        ansiNullDefault: true,
                        ansiPadding: true,
                        ansiWarnings: true,
                        arithAbort: true,
                        concatNullYieldsNull: true,
                        cursorCloseOnCommit: nil,
                        dateFirst: 7,
                        dateFormat: "mdy",
                        implicitTransactions: false,
                        language: "us_english",
                        numericRoundAbort: false,
                        quotedIdentifier: true,
                        textSize: 2_147_483_647,
                        isolationLevel: .readCommitted,
                        xactAbort: false
                    )
                }

                var sqlBatch: String {
                    var options: [String] = []
                    Self.appendSet("ansi_nulls", self.ansiNulls, to: &options)
                    Self.appendSet("ansi_null_dflt_on", self.ansiNullDefault, to: &options)
                    Self.appendSet("ansi_padding", self.ansiPadding, to: &options)
                    Self.appendSet("ansi_warnings", self.ansiWarnings, to: &options)
                    Self.appendSet("arithabort", self.arithAbort, to: &options)
                    Self.appendSet("concat_null_yields_null", self.concatNullYieldsNull, to: &options)
                    Self.appendSet("cursor_close_on_commit", self.cursorCloseOnCommit, to: &options)
                    if let dateFirst {
                        options.append("set datefirst \(dateFirst)")
                    }
                    if let dateFormat {
                        options.append("set dateformat \(dateFormat)")
                    }
                    Self.appendSet("implicit_transactions", self.implicitTransactions, to: &options)
                    if let language {
                        options.append("set language \(language)")
                    }
                    Self.appendSet("numeric_roundabort", self.numericRoundAbort, to: &options)
                    Self.appendSet("quoted_identifier", self.quotedIdentifier, to: &options)
                    if let textSize {
                        options.append("set textsize \(textSize)")
                    }
                    if let isolationLevel, isolationLevel != .current {
                        options.append(
                            "set transaction isolation level \(Self.sqlText(for: isolationLevel))")
                    }
                    Self.appendSet("xact_abort", self.xactAbort, to: &options)
                    return options.joined(separator: "\n")
                }

                private static func appendSet(
                    _ option: String,
                    _ value: Bool?,
                    to options: inout [String]
                ) {
                    guard let value else {
                        return
                    }
                    options.append("set \(option) \(value ? "on" : "off")")
                }

                private static func sqlText(
                    for isolationLevel: TDSTransactionManagerRequest.IsolationLevel
                ) -> String {
                    switch isolationLevel {
                    case .current, .readCommitted:
                        return "read committed"
                    case .readUncommitted:
                        return "read uncommitted"
                    case .repeatableRead:
                        return "repeatable read"
                    case .serializable:
                        return "serializable"
                    case .snapshot:
                        return "snapshot"
                    }
                }
            }

            /// A timeout for connection attempts. Defaults to ten seconds.
            public var connectTimeout: TimeAmount

            /// The server name to use for certificate validation and SNI (Server Name Indication) when
            /// TLS is enabled.
            ///
            /// Defaults to none (but see below).
            ///
            /// > When set to `nil`:
            /// If the connection is made to a server over TCP using
            /// ``TDSConnection/Configuration/init(host:port:username:password:database:language:tls:packetSize:applicationName:clientHostName:clientID:protocolVersion:applicationIntent:authentication:)``,
            /// the given `host` is used, unless it was an IP address string. If it _was_ an IP, or the
            /// connection is made by any other method, SNI is disabled.
            public var tlsServerName: String?

            /// Maximum number of server routing redirects to follow while establishing a connection.
            public var routingRedirectLimit: Int

            /// Called when SQL Server sends an informational INFO token.
            ///
            /// The callback is invoked on the connection's event loop. Informational messages do not
            /// fail the active request.
            public var infoMessageHandler: (@Sendable (TDSInfoMessage) -> Void)?

            /// Called when SQL Server sends an ERROR token.
            ///
            /// The callback is invoked on the connection's event loop. Error messages also fail the
            /// active request through ``TDSSQLError``.
            public var errorMessageHandler: (@Sendable (TDSErrorMessage) -> Void)?

            /// Called when SQL Server sends an ENVCHANGE token.
            ///
            /// The callback is invoked on the connection's event loop after the driver has applied any
            /// internal state changes such as routing redirects and transaction descriptors.
            public var envChangeHandler: (@Sendable (TDSEnvChangeMessage) -> Void)?

            /// Optional SQL batch sent immediately after login completes and before the connection is
            /// reported as ready.
            ///
            /// This can be used to establish session defaults with a post-login `SET` batch.
            public var initialSQL: String?

            /// Typed session defaults sent as a post-login `SET` batch when ``initialSQL`` is unset.
            public var initialSessionSettings: InitialSessionSettings?

            /// Called when SQL Server sends a SESSIONSTATE token.
            ///
            /// The callback is invoked on the connection's event loop. Session-state messages do not
            /// fail the active request.
            public var sessionStateHandler: (@Sendable (TDSSessionStateMessage) -> Void)?

            /// Create an options structure with default values.
            ///
            /// Most users should not need to adjust the defaults.
            public init() {
                self.connectTimeout = .seconds(10)
                self.routingRedirectLimit = 1
                self.infoMessageHandler = nil
                self.errorMessageHandler = nil
                self.envChangeHandler = nil
                self.initialSQL = nil
                self.initialSessionSettings = .init(textSize: 2_147_483_647)
                self.sessionStateHandler = nil
            }

            var startupInitialSQL: String? {
                if let initialSQL, initialSQL.contains(where: { !$0.isWhitespace }) {
                    return initialSQL
                }
                guard let settingsSQL = self.initialSessionSettings?.sqlBatch,
                    settingsSQL.contains(where: { !$0.isWhitespace })
                else {
                    return nil
                }
                return settingsSQL
            }
        }

        public var options: Options = .init()

        /// Application intent sent in LOGIN7.
        public enum ApplicationIntent: Sendable, Hashable {
            /// Connect with the default read/write intent.
            case readWrite
            /// Connect with read-only routing intent.
            case readOnly
        }

        /// The authentication mode advertised in LOGIN7.
        public enum Authentication: Sendable, Hashable {
            /// SQL Server username/password authentication.
            case sqlServer
            /// Integrated authentication using an optional initial SSPI/SPNEGO token.
            case sspi(initialToken: [UInt8] = [])
            /// Federated authentication using the ADAL workflow advertised in LOGIN7.
            case federatedAuthentication(workflow: FederatedAuthenticationWorkflow = .default)
            /// Federated authentication using an access token included in LOGIN7.
            case federatedAuthenticationToken(String)
        }

        /// Federated authentication workflow advertised in LOGIN7.
        public enum FederatedAuthenticationWorkflow: UInt8, Sendable, Hashable {
            /// User/password style ADAL workflow.
            case `default` = 0x01
            /// Integrated ADAL workflow.
            case integrated = 0x02
        }

        /// The name or IP address of the machine hosting the database or the database listener.
        public var host: String
        /// The port number on which the database listener is listening.
        public var port: Int
        /// The user name used in LOGIN7 authentication.
        public var username: String
        /// The password used in LOGIN7 authentication.
        public var password: String
        /// The optional initial database.
        public var database: String?
        /// The optional initial language requested during LOGIN7.
        public var language: String?
        /// Packet size, in bytes, requested in LOGIN7 and used for initial outbound splitting.
        ///
        /// SQL Server packet sizes are bounded to the TDS range of 512...32767 bytes.
        public var packetSize: Int {
            didSet {
                self.packetSize = TDSPacket.clampedPacketLength(self.packetSize)
            }
        }
        /// The application name reported to SQL Server.
        public var applicationName: String
        /// The client host name reported to SQL Server.
        public var clientHostName: String
        /// The client ID sent in LOGIN7. This is informational to SQL Server.
        public var clientID: [UInt8] {
            didSet {
                self.clientID = Self.normalizedClientID(self.clientID)
            }
        }
        /// Highest TDS version requested during LOGIN7 negotiation.
        public var protocolVersion: TDSProtocolVersion
        /// Application intent requested during LOGIN7 negotiation.
        public var applicationIntent: ApplicationIntent
        /// Authentication mode requested during LOGIN7 negotiation.
        public var authentication: Authentication

        #if DistributedTracingSupport
            /// The distributed tracing configuration to use for this connection.
            ///
            /// Defaults to using the globally bootstrapped tracer with OpenTelemetry semantic conventions.
            public var tracing: TDSTracingConfiguration = .init()
        #endif

        public var tls: TLS
        public var serverNameForTLS: String? {
            // If a name was explicitly configured always use it.
            if let tlsServerName = options.tlsServerName {
                return tlsServerName
            }

            // Otherwise, if the hostname wasn't an IP use that.
            if !host.isIPAddress() { return host }

            // Otherwise disable SNI
            return nil
        }

        /// The number of tries that a connection attempt
        /// should be retried before the attempt is terminated.
        ///
        /// Defaults to `0`.
        public var retryCount: Int = 0

        /// The number of seconds to wait before making a new connection attempt.
        ///
        /// Defaults to `0`.
        public var retryDelay: Int = 0

        public init(
            host: String,
            port: Int = 1433,
            username: String,
            password: String,
            database: String? = nil,
            language: String? = nil,
            tls: TLS = .disable,
            packetSize: Int = 4096,
            applicationName: String = "TDSNIO",
            clientHostName: String = ProcessInfo.processInfo.hostName,
            clientID: [UInt8] = [0, 0, 0, 0, 0, 0],
            protocolVersion: TDSProtocolVersion = .v7_4,
            applicationIntent: ApplicationIntent = .readWrite,
            authentication: Authentication = .sqlServer
        ) {
            self.host = host
            self.port = port
            self.username = username
            self.password = password
            self.database = database
            self.language = language
            self.packetSize = TDSPacket.clampedPacketLength(packetSize)
            self.tls = tls
            self.applicationName = applicationName
            self.clientHostName = clientHostName
            self.protocolVersion = protocolVersion
            self.applicationIntent = applicationIntent
            self.authentication = authentication
            self.clientID = Self.normalizedClientID(clientID)
        }

        public init(connectionString: String) throws {
            let values = Self.parseConnectionString(connectionString)

            guard
                let server = values.firstValue(for: [
                    "server", "data source", "addr", "address", "network address",
                ]), !server.isEmpty
            else {
                throw TDSConnectionStringError.missingServer
            }
            let endpoint = try Self.parseServerEndpoint(server)

            let integratedSecurity =
                values.firstValue(for: [
                    "integrated security", "trusted_connection", "trusted connection",
                ]).map(Self.isTruthy) ?? false

            let username = values.firstValue(for: ["user id", "uid", "user", "username"]) ?? ""
            let password = values.firstValue(for: ["password", "pwd"]) ?? ""
            if !integratedSecurity {
                guard !username.isEmpty else { throw TDSConnectionStringError.missingUsername }
                guard !password.isEmpty else { throw TDSConnectionStringError.missingPassword }
            }

            let database = values.firstValue(for: ["database", "initial catalog"])
            let language = values.firstValue(for: ["language", "current language"])
            let applicationName = values.firstValue(for: ["application name", "app"])
            let packetSize: Int
            if let packetSizeString = values.firstValue(for: ["packet size", "packet_size"]) {
                guard let parsed = Int(packetSizeString) else {
                    throw TDSConnectionStringError.invalidPacketSize(packetSizeString)
                }
                packetSize = parsed
            } else {
                packetSize = TDSPacket.defaultPacketLength
            }

            let tls: TLS
            if let encrypt = values.firstValue(for: ["encrypt", "encryption"]) {
                switch encrypt.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
                case "false", "no", "optional", "disable", "disabled":
                    tls = .disable
                case "true", "yes", "mandatory", "required", "require":
                    tls = try .require(NIOSSLContext(configuration: .makeClientConfiguration()))
                case "strict":
                    tls = try .require(NIOSSLContext(configuration: .makeClientConfiguration()))
                default:
                    throw TDSConnectionStringError.invalidEncryptValue(encrypt)
                }
            } else {
                tls = .disable
            }

            let applicationIntent: ApplicationIntent
            if let intent = values.firstValue(for: ["application intent", "applicationintent"]) {
                switch intent.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
                case "readonly", "read only":
                    applicationIntent = .readOnly
                case "readwrite", "read write":
                    applicationIntent = .readWrite
                default:
                    throw TDSConnectionStringError.invalidApplicationIntent(intent)
                }
            } else {
                applicationIntent = .readWrite
            }

            self.init(
                host: endpoint.host,
                port: endpoint.port,
                username: username,
                password: password,
                database: database,
                language: language,
                tls: tls,
                packetSize: packetSize,
                applicationName: applicationName ?? "TDSNIO",
                applicationIntent: applicationIntent,
                authentication: integratedSecurity ? .sspi() : .sqlServer
            )
        }

        func redirected(to routing: TDSBackendMessage.EnvChange.Routing) throws -> Self {
            guard routing.protocolByte == 0 else {
                throw RoutingRedirectionError.unsupportedProtocol(routing.protocolByte)
            }
            guard !routing.server.isEmpty else {
                throw RoutingRedirectionError.emptyServer
            }

            var configuration = self
            configuration.host = String(routing.server.split(separator: "\\", maxSplits: 1)[0])
            configuration.port = Int(routing.port)
            return configuration
        }

        func shouldRetryConnection(after error: Error, remainingRetries: Int) -> Bool {
            guard remainingRetries > 0 else {
                return false
            }
            guard let sqlError = error as? TDSSQLError else {
                return false
            }
            return sqlError.isTransientLoginError
        }

        private static func normalizedClientID(_ clientID: [UInt8]) -> [UInt8] {
            if clientID.count == 6 {
                return clientID
            }
            if clientID.count > 6 {
                return Array(clientID.prefix(6))
            }
            return clientID + Array(repeating: 0, count: 6 - clientID.count)
        }

        var requestedFeatureExtAckFeatureIDs: Set<UInt8> {
            var ids: Set<UInt8> = [
                Capabilities.FeatureID.utf8Support.rawValue
            ]
            switch self.authentication {
            case .federatedAuthentication, .federatedAuthenticationToken:
                ids.insert(0x02)
            case .sqlServer, .sspi:
                break
            }
            return ids
        }

        private static func parseConnectionString(_ connectionString: String) -> [String: String] {
            var values: [String: String] = [:]
            for part in Self.splitConnectionString(connectionString) {
                guard let separator = part.firstIndex(of: "=") else {
                    continue
                }
                let key = part[..<separator]
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                    .lowercased()
                var value = part[part.index(after: separator)...]
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                if value.first == "{", value.last == "}" {
                    value.removeFirst()
                    value.removeLast()
                }
                values[key] = String(value)
            }
            return values
        }

        private static func splitConnectionString(_ connectionString: String) -> [String] {
            var parts: [String] = []
            var current = ""
            var inBraces = false
            for character in connectionString {
                switch character {
                case "{":
                    inBraces = true
                    current.append(character)
                case "}":
                    inBraces = false
                    current.append(character)
                case ";" where !inBraces:
                    if !current.isEmpty {
                        parts.append(current)
                    }
                    current.removeAll(keepingCapacity: true)
                default:
                    current.append(character)
                }
            }
            if !current.isEmpty {
                parts.append(current)
            }
            return parts
        }

        private static func parseServerEndpoint(_ server: String) throws -> (host: String, port: Int) {
            var server = server.trimmingCharacters(in: .whitespacesAndNewlines)
            if server.lowercased().hasPrefix("tcp:") {
                server.removeFirst(4)
            }
            if let comma = server.lastIndex(of: ",") {
                let host = server[..<comma]
                let portString = server[server.index(after: comma)...]
                guard let port = Int(portString) else {
                    throw TDSConnectionStringError.invalidPort(String(portString))
                }
                return (String(host), port)
            }
            return (server, 1433)
        }

        private static func isTruthy(_ value: String) -> Bool {
            switch value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
            case "true", "yes", "sspi":
                return true
            default:
                return false
            }
        }

    }
}

#if DistributedTracingSupport
    /// A configuration object that defines distributed tracing behavior of a TDS connection.
    public struct TDSTracingConfiguration: Sendable {
        /// The tracer to use, or `nil` to disable tracing.
        ///
        /// Defaults to the globally bootstrapped tracer.
        public var tracer: (any Tracer)? = InstrumentationSystem.tracer

        /// The attribute names used in spans created by TDSNIO.
        public var attributeNames: AttributeNames = .init()

        /// The static attribute values used in spans created by TDSNIO.
        public var attributeValues: AttributeValues = .init()

        /// Attribute names used in spans created by TDSNIO.
        public struct AttributeNames: Sendable {
            public var databaseNamespace: String = "db.namespace"
            public var databaseResponseStatusCode: String = "db.response.status_code"
            public var errorType: String = "error.type"
            public var serverPort: String = "server.port"
            public var databaseOperationName: String = "db.operation.name"
            public var databaseQuerySummary: String = "db.query.summary"
            public var databaseQueryText: String = "db.query.text"
            public var databaseSystem: String = "db.system"
            public var serverAddress: String = "server.address"
            public var databaseResponseReturnedRows: String = "db.response.returned_rows"
        }

        /// Static attribute values used in spans created by TDSNIO.
        public struct AttributeValues: Sendable {
            public var databaseSystem: String = "mssql"
        }
    }
#endif

extension Dictionary where Key == String, Value == String {
    fileprivate func firstValue(for keys: [String]) -> String? {
        for key in keys {
            if let value = self[key] {
                return value
            }
        }
        return nil
    }
}

private enum RoutingRedirectionError: Error {
    case unsupportedProtocol(UInt8)
    case emptyServer
}

// originally taken from NIOSSL
extension String {
    fileprivate func isIPAddress() -> Bool {
        // We need some scratch space to let inet_pton write into.
        var ipv4Addr = in_addr()
        var ipv6Addr = in6_addr()
        // inet_pton() assumes the provided address buffer is non-NULL

        /// N.B.: ``String/withCString(_:)`` is much more efficient than directly passing
        /// `self`, especially twice.
        return self.withCString { ptr in
            inet_pton(AF_INET, ptr, &ipv4Addr) == 1 || inet_pton(AF_INET6, ptr, &ipv6Addr) == 1
        }
    }
}
