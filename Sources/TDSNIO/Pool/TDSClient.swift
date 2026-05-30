import Logging
import NIOConcurrencyHelpers
import NIOCore
import ServiceLifecycle
import _ConnectionPoolModule

/// A TDS client backed by an underlying connection pool.
///
/// Create a client from a ``TDSConnection/Configuration`` and run ``run()`` in a
/// long-running task before leasing connections with ``withConnection(_:)``.
public final class TDSClient: Sendable, Service {
    /// Describes general client behavior options.
    public struct Options: Sendable {
        /// Keep-alive behavior for idle SQL Server connections.
        public struct KeepAliveBehavior: Sendable {
            /// The amount of time that shall pass before an idle connection runs a keep-alive ping.
            public var frequency: Duration

            public init(frequency: Duration = .seconds(30)) {
                self.frequency = frequency
            }
        }

        /// The minimum number of connections to keep open at any time.
        public var minimumConnections: Int = 0

        /// The maximum number of connections that may be opened at any time.
        public var maximumConnections: Int = 20

        /// The maximum time a connection outside ``minimumConnections`` is kept open while idle.
        public var connectionIdleTimeout: Duration = .seconds(60)

        /// The behavior used to keep idle TCP connections active.
        public var keepAliveBehavior: KeepAliveBehavior? = KeepAliveBehavior()

        public init() {}
    }

    typealias Pool = ConnectionPool<
        TDSConnection,
        TDSConnection.ID,
        ConnectionIDGenerator,
        ConnectionRequest<TDSConnection>,
        ConnectionRequest<TDSConnection>.ID,
        TDSKeepAliveBehavior,
        TDSClientMetrics,
        ContinuousClock
    >

    let pool: Pool
    let factory: TDSConnectionFactory
    let runningBox = NIOLockedValueBox(false)
    let backgroundLogger: Logger

    public convenience init(
        configuration: TDSConnection.Configuration,
        options: Options = .init(),
        eventLoopGroup: any EventLoopGroup = TDSClient.defaultEventLoopGroup
    ) {
        self.init(
            configuration: configuration,
            options: options,
            eventLoopGroup: eventLoopGroup,
            backgroundLogger: TDSConnection.noopLogger
        )
    }

    public init(
        configuration: TDSConnection.Configuration,
        options: Options = .init(),
        eventLoopGroup: any EventLoopGroup = TDSClient.defaultEventLoopGroup,
        backgroundLogger: Logger
    ) {
        let factory = TDSConnectionFactory(
            configuration: configuration,
            eventLoopGroup: eventLoopGroup,
            logger: backgroundLogger
        )
        self.factory = factory
        self.backgroundLogger = backgroundLogger
        self.pool = ConnectionPool(
            configuration: .init(options),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<TDSConnection>.self,
            keepAliveBehavior: .init(options.keepAliveBehavior),
            observabilityDelegate: .init(logger: backgroundLogger),
            clock: ContinuousClock(),
            connectionFactory: { connectionID, _ in
                let connection = try await factory.makeConnection(connectionID)
                return ConnectionAndMetadata(connection: connection, maximalStreamsOnConnection: 1)
            }
        )
    }

    /// Lease a connection for the provided closure's lifetime.
    public func withConnection<Result>(
        _ closure: (TDSConnection) async throws -> Result
    ) async throws -> Result {
        let connection = try await self.leaseConnection()
        defer {
            connection.markForResetOnNextRequest()
            self.pool.releaseConnection(connection)
        }
        return try await closure(connection)
    }

    /// Starts the client's background pool management task.
    public func run() async {
        let alreadyRunning = self.runningBox.withLockedValue { running in
            defer { running = true }
            return running
        }
        precondition(!alreadyRunning, "TDSClient.run() should only be called once.")

        await cancelWhenGracefulShutdown {
            await self.pool.run()
        }
    }

    private func leaseConnection() async throws -> TDSConnection {
        if !self.runningBox.withLockedValue({ $0 }) {
            self.backgroundLogger.warning(
                "Trying to lease connection from `TDSClient`, but `TDSClient.run()` hasn't been called yet."
            )
        }
        return try await self.pool.leaseConnection()
    }

    /// Returns the default `EventLoopGroup` singleton, automatically selecting the best for the platform.
    public static var defaultEventLoopGroup: EventLoopGroup {
        TDSConnection.defaultEventLoopGroup
    }
}

struct TDSKeepAliveBehavior: ConnectionKeepAliveBehavior {
    let behavior: TDSClient.Options.KeepAliveBehavior?

    init(_ behavior: TDSClient.Options.KeepAliveBehavior?) {
        self.behavior = behavior
    }

    var keepAliveFrequency: Duration? {
        self.behavior?.frequency
    }

    func runKeepAlive(for connection: TDSConnection) async throws {
        try await connection.ping()
    }
}

extension ConnectionPoolConfiguration {
    init(_ options: TDSClient.Options) {
        self = ConnectionPoolConfiguration()
        self.minimumConnectionCount = options.minimumConnections
        self.maximumConnectionSoftLimit = options.maximumConnections
        self.maximumConnectionHardLimit = options.maximumConnections
        self.idleTimeout = options.connectionIdleTimeout
    }
}

extension TDSConnection: PooledConnection {
    public func onClose(_ closure: @escaping @Sendable ((Error)?) -> Void) {
        self.closeFuture.whenComplete { result in
            switch result {
            case .success:
                closure(nil)
            case .failure(let error):
                closure(error)
            }
        }
    }

    public func close() {
        self.channel.close(mode: .all, promise: nil)
    }
}
