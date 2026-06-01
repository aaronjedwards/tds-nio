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

@preconcurrency import Dispatch
public import Logging
import NIOConcurrencyHelpers
public import NIOCore
import NIOPosix
import NIOSSL

import class Foundation.ProcessInfo

#if DistributedTracingSupport
    import Tracing
#endif


#if canImport(Network)
    import NIOTransportServices
#endif

/// An TDS connection. Use it to run queries against an TDS server.
public final class TDSConnection: Sendable {
    /// A TDS connection ID, for logging
    public typealias ID = Int
    public let id: ID

    let configuration: Configuration
    let channel: Channel
    let logger: Logger
    private let resetConnectionOnNextRequestBox: NIOLockedValueBox<Bool>

    #if DistributedTracingSupport
        let tracer: (any Tracer)?

        var databaseNamespace: String {
            self.configuration.database ?? ""
        }
    #endif

    public var closeFuture: EventLoopFuture<Void> {
        channel.closeFuture
    }

    public var isClosed: Bool {
        !self.channel.isActive
    }

    /// The `EventLoop` that the connection and its channel run on.
    public var eventLoop: EventLoop { channel.eventLoop }

    /// The TDS protocol version for the server that is connected to.
    public let protocolVersion: TDSProtocolVersion

    static let noopLogger = Logger(label: "tds-nio.noop-logger") { _ in
        SwiftLogNoOpLogHandler()
    }

    init(
        configuration: TDSConnection.Configuration,
        channel: Channel,
        connectionID: ID,
        logger: Logger,
        protocolVersion: TDSProtocolVersion
    ) {
        self.id = connectionID
        self.configuration = configuration
        self.logger = logger
        self.channel = channel
        self.protocolVersion = protocolVersion
        self.resetConnectionOnNextRequestBox = NIOLockedValueBox(false)
        #if DistributedTracingSupport
            self.tracer = configuration.tracing.tracer
        #endif
    }

    deinit {
        assert(isClosed, "TDSConnection deinitialized before being closed.")
    }

    private static func start(
        configuration: Configuration,
        connectionID: TDSConnection.ID,
        channel: Channel,
        logger: Logger,
        remainingRoutingRedirects: Int
    ) -> EventLoopFuture<TDSConnection> {
        // 1. configure handlers

        let frontendMessageHandler = TDSFrontendMessagePostProcessor(packetLength: configuration.packetSize)
        // Main channel handler, responsible for organizing the channel pipeline (including TLS negotiation)
        let channelHandler = TDSChannelHandler(
            configuration: configuration,
            logger: logger
        )

        let eventHandler = TDSEventsHandler(logger: logger)

        // 2. add handlers

        do {
            #if DEBUG
                // This is very useful for analyzing protocol problems in the driver.
                let tracer = Logger(label: "tds-nio.network-tracing")
                try channel.pipeline.syncOperations
                    .addHandler(DebugLogHandler(connectionID: connectionID, logger: tracer))
            #endif
            try channel.pipeline.syncOperations.addHandler(eventHandler)
            try channel.pipeline.syncOperations
                .addHandler(channelHandler, position: .before(eventHandler))
            try channel.pipeline.syncOperations
                .addHandler(frontendMessageHandler, position: .before(channelHandler))
        } catch {
            return channel.eventLoop.makeFailedFuture(error)
        }

        // 3. wait for startup future to succeed.

        return eventHandler.startupDoneFuture
            .flatMapError { error in
                // in case of a startup error, the connection must be closed and
                // after that the originating error should be surfaced
                channel.closeFuture.flatMapThrowing { _ in
                    throw error
                }
            }
            .flatMap { context in
                if let routing = context.routing {
                    guard remainingRoutingRedirects > 0 else {
                        return channel.eventLoop.makeFailedFuture(
                            TDSSQLError.connectionError(
                                underlying: RoutingRedirectionError.tooManyRedirects
                            )
                        )
                    }

                    let redirectedConfiguration: Configuration
                    do {
                        redirectedConfiguration = try configuration.redirected(to: routing)
                    } catch {
                        return channel.eventLoop.makeFailedFuture(
                            TDSSQLError.connectionError(underlying: error)
                        )
                    }

                    let closePromise = channel.eventLoop.makePromise(of: Void.self)
                    channel.close(mode: .all, promise: closePromise)
                    return closePromise.futureResult.flatMap {
                        self.connect(
                            on: channel.eventLoop,
                            configuration: redirectedConfiguration,
                            id: connectionID,
                            logger: logger,
                            remainingRoutingRedirects: remainingRoutingRedirects - 1
                        )
                    }
                }

                return channel.eventLoop.makeSucceededFuture(
                    TDSConnection(
                        configuration: configuration,
                        channel: channel,
                        connectionID: connectionID,
                        logger: logger,
                        protocolVersion: context.version
                    ))
            }
    }

    /// Create a new connection to an TDS server.
    ///
    /// - Parameters:
    ///   - eventLoop: The `EventLoop` the connection shall be created on.
    ///   - configuration: A ``Configuration`` that shall be used for the connection.
    ///   - connectionID: An `Int` id, used for metadata logging.
    ///   - logger: A logger to log background events into.
    /// - Returns: A SwiftNIO `EventLoopFuture` that will provide a ``TDSConnection``
    ///            at a later point in time.
    private static func connect(
        on eventLoop: EventLoop = TDSConnection.defaultEventLoopGroup.any(),
        configuration: TDSConnection.Configuration,
        id connectionID: ID,
        logger: Logger,
        remainingRoutingRedirects: Int
    ) -> EventLoopFuture<TDSConnection> {
        var logger = logger
        logger[tdsMetadataKey: .connectionID] = "\(connectionID)"

        return eventLoop.flatSubmit { [logger] in
            makeBootstrap(on: eventLoop, configuration: configuration)
                .connect(host: configuration.host, port: configuration.port)
                .flatMap { channel -> EventLoopFuture<TDSConnection> in
                    return TDSConnection.start(
                        configuration: configuration,
                        connectionID: connectionID,
                        channel: channel,
                        logger: logger,
                        remainingRoutingRedirects: remainingRoutingRedirects
                    )
                }
        }
    }

    static func makeBootstrap(
        on eventLoop: EventLoop,
        configuration: TDSConnection.Configuration
    ) -> NIOClientTCPBootstrapProtocol {
        #if canImport(Network)
            if let tsBootstrap =
                NIOTSConnectionBootstrap(validatingGroup: eventLoop)
            {
                return
                    tsBootstrap
                    .connectTimeout(configuration.options.connectTimeout)
            }
        #endif

        guard let bootstrap = ClientBootstrap(validatingGroup: eventLoop) else {
            fatalError("No matching bootstrap found")
        }
        return
            bootstrap
            .connectTimeout(configuration.options.connectTimeout)
            .channelOption(
                ChannelOptions
                    .socket(SocketOptionLevel(SOL_SOCKET), SO_KEEPALIVE), value: 1
            )
            .channelOption(
                ChannelOptions
                    .socket(SocketOptionLevel(IPPROTO_TCP), TCP_NODELAY), value: 1)
    }

    /// Closes the connection to the database server synchronously.
    ///
    /// - Note: This method blocks the thread indefinitely, prefer using ``close()``.
    @available(
        *, noasync, message: "syncClose() can block indefinitely, prefer close()",
        renamed: "close()"
    )
    public func syncClose() throws {
        guard !self.isClosed else { return }

        if let eventLoop = MultiThreadedEventLoopGroup.currentEventLoop {
            preconditionFailure(
                """
                syncClose() must not be called when on an NIO EventLoop.
                Calling syncClose() on any EventLoop can lead to deadlocks.
                Current eventLoop: \(eventLoop)
                """)
        }

        self.channel.close(mode: .all, promise: nil)

        func close(queue: DispatchQueue, _ callback: @escaping @Sendable (Error?) -> Void) {
            self.closeFuture.whenComplete { result in
                let error: Error? =
                    switch result {
                    case .failure(let error): error
                    case .success: nil
                    }
                queue.async {
                    callback(error)
                }
            }
        }

        let errorStorage = NIOLockedValueBox<Error?>(nil)
        let continuation = DispatchWorkItem {}
        close(queue: DispatchQueue(label: "tds-nio.close-connection-\(self.id)")) { error in
            if let error {
                errorStorage.withLockedValue { $0 = error }
            }
            continuation.perform()
        }
        continuation.wait()
        try errorStorage.withLockedValue { error in
            if let error { throw error }
        }
    }

    /// Closes the connection to the database server.
    private func close() -> EventLoopFuture<Void> {
        guard !self.isClosed else {
            return self.eventLoop.makeSucceededVoidFuture()
        }

        self.channel.close(mode: .all, promise: nil)
        return self.closeFuture
    }

    func markForResetOnNextRequest() {
        self.resetConnectionOnNextRequestBox.withLockedValue { $0 = true }
    }

    func prepareForNextRequestIfNeeded() {
        let shouldReset = self.resetConnectionOnNextRequestBox.withLockedValue { value in
            defer { value = false }
            return value
        }
        if shouldReset {
            self.channel.triggerUserOutboundEvent(TDSSQLEvent.resetConnectionOnNextRequest, promise: nil)
        }
    }

    func writeAndFlush(_ task: TDSTask) {
        guard !self.isClosed else {
            task.fail(.connectionError(underlying: ChannelError.ioOnClosedChannel))
            return
        }

        let writePromise = self.eventLoop.makePromise(of: Void.self)
        writePromise.futureResult.whenFailure { error in
            task.fail(.connectionError(underlying: error))
        }
        self.channel.writeAndFlush(task, promise: writePromise)
    }
}

// MARK: Async/Await Interface

extension TDSConnection {

    /// Creates a new connection to an TDS server.
    ///
    /// - Parameters:
    ///   - eventLoop: The `EventLoop` the connection shall be created on.
    ///   - configuration: A ``Configuration`` that shall be used for the connection.
    ///   - connectionID: An `Int` id, used for metadata logging.
    /// - Returns: An established ``TDSConnection`` asynchronously that can be used to run
    ///            queries.
    public static func connect(
        on eventLoop: EventLoop = TDSConnection.defaultEventLoopGroup.any(),
        configuration: TDSConnection.Configuration,
        id connectionID: ID
    ) async throws -> TDSConnection {
        try await self.connect(
            on: eventLoop,
            configuration: configuration,
            id: connectionID,
            logger: self.noopLogger
        )
    }

    /// Creates a new connection to an TDS server.
    ///
    /// - Parameters:
    ///   - eventLoop: The `EventLoop` the connection shall be created on.
    ///   - configuration: A ``Configuration`` that shall be used for the connection.
    ///   - connectionID: An `Int` id, used for metadata logging.
    ///   - logger: A logger to log background events into.
    /// - Returns: An established ``TDSConnection`` asynchronously that can be used to run
    ///            queries.
    public static func connect(
        on eventLoop: EventLoop = TDSConnection.defaultEventLoopGroup.any(),
        configuration: TDSConnection.Configuration,
        id connectionID: ID,
        logger: Logger
    ) async throws -> TDSConnection {
        var remainingRetries = configuration.retryCount
        while true {
            try Task.checkCancellation()
            do {
                return try await self.connect(
                    on: eventLoop,
                    configuration: configuration,
                    id: connectionID,
                    logger: logger,
                    remainingRoutingRedirects: configuration.options.routingRedirectLimit
                ).get()
            } catch let error as CancellationError {
                throw error
            } catch {
                guard
                    configuration.shouldRetryConnection(
                        after: error,
                        remainingRetries: remainingRetries
                    )
                else {
                    throw error
                }
                remainingRetries -= 1
            }
            try Task.checkCancellation()
            if configuration.retryDelay > 0 {
                try await Task.sleep(for: .seconds(configuration.retryDelay))
            }
        }
    }

    /// Closes the connection to the database server.
    public func close() async throws {
        try await self.close().get()
    }
}

private enum RoutingRedirectionError: Error {
    case tooManyRedirects
}

extension TDSConnection {
    /// Returns the default `EventLoopGroup` singleton, automatically selecting the best for the
    /// platform.
    ///
    /// This will select the concrete `EventLoopGroup` depending on which platform this is running on.
    public static var defaultEventLoopGroup: EventLoopGroup {
        #if canImport(Network)
            if #available(OSX 10.14, iOS 12.0, tvOS 12.0, watchOS 6.0, *) {
                return NIOTSEventLoopGroup.singleton
            } else {
                return MultiThreadedEventLoopGroup.singleton
            }
        #else
            return MultiThreadedEventLoopGroup.singleton
        #endif
    }
}

#if DEBUG
    private final class DebugLogHandler: ChannelDuplexHandler {
        typealias InboundIn = ByteBuffer
        typealias OutboundIn = ByteBuffer

        private var logger: Logger
        private var shouldLog: Bool

        init(connectionID: TDSConnection.ID, logger: Logger, shouldLog: Bool? = nil) {
            if let shouldLog {
                self.shouldLog = shouldLog
            } else {
                let envValue =
                    getenv("ORANIO_TRACE_PACKETS")
                    .flatMap { String(cString: $0) }
                    .flatMap(Int.init) ?? 0
                self.shouldLog = envValue != 0
            }
            var logger = logger
            logger[tdsMetadataKey: .connectionID] = "\(connectionID)"
            self.logger = logger
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            if self.shouldLog {
                let buffer = self.unwrapInboundIn(data)
                self.logger.info(
                    "\n\(buffer.hexDump(format: .detailed))",
                    metadata: ["direction": "incoming"]
                )
            }
            context.fireChannelRead(data)
        }

        func write(
            context: ChannelHandlerContext,
            data: NIOAny,
            promise: EventLoopPromise<Void>?
        ) {
            if self.shouldLog {
                let buffer = self.unwrapOutboundIn(data)
                self.logger.info(
                    "\n\(buffer.hexDump(format: .detailed))",
                    metadata: ["direction": "outgoing"]
                )
            }
            context.write(data, promise: promise)
        }
    }
#endif
