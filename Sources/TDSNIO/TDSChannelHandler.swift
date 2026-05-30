//===----------------------------------------------------------------------===//
//
// This source file is part of the TDSNIO open source project
//
// Copyright (c) 2026 TDSNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
// See CONTRIBUTORS.md for the list of TDSNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import NIOCore
import NIOSSL
import NIOTLS

import class Foundation.ProcessInfo

final class TDSChannelHandler: ChannelDuplexHandler {
    typealias OutboundIn = TDSTask
    typealias InboundIn = TinySequence<TDSBackendMessageDecoder.Container>
    typealias OutboundOut = ByteBuffer

    private let logger: Logger
    private var state: ConnectionStateMachine
    private let configuration: TDSConnection.Configuration

    /// A `ChannelHandlerContext` to be used for non channel related events.
    ///
    /// For example: More rows needed.
    /// The context is captured in `handlerAdded` and released in `handlerRemoved`.
    private var handlerContext: ChannelHandlerContext?
    private var decoder: ByteToMessageHandler<TDSBackendMessageDecoder>?
    private let decoderContext: TDSBackendMessageDecoder.Context
    private var encoder: TDSFrontendMessageEncoder!
    private var preloginTLSHandler: TDSPreloginTLSHandler?
    private var sslHandler: NIOSSLClientHandler?
    private var session: TDSSessionContext
    private var rowStream: TDSRowStream?

    init(
        configuration: TDSConnection.Configuration,
        logger: Logger
    ) {
        self.state = ConnectionStateMachine(debugLog: { message in
            logger.debug("TDS state machine", metadata: ["tds.debug": "\(message)"])
        })
        self.configuration = configuration
        self.logger = logger
        self.session = TDSSessionContext(requestedProtocolVersion: configuration.protocolVersion)
        self.decoderContext = .init()
    }

    // MARK: Handler Lifecycle

    func handlerAdded(context: ChannelHandlerContext) {
        self.handlerContext = context
        self.decoder = ByteToMessageHandler(TDSBackendMessageDecoder(context: self.decoderContext))
        self.encoder = TDSFrontendMessageEncoder(
            buffer: context.channel.allocator.buffer(capacity: self.configuration.packetSize)
        )
        do {
            try context.pipeline.syncOperations
                .addHandler(self.decoder!, position: .before(self))
        } catch {
            context.fireErrorCaught(error)
            return
        }

        if context.channel.isActive {
            self.connected(context: context)
        }
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        self.handlerContext = nil
    }

    // MARK: Channel handler incoming

    func channelActive(context: ChannelHandlerContext) {
        // `fireChannelActive` needs to be called BEFORE we set the state
        // machine to connected, since we want to make sure that upstream
        // handlers know about the active connection before it receives a
        context.fireChannelActive()

        self.connected(context: context)
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.logger.trace("Channel inactive.")
        let action = self.state.closed()
        self.run(action, with: context)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.logger.debug("Channel error caught.", metadata: [.error: "\(error)"])
        let action =
            if let error = error as? TDSSQLError {
                self.state.errorHappened(error)
            } else {
                self.state.errorHappened(.connectionError(underlying: error))
            }
        self.run(action, with: context)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let containers = self.unwrapInboundIn(data)
        for container in containers {
            for message in container.messages {
                self.handleMessage(
                    message,
                    context: context
                )
            }
        }
    }

    private func handleMessage(
        _ message: TDSBackendMessage,
        context: ChannelHandlerContext
    ) {
        self.logger.trace("Backend message received", metadata: [.message: "\(message)"])
        let action: ConnectionStateMachine.ConnectionAction
        switch message {
        case .prelogin(let response):
            if self.configuration.tls.isCompatible(with: response.encryption) {
                action = self.state.preloginReceived(
                    response,
                    clientEncryption: self.configuration.tls.preloginEncryption
                )
            } else {
                action = self.state.errorHappened(
                    .connectionError(
                        underlying: PreloginEncryptionNegotiationError(
                            client: self.configuration.tls.preloginEncryption,
                            server: response.encryption
                        )
                    )
                )
            }
        case .loginAck(let ack):
            self.session.receiveLoginAck(ack)
            action = self.state.loginAckReceived(ack)
        case .done(let done):
            self.logger.debug(
                "DONE token received",
                metadata: [
                    "tds.done.kind": "DONE",
                    "tds.done.status": "\(done.status.rawValue)",
                    "tds.done.row_count": "\(done.rowCount)",
                ])
            action = self.state.doneReceived(done, tokenKind: .done)
        case .doneProc(let done):
            self.logger.debug(
                "DONE token received",
                metadata: [
                    "tds.done.kind": "DONEPROC",
                    "tds.done.status": "\(done.status.rawValue)",
                    "tds.done.row_count": "\(done.rowCount)",
                ])
            action = self.state.doneReceived(done, tokenKind: .doneProc)
        case .doneInProc(let done):
            self.logger.debug(
                "DONE token received",
                metadata: [
                    "tds.done.kind": "DONEINPROC",
                    "tds.done.status": "\(done.status.rawValue)",
                    "tds.done.row_count": "\(done.rowCount)",
                ])
            action = self.state.doneReceived(done, tokenKind: .doneInProc)
        case .error(let error):
            action = self.state.backendErrorReceived(error)
        case .info(let info):
            let message = TDSInfoMessage(info)
            self.configuration.options.infoMessageHandler?(message)
            context.fireUserInboundEventTriggered(message)
            action = .wait
        case .envChange(let envChange):
            if case .routing(let routing) = envChange.value {
                context.fireUserInboundEventTriggered(TDSSQLEvent.routing(routing))
            }
            switch self.session.receiveEnvChange(envChange) {
            case .transactionDescriptorChanged(let new):
                self.logger.debug(
                    "Transaction descriptor ENVCHANGE received",
                    metadata: [
                        "tds.envchange.type": "\(envChange.type)",
                        "tds.envchange.new_length": "\(new.count)",
                    ])
                self.encoder.setTransactionDescriptor(new)
            case .packetSizeChanged(let packetSize):
                context.triggerUserOutboundEvent(
                    TDSSQLEvent.packetSizeChanged(packetSize), promise: nil)
            case .none:
                break
            }
            let message = TDSEnvChangeMessage(envChange)
            self.configuration.options.envChangeHandler?(message)
            context.fireUserInboundEventTriggered(message)
            action = .wait
        case .featureExtAck(let featureExtAck):
            self.session.receiveFeatureExtAck(featureExtAck)
            action = .wait
        case .colMetadata(let metadata):
            self.logger.debug(
                "COLMETADATA token received",
                metadata: [
                    "tds.columns": "\(metadata.columns.count)",
                    "tds.column_names": "\(metadata.columns.map(\.name))",
                ])
            action = self.state.colMetadataReceived(metadata)
        case .tabName(let tabName):
            action = self.state.tabNameReceived(tabName)
        case .colInfo(let colInfo):
            action = self.state.colInfoReceived(colInfo)
        case .order(let order):
            action = self.state.orderReceived(order)
        case .dataClassification(let dataClassification):
            action = self.state.dataClassificationReceived(dataClassification)
        case .altMetadata(let altMetadata):
            action = self.state.altMetadataReceived(altMetadata)
        case .altRow(let altRow):
            action = self.state.altRowReceived(altRow)
        case .offset(let offset):
            action = self.state.offsetReceived(offset)
        case .sessionState(let sessionState):
            let message = TDSSessionStateMessage(sessionState)
            self.configuration.options.sessionStateHandler?(message)
            context.fireUserInboundEventTriggered(message)
            action = .wait
        case .sspi(let bytes):
            action = self.state.sspiReceived(bytes)
        case .fedAuthInfo(let fedAuthInfo):
            action = self.state.fedAuthInfoReceived(fedAuthInfo)
        case .row(let row):
            self.logger.trace(
                "ROW token received",
                metadata: ["tds.values": "\(row.values.count)"])
            action = self.state.rowReceived(row)
        case .returnStatus(let status):
            action = self.state.returnStatusReceived(status)
        case .returnValue(let returnValue):
            action = self.state.returnValueReceived(returnValue)
        case .unknownToken:
            action = .wait
        }
        self.run(action, with: context)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        self.run(self.state.channelReadComplete(), with: context)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case TLSUserEvent.handshakeCompleted:
            self.run(self.state.tlsEstablished(), with: context)
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }

    // MARK: Channel handler outgoing

    func read(context: ChannelHandlerContext) {
        self.run(self.state.readEventCaught(), with: context)
    }

    func write(
        context: ChannelHandlerContext,
        data: NIOAny,
        promise: EventLoopPromise<Void>?
    ) {
        let task = self.unwrapOutboundIn(data)
        promise?.succeed(())
        self.run(self.state.enqueue(task: task, promise: promise), with: context)
    }

    func close(
        context: ChannelHandlerContext,
        mode: CloseMode,
        promise: EventLoopPromise<Void>?
    ) {
        guard mode == .all else {
            promise?.fail(ChannelError.operationUnsupported)
            return
        }
        self.run(self.state.close(promise), with: context)
    }

    func triggerUserOutboundEvent(
        context: ChannelHandlerContext,
        event: Any,
        promise: EventLoopPromise<Void>?
    ) {
        switch event {
        case TDSAuthenticationToken.sspi(let bytes):
            self.encoder.sspi(bytes)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: promise)
        case TDSAuthenticationToken.federated(let token, let nonce):
            guard nonce == nil || nonce?.count == 32 else {
                promise?.fail(
                    TDSSQLError.connectionError(
                        underlying: InvalidFederatedAuthenticationNonceLength()
                    ))
                return
            }
            self.encoder.federatedAuthenticationToken(token: token, nonce: nonce)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: promise)
        case TDSSQLEvent.resetConnectionOnNextRequest:
            self.encoder.markResetConnectionOnNextRequest()
            promise?.succeed(())
        default:
            context.triggerUserOutboundEvent(event, promise: promise)
        }
    }

    // Mark: Channel handler functions
    func run(
        _ action: ConnectionStateMachine.ConnectionAction,
        flags: UInt8? = nil,
        with context: ChannelHandlerContext
    ) {
        self.logger.trace(
            "Run action",
            metadata: [
                .connectionAction: "\(action)"
            ])

        switch action {
        case .wait:
            break
        case .read:
            context.read()
        case .sendPreloginRequest:
            self.sendPreloginRequest(context: context)
        case .startTLS:
            self.startTLS(context: context)
        case .sendLoginRequest:
            self.sendLoginRequest(context: context)
        case .fireAuthenticationChallenge(let challenge):
            context.fireUserInboundEventTriggered(challenge)
        case .authenticated(let loginAck, let removeTLS):
            if removeTLS {
                self.removeLoginOnlyTLS(context: context)
            }
            context.fireUserInboundEventTriggered(
                TDSSQLEvent.startupDone(
                    version: TDSProtocolVersion(loginAck: loginAck),
                    sessionID: 0,
                    serialNumber: 0
                )
            )
            self.startNextTaskOrFireReady(context: context)
        case .sendSQLBatch(let sql):
            self.encoder.sqlBatch(sql)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .sendRPC(let rpc):
            self.encoder.rpc(rpc)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .sendTransactionManagerRequest(let request):
            self.encoder.transactionManagerRequest(request)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .sendBulkLoad(let request):
            self.encoder.bulkLoad(request)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .sendAttention:
            self.logger.debug("Sending TDS attention packet.")
            self.encoder.attention()
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .succeedQuery(let promise, let result):
            promise.succeed(result)
            self.startNextTaskOrFireReady(context: context)
        case .succeedTask(let promise):
            promise.succeed(())
            self.startNextTaskOrFireReady(context: context)
        case .failTask(let promise, let error):
            promise.fail(error)
        case .completeFailedQuery:
            self.startNextTaskOrFireReady(context: context)
        case .succeedRowStream(let promise, let columns):
            let stream = TDSRowStream(
                columns: columns,
                eventLoop: promise.futureResult.eventLoop,
                dataSource: self,
                debugLog: { [logger] message in
                    logger.debug("TDS row stream", metadata: ["tds.debug": "\(message)"])
                }
            )
            self.rowStream = stream
            promise.succeed(stream)
        case .forwardRows(let rows):
            self.rowStream?.receive(rows)
        case .forwardRowsAndComplete(let rows):
            self.rowStream?.receive(rows)
            self.rowStream?.receive(completion: .success(()))
            self.rowStream = nil
        case .forwardRowsAndCompleteQuery(let rows, let emptyStreamPromise):
            if let emptyStreamPromise {
                emptyStreamPromise.succeed(
                    TDSRowStream(rows: rows, eventLoop: emptyStreamPromise.futureResult.eventLoop))
            } else {
                self.rowStream?.receive(rows)
                self.rowStream?.receive(completion: .success(()))
                self.rowStream = nil
            }
            self.startNextTaskOrFireReady(context: context)
        case .forwardRow(let row):
            self.rowStream?.receive(row)
        case .finishActiveRowStream:
            self.rowStream?.receive(completion: .success(()))
            self.rowStream = nil
        case .failActiveRowStream(let error):
            self.rowStream?.receive(completion: .failure(error))
            self.rowStream = nil
        case .cancelActiveRowStream(let promise):
            self.rowStream?.receive(completion: .failure(TDSSQLError.requestCancelled()))
            self.rowStream = nil
            promise?.succeed(())
            self.startNextTaskOrFireReady(context: context)
        case .completeRowStreamQuery(let emptyStreamPromise):
            if let emptyStreamPromise {
                emptyStreamPromise.succeed(
                    TDSRowStream(rows: [], eventLoop: emptyStreamPromise.futureResult.eventLoop))
            } else {
                self.rowStream?.receive(completion: .success(()))
                self.rowStream = nil
            }
            self.startNextTaskOrFireReady(context: context)
        case .succeedCancel(let promise):
            promise.succeed(())
            self.startNextTaskOrFireReady(context: context)
        case .failCancel(let promise, let error):
            promise.fail(error)
        case .closeConnection(let promise):
            context.close(mode: .all, promise: promise)
        case .fireChannelInactive:
            context.fireChannelInactive()
        case .closeConnectionAndCleanup(let cleanup):
            for task in cleanup.tasks {
                task.fail(cleanup.error)
            }
            if let rowStreamError = cleanup.rowStreamError {
                self.rowStream?.receive(completion: .failure(rowStreamError))
                self.rowStream = nil
            }
            context.fireErrorCaught(cleanup.error)
            if cleanup.read {
                context.read()
            }
            switch cleanup.action {
            case .close:
                context.close(mode: .all, promise: cleanup.closePromise)
            case .fireChannelInactive:
                context.fireChannelInactive()
            }
        }
    }

    // MARK: Private

    private func startNextTaskOrFireReady(context: ChannelHandlerContext) {
        let action = self.state.startNextTask()
        if case .wait = action {
            context.fireUserInboundEventTriggered(TDSSQLEvent.readyForQuery)
        } else {
            self.run(action, with: context)
        }
    }

    private func connected(context: ChannelHandlerContext) {
        let action = self.state.connected()
        self.run(action, with: context)
    }

    private func sendPreloginRequest(
        context: ChannelHandlerContext
    ) {
        self.encoder.prelogin(encryption: self.configuration.tls.preloginEncryption)
        context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
    }

    private func sendLoginRequest(
        context: ChannelHandlerContext
    ) {
        self.encoder.login(configuration: self.configuration)
        context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
    }

    private func startTLS(context: ChannelHandlerContext) {
        guard let sslContext = self.configuration.tls.sslContext else {
            context.fireErrorCaught(
                TDSSQLError.connectionError(
                    underlying: MissingTLSContextError()
                )
            )
            return
        }

        do {
            let preloginTLSHandler = TDSPreloginTLSHandler()
            let sslHandler = try NIOSSLClientHandler(
                context: sslContext,
                serverHostname: self.configuration.serverNameForTLS
            )
            self.preloginTLSHandler = preloginTLSHandler
            self.sslHandler = sslHandler
            try context.pipeline.syncOperations.addHandler(
                preloginTLSHandler,
                position: .first
            )
            try context.pipeline.syncOperations.addHandler(
                sslHandler,
                position: .after(preloginTLSHandler)
            )
        } catch {
            context.fireErrorCaught(TDSSQLError.connectionError(underlying: error))
        }
    }

    private func removeLoginOnlyTLS(
        context: ChannelHandlerContext
    ) {
        if let sslHandler = self.sslHandler {
            _ = context.pipeline.syncOperations.removeHandler(sslHandler)
            self.sslHandler = nil
        }
        if let preloginTLSHandler = self.preloginTLSHandler {
            _ = context.pipeline.syncOperations.removeHandler(preloginTLSHandler)
            self.preloginTLSHandler = nil
        }
    }
}

private struct MissingTLSContextError: Error {}

private struct InvalidFederatedAuthenticationNonceLength: Error {}

extension TDSChannelHandler: TDSRowsDataSource {
    func request(for stream: TDSRowStream) {
        guard self.rowStream === stream, let handlerContext else {
            return
        }
        self.run(self.state.requestQueryRows(), with: handlerContext)
    }

    func cancel(for stream: TDSRowStream) {
        guard self.rowStream === stream, let handlerContext else {
            return
        }
        self.rowStream = nil
        self.run(self.state.cancelQueryStream(), with: handlerContext)
    }
}

private struct PreloginEncryptionNegotiationError: Error, CustomStringConvertible {
    var client: TDSFrontendMessageEncoder.PreloginEncryption
    var server: TDSFrontendMessageEncoder.PreloginEncryption?

    var description: String {
        "Incompatible PRELOGIN encryption negotiation. client=\(self.client), server=\(String(describing: self.server))"
    }
}
