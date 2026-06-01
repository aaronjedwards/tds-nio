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

import NIOCore

import struct Foundation.TimeZone

struct ConnectionStateMachine {
    enum State {
        case initialized
        case sentPrelogin
        case sentTLSNegotiation
        case sentLoginWithCompleteAuth
        case sentLoginWithSpengo
        case sentLoginWithFedAuth
        case sentInitialSQL(loginAck: TDSBackendMessage.LoginAck, removeTLS: Bool)
        case loggedIn
        case sentClientRequest
        case sentAttention
        case routingComplete
        case closing
        case closed
        case modifying
    }

    enum QuiescingState {
        case notQuiescing
        case quiescing(closePromise: EventLoopPromise<Void>?)
    }

    enum MarkerState {
        case noMarkerSent
        case markerSent
    }

    enum DoneTokenKind {
        case done
        case doneProc
        case doneInProc
    }

    enum ConnectionAction {

        struct CleanUpContext {
            enum Action {
                case close
                case fireChannelInactive
            }

            let action: Action

            /// Tasks to fail with the error
            let tasks: [TDSTask]

            let error: TDSSQLError

            /// A live row stream to fail after the state machine has been cleaned up.
            let rowStreamError: TDSSQLError?

            /// We need to read remaining data from the channel
            /// if a marker response is pending.
            let read: Bool

            let closePromise: EventLoopPromise<Void>?
        }

        case wait
        case read
        case closeConnection(EventLoopPromise<Void>?)
        case fireChannelInactive
        /// Close connection because of an error state. Fail all tasks with the provided error.
        case closeConnectionAndCleanup(CleanUpContext)
        case sendPreloginRequest
        case startTLS(removeAfterLogin: Bool)
        case sendLoginRequest
        case fireAuthenticationChallenge(TDSAuthenticationChallenge)
        case authenticated(TDSBackendMessage.LoginAck, removeTLS: Bool)
        case startupComplete(TDSBackendMessage.LoginAck, removeTLS: Bool)
        case sendSQLBatch(String)
        case sendRPC(TDSRPC)
        case sendTransactionManagerRequest(TDSTransactionManagerRequest)
        case sendBulkLoad(TDSBulkLoadRequest)
        case sendAttention
        case succeedQuery(EventLoopPromise<TDSQueryResult>, TDSQueryResult)
        case succeedTask(EventLoopPromise<Void>)
        case failTask(EventLoopPromise<TDSQueryResult>, TDSSQLError)
        case completeFailedQuery
        case succeedRowStream(EventLoopPromise<TDSRowStream>, [TDSColumn])
        case forwardRows([TDSRow])
        case forwardRowsAndComplete([TDSRow])
        case forwardRowsAndCompleteQuery([TDSRow], EventLoopPromise<TDSRowStream>?)
        case forwardRow(TDSRow)
        case finishActiveRowStream
        case failActiveRowStream(TDSSQLError)
        case cancelActiveRowStream(EventLoopPromise<Void>?)
        case completeRowStreamQuery(EventLoopPromise<TDSRowStream>?)
        case succeedCancel(EventLoopPromise<Void>)
        case failCancel(EventLoopPromise<Void>, TDSSQLError)
    }

    private var state: State
    private var taskQueue = CircularBuffer<TDSTask>()
    private var quiescingState: QuiescingState = .notQuiescing
    private var markerState: MarkerState = .noMarkerSent
    private var loginAck: TDSBackendMessage.LoginAck?
    private var authenticationStateMachine: AuthenticationStateMachine?
    private var activeTask: TDSTask?
    private var activeRequest: StatementStateMachine?
    private var activeColumns: [TDSColumn] = []
    private var activeRows: [TDSRow] = []
    private var activeRowStreamStarted = false
    private var activeResultSets: [TDSResultSet] = []
    private var activeOffsets: [TDSOffset] = []
    private var activeAlternateResultSets: [TDSAlternateResultSet] = []
    private var activeReturnStatus: Int32?
    private var activeOutputParameters: [TDSOutputParameter] = []
    private var activeTableNames: [String] = []
    private var activeError: TDSSQLError?
    private var activeTaskFailed = false
    private var removeTLSAfterLogin = false
    private var attentionPromise: EventLoopPromise<Void>?
    private let debugLog: (@Sendable (String) -> Void)?

    var isWaitingForAttentionAcknowledgement: Bool {
        if case .sentAttention = self.state {
            return true
        }
        return false
    }

    var isWaitingForLoginResponse: Bool {
        if case .sentLoginWithCompleteAuth = self.state {
            return true
        }
        return false
    }

    init(debugLog: (@Sendable (String) -> Void)? = nil) {
        self.state = .initialized
        self.debugLog = debugLog
    }

    #if DEBUG
        /// for testing purposes only
        init(_ state: State) {
            self.state = state
            self.debugLog = nil
        }
    #endif

    mutating func connected() -> ConnectionAction {
        switch self.state {
        case .initialized:
            self.state = .sentPrelogin
            var authenticationStateMachine = AuthenticationStateMachine()
            let action = authenticationStateMachine.connected()
            self.authenticationStateMachine = authenticationStateMachine
            return self.connectionAction(from: action)
        default:
            return .wait
        }
    }

    mutating func close(
        _ promise: EventLoopPromise<Void>?
    ) -> ConnectionAction {
        return self.closeConnectionAndCleanup(
            .clientClosedConnection(underlying: nil),
            closePromise: promise
        )
    }

    mutating func closed() -> ConnectionAction {
        switch self.state {
        case .initialized:
            preconditionFailure(
                "How can a connection be closed, if it was never connected."
            )
        case .sentPrelogin,
            .sentTLSNegotiation,
            .sentLoginWithCompleteAuth,
            .sentLoginWithSpengo,
            .sentLoginWithFedAuth,
            .sentInitialSQL,
            .loggedIn,
            .sentClientRequest,
            .sentAttention,
            .routingComplete:
            return self.closeConnectionAndCleanup(
                .connectionError(underlying: ChannelError.ioOnClosedChannel),
                closePromise: nil,
                action: .fireChannelInactive
            )
        case .closing:
            self.state = .closed
            self.quiescingState = .notQuiescing
            return .fireChannelInactive
        case .closed:
            preconditionFailure(
                "How can a connection be closed, if it is already closed."
            )
        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }

    mutating func errorHappened(_ error: TDSSQLError) -> ConnectionAction {
        switch self.state {
        case .closing:
            return .wait
        case .closed:
            return self.closeConnectionAndCleanup(
                error,
                action: .fireChannelInactive
            )
        default:
            return self.closeConnectionAndCleanup(error)
        }
    }

    mutating func preloginReceived(
        _ response: TDSBackendMessage.PreloginResponse,
        clientEncryption: TDSFrontendMessageEncoder.PreloginEncryption
    ) -> ConnectionAction {
        guard case .sentPrelogin = self.state else {
            return .wait
        }

        var authenticationStateMachine = self.authenticationStateMachine ?? AuthenticationStateMachine()
        if self.authenticationStateMachine == nil {
            _ = authenticationStateMachine.connected()
        }
        let action = authenticationStateMachine.preloginReceived(
            response,
            clientEncryption: clientEncryption
        )
        self.authenticationStateMachine = authenticationStateMachine
        switch action {
        case .startTLS(let removeAfterLogin):
            self.state = .sentTLSNegotiation
            self.removeTLSAfterLogin = removeAfterLogin
        case .sendLoginRequest:
            self.state = .sentLoginWithCompleteAuth
        default:
            break
        }
        return self.connectionAction(from: action)
    }

    mutating func tlsEstablished() -> ConnectionAction {
        guard case .sentTLSNegotiation = self.state else {
            return .wait
        }
        guard var authenticationStateMachine = self.authenticationStateMachine else {
            return .wait
        }
        let action = authenticationStateMachine.tlsEstablished()
        self.authenticationStateMachine = authenticationStateMachine
        if case .sendLoginRequest = action {
            self.state = .sentLoginWithCompleteAuth
        }
        return self.connectionAction(from: action)
    }

    mutating func loginAckReceived(_ ack: TDSBackendMessage.LoginAck) -> ConnectionAction {
        guard case .sentLoginWithCompleteAuth = self.state else {
            return .wait
        }
        if var authenticationStateMachine = self.authenticationStateMachine {
            let action = authenticationStateMachine.loginAckReceived(ack)
            self.authenticationStateMachine = authenticationStateMachine
            self.loginAck = ack
            return self.connectionAction(from: action)
        }
        return .wait
    }

    mutating func sspiReceived(_ bytes: [UInt8]) -> ConnectionAction {
        guard var authenticationStateMachine = self.authenticationStateMachine else {
            return .wait
        }
        let action = authenticationStateMachine.sspiReceived(bytes)
        self.authenticationStateMachine = authenticationStateMachine
        return self.connectionAction(from: action)
    }

    mutating func fedAuthInfoReceived(
        _ fedAuthInfo: TDSBackendMessage.FedAuthInfo
    ) -> ConnectionAction {
        guard var authenticationStateMachine = self.authenticationStateMachine else {
            return .wait
        }
        let action = authenticationStateMachine.fedAuthInfoReceived(fedAuthInfo)
        self.authenticationStateMachine = authenticationStateMachine
        return self.connectionAction(from: action)
    }

    mutating func authenticationTokenSent() -> ConnectionAction {
        guard var authenticationStateMachine = self.authenticationStateMachine else {
            return .wait
        }
        let action = authenticationStateMachine.authenticationTokenSent()
        self.authenticationStateMachine = authenticationStateMachine
        return self.connectionAction(from: action)
    }

    mutating func featureExtAckReceived(
        _ featureExtAck: TDSBackendMessage.FeatureExtAck,
        requestedFeatureIDs: Set<UInt8>? = nil
    ) -> ConnectionAction {
        guard var authenticationStateMachine = self.authenticationStateMachine else {
            return .wait
        }
        let action: AuthenticationStateMachine.Action
        if let requestedFeatureIDs {
            action = authenticationStateMachine.featureExtAckReceived(
                featureExtAck,
                requestedFeatureIDs: requestedFeatureIDs
            )
        } else {
            action = authenticationStateMachine.featureExtAckReceived(featureExtAck)
        }
        self.authenticationStateMachine = authenticationStateMachine
        return self.connectionAction(from: action)
    }

    mutating func doneReceived(
        _ done: TDSBackendMessage.Done,
        tokenKind: DoneTokenKind = .done
    ) -> ConnectionAction {
        switch self.state {
        case .sentLoginWithCompleteAuth:
            guard tokenKind == .done else {
                return .wait
            }
            if var authenticationStateMachine = self.authenticationStateMachine {
                let authAction = authenticationStateMachine.doneReceived()
                self.authenticationStateMachine = authenticationStateMachine
                if case .authenticated = authAction {
                    self.state = .loggedIn
                    self.loginAck = nil
                    self.removeTLSAfterLogin = false
                    self.authenticationStateMachine = nil
                }
                return self.connectionAction(from: authAction)
            } else if let loginAck {
                self.state = .loggedIn
                self.loginAck = nil
                let removeTLS = self.removeTLSAfterLogin
                self.removeTLSAfterLogin = false
                return .authenticated(loginAck, removeTLS: removeTLS)
            }
            return .wait
        case .sentInitialSQL(let loginAck, let removeTLS):
            guard tokenKind == .done else {
                return .wait
            }
            guard !done.status.contains(.more) else {
                return .wait
            }
            if done.status.contains(.error) || done.status.contains(.serverError) {
                let error =
                    self.activeError
                    ?? .server("Server completed the startup SQL batch with a DONE error status.")
                return self.closeConnectionAndCleanup(error)
            }
            if let error = self.activeError {
                return self.closeConnectionAndCleanup(error)
            }
            self.state = .loggedIn
            self.clearActiveResult()
            return .startupComplete(loginAck, removeTLS: removeTLS)
        case .sentClientRequest:
            guard var activeRequest = self.activeRequest else {
                self.state = .loggedIn
                return .wait
            }
            let requestAction = activeRequest.doneReceived(
                done,
                tokenKind: .init(tokenKind)
            )
            self.activeRequest = activeRequest
            return self.connectionAction(from: requestAction)
        case .sentAttention:
            self.debug(
                "DONE token while waiting for attention ack kind=\(tokenKind) statusRaw=\(done.status.rawValue) "
                    + "rowCount=\(done.rowCount) hasAttention=\(done.status.contains(.attention))"
            )
            guard tokenKind == .done, done.status.contains(.attention) else {
                return .wait
            }
            if self.activeRequest == nil, let activeTask = self.activeTask {
                activeTask.fail(.requestCancelled())
            }
            let requestAction = self.activeRequest?.fail(.requestCancelled())
            self.activeTask = nil
            self.activeRequest = nil
            self.clearActiveResult()
            self.state = .loggedIn
            if case .forwardStreamError(let error) = requestAction {
                let promise = self.attentionPromise
                self.attentionPromise = nil
                if let promise {
                    promise.succeed(())
                }
                _ = error
                return .cancelActiveRowStream(nil)
            }
            if let attentionPromise {
                self.attentionPromise = nil
                return .succeedCancel(attentionPromise)
            }
            return .wait
        default:
            return .wait
        }
    }

    mutating func backendMessageComplete() -> ConnectionAction {
        guard case .sentLoginWithCompleteAuth = self.state else {
            return .wait
        }
        guard var authenticationStateMachine = self.authenticationStateMachine else {
            return .wait
        }
        let action = authenticationStateMachine.messageComplete()
        self.authenticationStateMachine = authenticationStateMachine
        if case .authenticated = action {
            self.state = .loggedIn
            self.loginAck = nil
            self.removeTLSAfterLogin = false
            self.authenticationStateMachine = nil
        }
        return self.connectionAction(from: action)
    }

    mutating func startInitialSQL(
        _ sql: String,
        loginAck: TDSBackendMessage.LoginAck,
        removeTLS: Bool
    ) -> ConnectionAction {
        guard case .loggedIn = self.state else {
            return .wait
        }
        self.state = .sentInitialSQL(loginAck: loginAck, removeTLS: removeTLS)
        self.clearActiveResult()
        return .sendSQLBatch(sql)
    }

    mutating func backendErrorReceived(_ error: TDSBackendMessage.InfoError) -> ConnectionAction {
        if case .sentLoginWithCompleteAuth = self.state,
            var authenticationStateMachine = self.authenticationStateMachine
        {
            let action = authenticationStateMachine.backendErrorReceived(error)
            self.authenticationStateMachine = authenticationStateMachine
            return self.connectionAction(from: action)
        }
        if case .sentInitialSQL = self.state {
            if self.activeError != nil {
                self.activeError?.appendServerError(error)
            } else {
                self.activeError = TDSSQLError.server(error)
            }
            return .wait
        }
        if var activeRequest = self.activeRequest {
            let action = activeRequest.backendErrorReceived(error)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        let sqlError = TDSSQLError.server(error)
        return self.closeConnectionAndCleanup(sqlError)
    }

    mutating func colMetadataReceived(_ metadata: TDSBackendMessage.ColMetadata) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.colMetadataReceived(metadata)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return self.unexpectedTokenReceived("COLMETADATA")
    }

    mutating func tabNameReceived(_ tabName: TDSBackendMessage.TabName) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.tabNameReceived(tabName)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return self.unexpectedTokenReceived("TABNAME")
    }

    mutating func colInfoReceived(_ colInfo: TDSBackendMessage.ColInfo) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.colInfoReceived(colInfo)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return self.unexpectedTokenReceived("COLINFO")
    }

    mutating func orderReceived(_ order: TDSBackendMessage.Order) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.orderReceived(order)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return self.unexpectedTokenReceived("ORDER")
    }

    mutating func offsetReceived(_ offset: TDSBackendMessage.Offset) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.offsetReceived(offset)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return self.unexpectedTokenReceived("OFFSET")
    }

    mutating func dataClassificationReceived(
        _ dataClassification: TDSBackendMessage.DataClassification
    ) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.dataClassificationReceived(dataClassification)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return self.unexpectedTokenReceived("DATACLASSIFICATION")
    }

    mutating func altMetadataReceived(_ altMetadata: TDSBackendMessage.AltMetadata)
        -> ConnectionAction
    {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.altMetadataReceived(altMetadata)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return self.unexpectedTokenReceived("ALTMETADATA")
    }

    mutating func altRowReceived(_ altRow: TDSBackendMessage.AltRow) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.altRowReceived(altRow)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return self.unexpectedTokenReceived("ALTROW")
    }

    mutating func rowReceived(_ row: TDSBackendMessage.Row) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.rowReceived(row)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return self.unexpectedTokenReceived("ROW")
    }

    mutating func returnStatusReceived(_ status: Int32) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.returnStatusReceived(status)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        if case .sentInitialSQL = self.state {
            return .wait
        }
        return self.unexpectedTokenReceived("RETURNSTATUS")
    }

    mutating func returnValueReceived(_ returnValue: TDSBackendMessage.ReturnValue)
        -> ConnectionAction
    {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.returnValueReceived(returnValue)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        if case .sentInitialSQL = self.state {
            return .wait
        }
        return self.unexpectedTokenReceived("RETURNVALUE")
    }

    mutating func channelReadComplete() -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.channelReadComplete()
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return .wait
    }

    mutating func readEventCaught() -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.read()
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        return .read
    }

    mutating func requestQueryRows() -> ConnectionAction {
        guard var activeRequest = self.activeRequest else {
            return .wait
        }
        let action = activeRequest.requestRows()
        self.activeRequest = activeRequest
        return self.connectionAction(from: action)
    }

    mutating func cancelQueryStream() -> ConnectionAction {
        guard case .sentClientRequest = self.state else {
            return .wait
        }
        self.state = .sentAttention
        self.attentionPromise = nil
        return .sendAttention
    }

    mutating func enqueue(
        task: TDSTask,
        promise: EventLoopPromise<Void>?
    ) -> ConnectionAction {
        switch self.state {
        case .loggedIn:
            return self.start(task)
        case .sentClientRequest:
            switch task {
            case .attention(let promise):
                self.state = .sentAttention
                self.attentionPromise = promise
                return .sendAttention
            case .sqlBatch, .rpc, .transactionManager, .bulkLoad, .sqlBatchRows, .rpcRows, .ping:
                self.taskQueue.append(task)
                return .wait
            }
        case .sentInitialSQL:
            self.taskQueue.append(task)
            return .wait
        case .sentAttention:
            switch task {
            case .attention(let promise):
                return .failCancel(
                    promise,
                    .connectionError(underlying: ChannelError.operationUnsupported)
                )
            case .sqlBatch, .rpc, .transactionManager, .bulkLoad, .sqlBatchRows, .rpcRows, .ping:
                let error = TDSSQLError.connectionError(
                    underlying: ChannelError.operationUnsupported)
                task.fail(error)
                promise?.fail(error)
                return .wait
            }
        case .closing, .closed:
            let error = TDSSQLError.connectionError(underlying: ChannelError.ioOnClosedChannel)
            task.fail(error)
            promise?.fail(error)
            return .wait
        default:
            self.taskQueue.append(task)
            return .wait
        }
    }

    mutating func startNextTask() -> ConnectionAction {
        guard case .loggedIn = self.state, let task = self.taskQueue.popFirst() else {
            return .wait
        }
        return self.start(task)
    }

    private mutating func closeConnectionAndCleanup(
        _ error: TDSSQLError,
        closePromise: EventLoopPromise<Void>? = nil,
        action requestedAction: ConnectionAction.CleanUpContext.Action? = nil
    ) -> ConnectionAction {
        let tasks = Array(self.taskQueue) + [self.activeTask].compactMap { $0 }
        self.taskQueue.removeAll()
        self.activeTask = nil
        self.activeRequest = nil
        self.attentionPromise?.fail(error)
        self.attentionPromise = nil
        let rowStreamError = self.activeRowStreamStarted ? error : nil
        self.activeRowStreamStarted = false
        self.clearActiveResult()
        let defaultAction: ConnectionAction.CleanUpContext.Action
        if case .closed = self.state {
            defaultAction = .fireChannelInactive
        } else {
            defaultAction = .close
        }
        let action = requestedAction ?? defaultAction
        self.state = action == .close ? .closing : .closed
        return .closeConnectionAndCleanup(
            .init(
                action: action,
                tasks: tasks,
                error: error,
                rowStreamError: rowStreamError,
                read: false,
                closePromise: closePromise
            )
        )
    }

    private static func requiresTLS(
        serverEncryption: TDSFrontendMessageEncoder.PreloginEncryption?,
        clientEncryption: TDSFrontendMessageEncoder.PreloginEncryption
    ) -> Bool {
        switch serverEncryption {
        case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
            return true
        case .encryptOff, .encryptNotSup, .encryptClientCertOff, nil:
            switch clientEncryption {
            case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
                return true
            case .encryptOff, .encryptNotSup, .encryptClientCertOff:
                return false
            }
        }
    }

    private static func isLoginOnlyTLS(
        serverEncryption: TDSFrontendMessageEncoder.PreloginEncryption?,
        clientEncryption: TDSFrontendMessageEncoder.PreloginEncryption
    ) -> Bool {
        switch serverEncryption {
        case .encryptOff, .encryptNotSup, .encryptClientCertOff, nil:
            switch clientEncryption {
            case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
                return true
            case .encryptOff, .encryptNotSup, .encryptClientCertOff:
                return false
            }
        case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
            return false
        }
    }

    private mutating func clearActiveResult() {
        self.debug(
            "clearing active result columns=\(self.activeColumns.count) bufferedRows=\(self.activeRows.count) "
                + "rowStreamStarted=\(self.activeRowStreamStarted)"
        )
        self.activeColumns = []
        self.activeRows = []
        self.activeRowStreamStarted = false
        self.activeResultSets = []
        self.activeOffsets = []
        self.activeAlternateResultSets = []
        self.activeReturnStatus = nil
        self.activeOutputParameters = []
        self.activeTableNames = []
        self.activeError = nil
        self.activeTaskFailed = false
    }

    private mutating func recordActiveTaskFailure(_ error: TDSSQLError) -> ConnectionAction {
        guard let activeTask, !self.activeTaskFailed else {
            return .wait
        }
        var error = error
        error.query = activeTask.query
        self.activeError = error
        self.activeTaskFailed = true

        switch activeTask {
        case .sqlBatch(_, let promise, _), .rpc(_, let promise, _),
            .transactionManager(_, let promise), .bulkLoad(_, let promise):
            promise.fail(error)
        case .sqlBatchRows(_, let promise, _), .rpcRows(_, let promise, _):
            if self.activeRowStreamStarted {
                self.activeRowStreamStarted = false
                return .failActiveRowStream(error)
            } else {
                promise.fail(error)
            }
        case .ping(let promise):
            promise.fail(error)
        case .attention(let promise):
            promise.fail(error)
        }
        return .wait
    }

    private mutating func unexpectedTokenReceived(_ tokenName: String) -> ConnectionAction {
        self.closeConnectionAndCleanup(
            .connectionError(
                underlying: UnexpectedTokenWithoutActiveRequest(
                    tokenName: tokenName,
                    state: self.state
                )
            )
        )
    }

    private mutating func start(_ task: TDSTask) -> ConnectionAction {
        switch task {
        case .attention(let promise):
            self.activeTask = nil
            self.state = .loggedIn
            return .succeedCancel(promise)
        case .sqlBatch, .rpc, .transactionManager, .bulkLoad, .sqlBatchRows, .rpcRows, .ping:
            self.state = .sentClientRequest
            self.activeTask = task
            self.clearActiveResult()
            let requestContext = TDSRequestContext(task: task)
            self.activeRequest = StatementStateMachine(
                context: requestContext,
                debugLog: self.debugLog
            )
            self.debug("starting task=\(self.taskDescription(task))")
            switch requestContext.frontendMessage {
            case .sqlBatch(let sql):
                return .sendSQLBatch(sql)
            case .rpc(let rpc):
                return .sendRPC(rpc)
            case .transactionManager(let request):
                return .sendTransactionManagerRequest(request)
            case .bulkLoad(let request):
                return .sendBulkLoad(request)
            case .ping:
                return .sendSQLBatch("SELECT 1")
            }
        }
    }

    private mutating func connectionAction(
        from requestAction: StatementStateMachine.Action
    ) -> ConnectionAction {
        switch requestAction {
        case .wait:
            return .wait
        case .read:
            return .read
        case .forwardRows(let rows):
            return .forwardRows(rows)
        case .forwardRowsAndComplete(let rows):
            return .forwardRowsAndComplete(rows)
        case .succeedRowStream(let promise, let columns):
            return .succeedRowStream(promise, columns)
        case .forwardStreamComplete:
            return .finishActiveRowStream
        case .forwardStreamError(let error):
            return .failActiveRowStream(error)
        case .succeedQuery(let promise, let result):
            self.activeTask = nil
            self.activeRequest = nil
            self.state = .loggedIn
            return .succeedQuery(promise, result)
        case .succeedTask(let promise):
            self.activeTask = nil
            self.activeRequest = nil
            self.state = .loggedIn
            return .succeedTask(promise)
        case .completeFailedQuery:
            self.activeTask = nil
            self.activeRequest = nil
            self.state = .loggedIn
            return .completeFailedQuery
        case .completeRowStreamQuery(let emptyStreamPromise):
            self.activeTask = nil
            self.activeRequest = nil
            self.state = .loggedIn
            return .completeRowStreamQuery(emptyStreamPromise)
        case .completeRowStreamQueryWithRows(let rows, let emptyStreamPromise):
            self.activeTask = nil
            self.activeRequest = nil
            self.state = .loggedIn
            if rows.isEmpty {
                return .completeRowStreamQuery(emptyStreamPromise)
            }
            return .forwardRowsAndCompleteQuery(rows, emptyStreamPromise)
        }
    }

    private mutating func connectionAction(
        from authenticationAction: AuthenticationStateMachine.Action
    ) -> ConnectionAction {
        switch authenticationAction {
        case .wait:
            return .wait
        case .sendPreloginRequest:
            return .sendPreloginRequest
        case .startTLS(let removeAfterLogin):
            return .startTLS(removeAfterLogin: removeAfterLogin)
        case .sendLoginRequest:
            return .sendLoginRequest
        case .fireAuthenticationChallenge(let challenge):
            return .fireAuthenticationChallenge(challenge)
        case .authenticated(let loginAck, let removeTLS):
            return .authenticated(loginAck, removeTLS: removeTLS)
        case .failAuthentication(let error):
            return self.closeConnectionAndCleanup(error)
        }
    }

    private mutating func finishCurrentResultSet(_ done: TDSBackendMessage.Done) {
        let rowsAffected = done.status.contains(.count) ? done.rowCount : nil
        guard !self.activeColumns.isEmpty || !self.activeRows.isEmpty || rowsAffected != nil else {
            return
        }

        self.activeResultSets.append(
            .init(
                columns: self.activeColumns,
                rows: self.activeRows,
                rowsAffected: rowsAffected,
                offsets: self.activeOffsets,
                alternateResultSets: self.activeAlternateResultSets
            ))
        self.activeColumns = []
        self.activeRows = []
        self.activeOffsets = []
        self.activeAlternateResultSets = []
        self.activeTableNames = []
    }

    private func makeQueryResult() -> TDSQueryResult {
        let firstResultSet =
            self.activeResultSets.first
            ?? .init(
                columns: [],
                rows: [],
                rowsAffected: nil
            )
        return .init(
            columns: firstResultSet.columns,
            rows: firstResultSet.rows,
            rowsAffected: firstResultSet.rowsAffected,
            offsets: firstResultSet.offsets,
            alternateResultSets: firstResultSet.alternateResultSets,
            returnStatus: self.activeReturnStatus,
            outputParameters: self.activeOutputParameters,
            resultSets: self.activeResultSets
        )
    }

    private mutating func finishRowStreamIfNeeded(
        promise: EventLoopPromise<TDSRowStream>
    ) -> EventLoopPromise<TDSRowStream>? {
        if self.activeRowStreamStarted {
            self.debug("finishing row stream at final DONE")
            self.activeRowStreamStarted = false
            return nil
        } else if !self.activeRowStreamStarted {
            self.debug("row stream task completed without COLMETADATA; succeeding empty stream")
            return promise
        }
        return nil
    }

    private func taskDescription(_ task: TDSTask) -> String {
        switch task {
        case .sqlBatch:
            "sqlBatch"
        case .rpc:
            "rpc"
        case .transactionManager:
            "transactionManager"
        case .bulkLoad:
            "bulkLoad"
        case .sqlBatchRows:
            "sqlBatchRows"
        case .rpcRows:
            "rpcRows"
        case .ping:
            "ping"
        case .attention:
            "attention"
        }
    }

    private func debug(_ message: @autoclosure () -> String) {
        self.debugLog?(message())
    }
}

extension StatementStateMachine.DoneTokenKind {
    init(_ kind: ConnectionStateMachine.DoneTokenKind) {
        switch kind {
        case .done:
            self = .done
        case .doneProc:
            self = .doneProc
        case .doneInProc:
            self = .doneInProc
        }
    }
}

private struct UnexpectedTokenWithoutActiveRequest: Error, CustomStringConvertible {
    var tokenName: String
    var state: ConnectionStateMachine.State

    var description: String {
        "Unexpected token '\(self.tokenName)' while no SQL request is active in state '\(self.state)'."
    }
}

extension TDSColumn {
    init(_ column: TDSBackendMessage.ColMetadata.Column) {
        self.init(
            name: column.name,
            dataType: column.typeInfo.dataType,
            metadata: .init(
                userType: column.userType,
                flags: column.flags,
                length: column.typeInfo.length,
                collation: column.typeInfo.collation,
                precision: column.typeInfo.precision,
                scale: column.typeInfo.scale,
                tableName: column.typeInfo.tableName,
                udtInfo: column.typeInfo.udtInfo.map(TDSColumn.Metadata.UDTInfo.init),
                xmlInfo: column.typeInfo.xmlInfo.map(TDSColumn.Metadata.XMLInfo.init)
            )
        )
    }

    init(_ column: TDSBackendMessage.AltMetadata.Column) {
        self.init(
            name: column.name,
            dataType: column.typeInfo.dataType,
            metadata: .init(
                userType: column.userType,
                flags: column.flags,
                length: column.typeInfo.length,
                collation: column.typeInfo.collation,
                precision: column.typeInfo.precision,
                scale: column.typeInfo.scale,
                tableName: column.typeInfo.tableName,
                alternateOperation: column.op,
                alternateOperand: column.operand,
                udtInfo: column.typeInfo.udtInfo.map(TDSColumn.Metadata.UDTInfo.init),
                xmlInfo: column.typeInfo.xmlInfo.map(TDSColumn.Metadata.XMLInfo.init)
            )
        )
    }
}

extension TDSColumn.Metadata.UDTInfo {
    init(_ info: TDSBackendMessage.ColMetadata.UDTInfo) {
        self.init(
            databaseName: info.databaseName,
            schemaName: info.schemaName,
            typeName: info.typeName,
            assemblyQualifiedName: info.assemblyQualifiedName
        )
    }
}

extension TDSColumn.Metadata.XMLInfo {
    init(_ info: TDSBackendMessage.ColMetadata.XMLInfo) {
        self.init(
            databaseName: info.databaseName,
            owningSchema: info.owningSchema,
            schemaCollection: info.schemaCollection
        )
    }
}

extension ConnectionStateMachine {
    /// While the state machine logic above is great, there is a downside to having all of the state machine
    /// data in associated data on enumerations: any modification of that data will trigger copy on write
    /// for heap-allocated data. That means that for _every operation on the state machine_ we will CoW
    /// our underlying state, which is not good.
    ///
    /// The way we can avoid this is by using this helper function. It will temporarily set state to a value with
    /// no associated data, before attempting the body of the function. It will also verify that the state
    /// machine never remains in this bad state.
    ///
    /// A key note here is that all callers must ensure that they return to a good state before they exit.
    private mutating func avoidingStateMachineCoW(
        _ body: (inout ConnectionStateMachine) -> ConnectionAction
    ) -> ConnectionAction {
        self.state = .modifying
        defer {
            assert(!self.isModifying)
        }

        return body(&self)
    }

    private var isModifying: Bool {
        if case .modifying = self.state {
            return true
        } else {
            return false
        }
    }
}
