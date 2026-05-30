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
        case loggedIn
        case sentClientRequest
        case sentAttention
        case routingComplete
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
        /// Close connection because of an error state. Fail all tasks with the provided error.
        case closeConnectionAndCleanup(CleanUpContext)
        case sendPreloginRequest
        case startTLS(removeAfterLogin: Bool)
        case sendLoginRequest
        case fireAuthenticationChallenge(TDSAuthenticationChallenge)
        case authenticated(TDSBackendMessage.LoginAck, removeTLS: Bool)
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
    private var activeRequest: TDSRequestStateMachine?
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
        self.state = .closed
        return self.closeConnectionAndCleanup(
            .connectionError(underlying: ChannelError.ioOnClosedChannel),
            closePromise: nil
        )
    }

    mutating func errorHappened(_ error: TDSSQLError) -> ConnectionAction {
        self.closeConnectionAndCleanup(error)
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
            guard tokenKind != .doneInProc else {
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

    mutating func backendErrorReceived(_ error: TDSBackendMessage.InfoError) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.backendErrorReceived(error)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        let sqlError = TDSSQLError.server(error)
        return self.closeConnectionAndCleanup(sqlError)
    }

    mutating func colMetadataReceived(_ metadata: TDSBackendMessage.ColMetadata) -> ConnectionAction
    {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.colMetadataReceived(metadata)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        self.activeColumns = metadata.columns.map(TDSColumn.init)
        self.debug(
            "COLMETADATA columns=\(self.activeColumns.count) names=\(self.activeColumns.map(\.name)) "
                + "task=\(self.activeTaskDescription) rowStreamStarted=\(self.activeRowStreamStarted)"
        )
        self.activeTableNames = []
        if let activeTask, !self.activeRowStreamStarted {
            switch activeTask {
            case .sqlBatchRows(_, let promise, _), .rpcRows(_, let promise, _):
                self.activeRowStreamStarted = true
                self.debug("succeeding row stream promise")
                return .succeedRowStream(promise, self.activeColumns)
            case .sqlBatch, .rpc, .transactionManager, .bulkLoad, .ping, .attention:
                break
            }
        }
        return .wait
    }

    mutating func tabNameReceived(_ tabName: TDSBackendMessage.TabName) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.tabNameReceived(tabName)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        self.activeTableNames = tabName.tableNames
        return .wait
    }

    mutating func colInfoReceived(_ colInfo: TDSBackendMessage.ColInfo) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.colInfoReceived(colInfo)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        for columnInfo in colInfo.columns {
            let index = Int(columnInfo.columnNumber) - 1
            guard index >= 0, index < self.activeColumns.count else {
                continue
            }

            self.activeColumns[index].metadata.tableNumber = columnInfo.tableNumber
            if columnInfo.tableNumber > 0 {
                let tableIndex = Int(columnInfo.tableNumber) - 1
                if tableIndex >= 0, tableIndex < self.activeTableNames.count {
                    self.activeColumns[index].metadata.baseTableName =
                        self.activeTableNames[tableIndex]
                }
            }
            self.activeColumns[index].metadata.baseColumnName = columnInfo.baseColumnName
            self.activeColumns[index].metadata.isExpression = columnInfo.status.contains(
                .expression)
            self.activeColumns[index].metadata.isKey = columnInfo.status.contains(.key)
            self.activeColumns[index].metadata.isHidden = columnInfo.status.contains(.hidden)
        }
        return .wait
    }

    mutating func orderReceived(_ order: TDSBackendMessage.Order) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.orderReceived(order)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        for columnNumber in order.columnNumbers {
            let index = Int(columnNumber) - 1
            guard index >= 0, index < self.activeColumns.count else {
                continue
            }
            self.activeColumns[index].metadata.isOrderBy = true
        }
        return .wait
    }

    mutating func offsetReceived(_ offset: TDSBackendMessage.Offset) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.offsetReceived(offset)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        self.activeOffsets.append(.init(identifier: offset.identifier, offset: offset.offset))
        return .wait
    }

    mutating func dataClassificationReceived(
        _ dataClassification: TDSBackendMessage.DataClassification
    ) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.dataClassificationReceived(dataClassification)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        for (index, column) in dataClassification.columns.enumerated()
        where index < self.activeColumns.count {
            self.activeColumns[index].metadata.sensitivityClassifications = column.properties
                .compactMap {
                    property in
                    let labelIndex = Int(property.labelIndex)
                    let informationTypeIndex = Int(property.informationTypeIndex)
                    guard
                        labelIndex >= 0, labelIndex < dataClassification.labels.count,
                        informationTypeIndex >= 0,
                        informationTypeIndex < dataClassification.informationTypes.count
                    else {
                        return nil
                    }

                    let label = dataClassification.labels[labelIndex]
                    let informationType = dataClassification.informationTypes[informationTypeIndex]
                    return .init(
                        labelName: label.name,
                        labelID: label.id,
                        informationTypeName: informationType.name,
                        informationTypeID: informationType.id,
                        rank: property.rank
                    )
                }
        }
        return .wait
    }

    mutating func altMetadataReceived(_ altMetadata: TDSBackendMessage.AltMetadata)
        -> ConnectionAction
    {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.altMetadataReceived(altMetadata)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        let alternateResultSet = TDSAlternateResultSet(
            id: altMetadata.id,
            byColumns: altMetadata.byColumns,
            columns: altMetadata.columns.map(TDSColumn.init)
        )

        if let index = self.activeAlternateResultSets.firstIndex(where: { $0.id == altMetadata.id })
        {
            self.activeAlternateResultSets[index] = alternateResultSet
        } else {
            self.activeAlternateResultSets.append(alternateResultSet)
        }
        return .wait
    }

    mutating func altRowReceived(_ altRow: TDSBackendMessage.AltRow) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.altRowReceived(altRow)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        guard let index = self.activeAlternateResultSets.firstIndex(where: { $0.id == altRow.id })
        else {
            return .wait
        }

        let columns = self.activeAlternateResultSets[index].columns
        self.activeAlternateResultSets[index].rows.append(
            TDSRow(columns: columns, values: altRow.values))
        return .wait
    }

    mutating func rowReceived(_ row: TDSBackendMessage.Row) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.rowReceived(row)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        let row = TDSRow(columns: self.activeColumns, values: row.values)
        if self.activeRowStreamStarted {
            return .forwardRow(row)
        } else if self.isActiveRowStreamTask {
            self.debug("dropping row for row-stream task before stream exists")
            return .wait
        } else {
            self.activeRows.append(row)
        }
        return .wait
    }

    mutating func returnStatusReceived(_ status: Int32) -> ConnectionAction {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.returnStatusReceived(status)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        self.activeReturnStatus = status
        return .wait
    }

    mutating func returnValueReceived(_ returnValue: TDSBackendMessage.ReturnValue)
        -> ConnectionAction
    {
        if var activeRequest = self.activeRequest {
            let action = activeRequest.returnValueReceived(returnValue)
            self.activeRequest = activeRequest
            return self.connectionAction(from: action)
        }
        self.activeOutputParameters.append(
            .init(
                ordinal: returnValue.ordinal,
                name: returnValue.name,
                status: returnValue.status,
                userType: returnValue.userType,
                flags: returnValue.flags,
                dataType: returnValue.typeInfo.dataType,
                metadata: TDSColumn.Metadata(
                    userType: returnValue.userType,
                    flags: returnValue.flags,
                    length: returnValue.typeInfo.length,
                    collation: returnValue.typeInfo.collation,
                    precision: returnValue.typeInfo.precision,
                    scale: returnValue.typeInfo.scale,
                    tableName: returnValue.typeInfo.tableName,
                    udtInfo: returnValue.typeInfo.udtInfo.map(TDSColumn.Metadata.UDTInfo.init),
                    xmlInfo: returnValue.typeInfo.xmlInfo.map(TDSColumn.Metadata.XMLInfo.init)
                ),
                value: returnValue.value
            ))
        return .wait
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
        default:
            let error = TDSSQLError.connectionError(underlying: ChannelError.ioOnClosedChannel)
            task.fail(error)
            promise?.fail(error)
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
        closePromise: EventLoopPromise<Void>? = nil
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
        let action: ConnectionAction.CleanUpContext.Action =
            self.state == .closed ? .fireChannelInactive : .close
        self.state = .closed
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
            self.activeRequest = TDSRequestStateMachine(
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
        from requestAction: TDSRequestStateMachine.Action
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

    private var isActiveRowStreamTask: Bool {
        guard let activeTask else {
            return false
        }
        switch activeTask {
        case .sqlBatchRows, .rpcRows:
            return true
        case .sqlBatch, .rpc, .transactionManager, .bulkLoad, .ping, .attention:
            return false
        }
    }

    private var activeTaskDescription: String {
        self.activeTask.map(self.taskDescription) ?? "nil"
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

extension TDSRequestStateMachine.DoneTokenKind {
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
