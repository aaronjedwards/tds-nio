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

import NIOCore

struct StatementStateMachine {
    enum DoneTokenKind {
        case done
        case doneProc
        case doneInProc
    }

    enum Action {
        case wait
        case read
        case forwardRows([TDSRow])
        case forwardRowsAndComplete([TDSRow])
        case succeedRowStream(EventLoopPromise<TDSRowStream>, [TDSColumn])
        case forwardStreamComplete
        case forwardStreamError(TDSSQLError)
        case succeedQuery(EventLoopPromise<TDSQueryResult>, TDSQueryResult)
        case succeedTask(EventLoopPromise<Void>)
        case completeFailedQuery
        case completeRowStreamQuery(EventLoopPromise<TDSRowStream>?)
        case completeRowStreamQueryWithRows([TDSRow], EventLoopPromise<TDSRowStream>?)
    }

    private let context: TDSRequestContext
    private var activeColumns: [TDSColumn] = []
    private var activeRows: [TDSRow] = []
    private var activeRowStreamStarted = false
    private var rowStreamStateMachine: RowStreamStateMachine?
    private var activeResultSets: [TDSResultSet] = []
    private var activeOffsets: [TDSOffset] = []
    private var activeAlternateResultSets: [TDSAlternateResultSet] = []
    private var activeReturnStatus: Int32?
    private var activeOutputParameters: [TDSOutputParameter] = []
    private var activeTableNames: [String] = []
    private var activeError: TDSSQLError?
    private var activeTaskFailed = false
    private let debugLog: (@Sendable (String) -> Void)?

    init(context: TDSRequestContext, debugLog: (@Sendable (String) -> Void)? = nil) {
        self.context = context
        self.debugLog = debugLog
    }

    mutating func doneReceived(
        _ done: TDSBackendMessage.Done,
        tokenKind: DoneTokenKind
    ) -> Action {
        self.debug(
            "DONE token kind=\(tokenKind) statusRaw=\(done.status.rawValue) rowCount=\(done.rowCount) "
                + "rowStreamStarted=\(self.activeRowStreamStarted) "
                + "activeRows=\(self.activeRows.count) activeColumns=\(self.activeColumns.count)"
        )
        if done.status.contains(.error) || done.status.contains(.serverError) {
            let action = self.recordFailure(
                .server("Server completed the request with a DONE error status.")
            )
            if case .wait = action {} else {
                return action
            }
        }

        if done.status.contains(.more) || tokenKind == .doneInProc {
            if self.activeRowStreamStarted {
                self.activeRowStreamStarted = false
                let rows = self.rowStreamStateMachine?.end() ?? []
                self.finishCurrentResultSet(done)
                if rows.isEmpty {
                    return .forwardStreamComplete
                }
                return .forwardRowsAndComplete(rows)
            }
            self.finishCurrentResultSet(done)
            return .wait
        }

        if self.activeError != nil {
            self.clearActiveResult()
            return .completeFailedQuery
        }

        self.finishCurrentResultSet(done)
        let result = self.makeQueryResult()

        switch self.context.resultMode {
        case .bufferedQueryResult:
            guard let promise = self.context.queryResultPromise else {
                return .wait
            }
            self.clearActiveResult()
            return .succeedQuery(promise, result)
        case .rowStream:
            let rows = self.activeRowStreamStarted ? self.rowStreamStateMachine?.end() ?? [] : []
            let emptyStreamPromise = self.finishRowStreamIfNeeded()
            self.clearActiveResult()
            if !rows.isEmpty {
                return .completeRowStreamQueryWithRows(rows, emptyStreamPromise)
            }
            return .completeRowStreamQuery(emptyStreamPromise)
        case .void:
            guard let promise = self.context.voidPromise else {
                return .wait
            }
            self.clearActiveResult()
            return .succeedTask(promise)
        }
    }

    mutating func backendErrorReceived(_ error: TDSBackendMessage.InfoError) -> Action {
        var sqlError = TDSSQLError.server(error)
        sqlError.query = self.context.query
        return self.recordFailure(sqlError)
    }

    mutating func colMetadataReceived(_ metadata: TDSBackendMessage.ColMetadata) -> Action {
        self.activeColumns = metadata.columns.map(TDSColumn.init)
        self.activeTableNames = []
        if self.context.resultMode == .rowStream,
            let promise = self.context.rowStreamPromise,
            !self.activeRowStreamStarted
        {
            self.activeRowStreamStarted = true
            self.rowStreamStateMachine = .init()
            return .succeedRowStream(promise, self.activeColumns)
        }
        return .wait
    }

    mutating func tabNameReceived(_ tabName: TDSBackendMessage.TabName) -> Action {
        self.activeTableNames = tabName.tableNames
        return .wait
    }

    mutating func colInfoReceived(_ colInfo: TDSBackendMessage.ColInfo) -> Action {
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

    mutating func orderReceived(_ order: TDSBackendMessage.Order) -> Action {
        for columnNumber in order.columnNumbers {
            let index = Int(columnNumber) - 1
            guard index >= 0, index < self.activeColumns.count else {
                continue
            }
            self.activeColumns[index].metadata.isOrderBy = true
        }
        return .wait
    }

    mutating func offsetReceived(_ offset: TDSBackendMessage.Offset) -> Action {
        self.activeOffsets.append(.init(identifier: offset.identifier, offset: offset.offset))
        return .wait
    }

    mutating func dataClassificationReceived(
        _ dataClassification: TDSBackendMessage.DataClassification
    ) -> Action {
        for (index, column) in dataClassification.columns.enumerated()
        where index < self.activeColumns.count {
            self.activeColumns[index].metadata.sensitivityClassifications = column.properties
                .compactMap { property in
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

    mutating func altMetadataReceived(_ altMetadata: TDSBackendMessage.AltMetadata) -> Action {
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

    mutating func altRowReceived(_ altRow: TDSBackendMessage.AltRow) -> Action {
        guard let index = self.activeAlternateResultSets.firstIndex(where: { $0.id == altRow.id })
        else {
            return .wait
        }

        let columns = self.activeAlternateResultSets[index].columns
        self.activeAlternateResultSets[index].rows.append(
            TDSRow(columns: columns, values: altRow.values))
        return .wait
    }

    mutating func rowReceived(_ row: TDSBackendMessage.Row) -> Action {
        let row = TDSRow(columns: self.activeColumns, values: row.values)
        if self.activeRowStreamStarted {
            self.rowStreamStateMachine?.receivedRow(row)
        } else if self.context.resultMode == .rowStream {
            self.debug("dropping row for row-stream request before stream exists")
        } else {
            self.activeRows.append(row)
        }
        return .wait
    }

    mutating func returnStatusReceived(_ status: Int32) -> Action {
        self.activeReturnStatus = status
        return .wait
    }

    mutating func returnValueReceived(_ returnValue: TDSBackendMessage.ReturnValue) -> Action {
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

    mutating func channelReadComplete() -> Action {
        guard var rowStreamStateMachine = self.rowStreamStateMachine,
            let rows = rowStreamStateMachine.channelReadComplete()
        else {
            return .wait
        }
        self.rowStreamStateMachine = rowStreamStateMachine
        return .forwardRows(rows)
    }

    mutating func requestRows() -> Action {
        guard var rowStreamStateMachine = self.rowStreamStateMachine else {
            return .wait
        }
        let action = rowStreamStateMachine.requestRows()
        self.rowStreamStateMachine = rowStreamStateMachine
        return Self.action(from: action)
    }

    mutating func read() -> Action {
        guard var rowStreamStateMachine = self.rowStreamStateMachine else {
            return .read
        }
        let action = rowStreamStateMachine.read()
        self.rowStreamStateMachine = rowStreamStateMachine
        return Self.action(from: action)
    }

    mutating func fail(_ error: TDSSQLError) -> Action {
        if self.activeRowStreamStarted {
            self.activeRowStreamStarted = false
            _ = self.rowStreamStateMachine?.fail()
            return .forwardStreamError(error)
        }
        self.context.fail(error)
        return .wait
    }

    private static func action(from action: RowStreamStateMachine.Action) -> Action {
        switch action {
        case .read:
            return .read
        case .wait:
            return .wait
        }
    }

    private mutating func clearActiveResult() {
        self.activeColumns = []
        self.activeRows = []
        self.activeRowStreamStarted = false
        self.rowStreamStateMachine = nil
        self.activeResultSets = []
        self.activeOffsets = []
        self.activeAlternateResultSets = []
        self.activeReturnStatus = nil
        self.activeOutputParameters = []
        self.activeTableNames = []
        self.activeError = nil
        self.activeTaskFailed = false
    }

    private mutating func recordFailure(_ error: TDSSQLError) -> Action {
        guard !self.activeTaskFailed else {
            return .wait
        }
        var error = error
        error.query = self.context.query
        self.activeError = error
        self.activeTaskFailed = true

        if self.context.resultMode == .rowStream, self.activeRowStreamStarted {
            self.activeRowStreamStarted = false
            _ = self.rowStreamStateMachine?.fail()
            return .forwardStreamError(error)
        }

        self.context.fail(error)
        return .wait
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

    private mutating func finishRowStreamIfNeeded() -> EventLoopPromise<TDSRowStream>? {
        guard let promise = self.context.rowStreamPromise else {
            return nil
        }
        if self.activeRowStreamStarted {
            self.activeRowStreamStarted = false
            return nil
        }
        return promise
    }

    private func debug(_ message: @autoclosure () -> String) {
        self.debugLog?(message())
    }
}
