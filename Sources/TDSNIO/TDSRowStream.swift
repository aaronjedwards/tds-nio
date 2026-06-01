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

public import NIOCore

/// A consumable stream of ``TDSRow`` values.
///
/// The stream can be consumed exactly once through ``asyncSequence()``, ``all()``,
/// or ``onRow(_:)``. It can either start with a completed buffer or be fed rows
/// by the channel handler as ROW tokens arrive.
public final class TDSRowStream: @unchecked Sendable {
    private typealias AsyncSequenceSource = NIOThrowingAsyncSequenceProducer<
        TDSRow, Error, TDSAdaptiveRowBuffer, TDSRowStream
    >.Source

    private enum BufferState {
        case streaming([TDSRow], TDSRowsDataSource)
        case finished([TDSRow])
        case failure(Error)

        var description: String {
            switch self {
            case .streaming(let rows, _):
                "streaming(buffer: \(rows.count))"
            case .finished(let rows):
                "finished(buffer: \(rows.count))"
            case .failure(let error):
                "failure(\(String(reflecting: error)))"
            }
        }
    }

    private enum DownstreamState {
        case waitingForConsumer(BufferState)
        case waitingForAll(rows: [TDSRow], EventLoopPromise<[TDSRow]>, TDSRowsDataSource)
        case iteratingRows((TDSRow) throws -> Void, EventLoopPromise<Void>, TDSRowsDataSource)
        case asyncSequence(AsyncSequenceSource, TDSRowsDataSource)
        case consumed(Result<Void, Error>)
    }

    private let eventLoop: EventLoop
    private let debugLog: (@Sendable (String) -> Void)?
    private var downstreamState: DownstreamState
    private var receivedRows = 0
    private var yieldedRows = 0

    public init(rows: [TDSRow], eventLoop: EventLoop) {
        self.eventLoop = eventLoop
        self.debugLog = nil
        self.downstreamState = .waitingForConsumer(.finished(rows))
    }

    init(
        columns: [TDSColumn],
        eventLoop: EventLoop,
        dataSource: TDSRowsDataSource,
        debugLog: (@Sendable (String) -> Void)? = nil
    ) {
        self.eventLoop = eventLoop
        self.debugLog = debugLog
        self.downstreamState = .waitingForConsumer(.streaming([], dataSource))
        self.debug("created live row stream columns=\(columns.count)")
    }

    public convenience init(resultSet: TDSResultSet, eventLoop: EventLoop) {
        self.init(rows: resultSet.rows, eventLoop: eventLoop)
    }

    public func asyncSequence() -> TDSRowSequence {
        if self.eventLoop.inEventLoop {
            return self.asyncSequence0()
        } else {
            if case .waitingForConsumer(.finished(let rows)) = self.downstreamState {
                self.downstreamState = .consumed(.success(()))
                return TDSRowSequence(rows)
            }
            do {
                return try self.eventLoop.submit {
                    self.asyncSequence0()
                }.wait()
            } catch {
                preconditionFailure("Unable to create TDS row async sequence: \(error)")
            }
        }
    }

    func cancelFromConsumer() {
        self.eventLoop.execute {
            if case .asyncSequence(_, let dataSource) = self.downstreamState {
                self.debug(
                    "consumer terminated async sequence; sending attention receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows)"
                )
                self.downstreamState = .consumed(.failure(CancellationError()))
                dataSource.cancel(for: self)
            } else {
                self.debug("consumer termination ignored state=\(self.stateDescription)")
            }
        }
    }

    public func all() -> EventLoopFuture<[TDSRow]> {
        if self.eventLoop.inEventLoop {
            return self.all0()
        } else {
            return self.eventLoop.flatSubmit {
                self.all0()
            }
        }
    }

    public func onRow(
        _ onRow: @escaping @Sendable (TDSRow) throws -> Void
    ) -> EventLoopFuture<Void> {
        if self.eventLoop.inEventLoop {
            return self.onRow0(onRow)
        } else {
            return self.eventLoop.flatSubmit {
                self.onRow0(onRow)
            }
        }
    }

    func receive(_ row: TDSRow) {
        self.receive([row])
    }

    func receive(_ rows: [TDSRow]) {
        self.eventLoop.preconditionInEventLoop()
        guard !rows.isEmpty else {
            return
        }
        self.receivedRows += rows.count
        self.debugRowProgress("received rows")

        switch self.downstreamState {
        case .waitingForConsumer(.streaming(var buffer, let dataSource)):
            buffer.append(contentsOf: rows)
            self.downstreamState = .waitingForConsumer(.streaming(buffer, dataSource))
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
            preconditionFailure("Received row after stream completion.")
        case .waitingForAll(var bufferedRows, let promise, let dataSource):
            bufferedRows.append(contentsOf: rows)
            self.downstreamState = .waitingForAll(rows: bufferedRows, promise, dataSource)
            dataSource.request(for: self)
        case .iteratingRows(let onRow, let promise, let dataSource):
            do {
                for row in rows {
                    try onRow(row)
                }
                self.yieldedRows += rows.count
                self.debugYieldProgress("delivered row to onRow")
                dataSource.request(for: self)
            } catch {
                self.debug("onRow threw; sending attention error=\(String(reflecting: error))")
                self.downstreamState = .consumed(.failure(error))
                promise.fail(error)
                dataSource.cancel(for: self)
            }
        case .asyncSequence(let source, let dataSource):
            let result = source.yield(contentsOf: rows)
            self.yieldedRows += rows.count
            self.debugYieldProgress("yielded row to async sequence result=\(result)")
            self.executeActionBasedOnYieldResult(result, dataSource: dataSource)
        case .consumed:
            self.debug(
                "dropping row after stream consumed receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows)"
            )
            break
        }
    }

    func receive(completion result: Result<Void, Error>) {
        self.eventLoop.preconditionInEventLoop()
        self.debug(
            "received completion result=\(result) state=\(self.stateDescription) receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows)"
        )

        switch self.downstreamState {
        case .waitingForConsumer(.streaming(let buffer, _)):
            switch result {
            case .success:
                self.downstreamState = .waitingForConsumer(.finished(buffer))
            case .failure(let error):
                self.downstreamState = .waitingForConsumer(.failure(error))
            }
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
            preconditionFailure("Received duplicate stream completion.")
        case .waitingForAll(let rows, let promise, _):
            self.downstreamState = .consumed(result)
            switch result {
            case .success:
                self.debug("succeeding all() rows=\(rows.count)")
                promise.succeed(rows)
            case .failure(let error):
                promise.fail(error)
            }
        case .iteratingRows(_, let promise, _):
            self.downstreamState = .consumed(result)
            switch result {
            case .success:
                promise.succeed(())
            case .failure(let error):
                promise.fail(error)
            }
        case .asyncSequence(let source, _):
            self.downstreamState = .consumed(result)
            switch result {
            case .success:
                self.debug(
                    "finishing async sequence receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows)"
                )
                source.finish()
            case .failure(let error):
                self.debug("finishing async sequence with error=\(String(reflecting: error))")
                source.finish(error)
            }
        case .consumed:
            break
        }
    }

    private func asyncSequence0() -> TDSRowSequence {
        self.eventLoop.preconditionInEventLoop()

        let producer = NIOThrowingAsyncSequenceProducer.makeSequence(
            elementType: TDSRow.self,
            failureType: Error.self,
            backPressureStrategy: TDSAdaptiveRowBuffer(),
            finishOnDeinit: true,
            delegate: self
        )

        let source = producer.source
        switch self.downstreamState {
        case .waitingForConsumer(.streaming(let buffer, let dataSource)):
            self.debug("creating live async sequence bufferedRows=\(buffer.count)")
            let result = source.yield(contentsOf: buffer)
            self.yieldedRows += buffer.count
            self.debug("async sequence now waiting for live rows")
            self.downstreamState = .asyncSequence(source, dataSource)
            self.executeActionBasedOnYieldResult(result, dataSource: dataSource)
        case .waitingForConsumer(.finished(let buffer)):
            self.debug("creating finished async sequence bufferedRows=\(buffer.count)")
            _ = source.yield(contentsOf: buffer)
            source.finish()
            self.downstreamState = .consumed(.success(()))
        case .waitingForConsumer(.failure(let error)):
            self.debug("creating failed async sequence error=\(String(reflecting: error))")
            source.finish(error)
            self.downstreamState = .consumed(.failure(error))
        case .waitingForAll, .iteratingRows, .asyncSequence, .consumed:
            preconditionFailure("TDSRowStream can only be consumed once.")
        }

        return TDSRowSequence(producer.sequence)
    }

    private func all0() -> EventLoopFuture<[TDSRow]> {
        switch self.downstreamState {
        case .waitingForConsumer(.streaming(let buffer, let dataSource)):
            let promise = self.eventLoop.makePromise(of: [TDSRow].self)
            self.debug("all() waiting for live rows bufferedRows=\(buffer.count)")
            self.downstreamState = .waitingForAll(rows: buffer, promise, dataSource)
            dataSource.request(for: self)
            return promise.futureResult
        case .waitingForConsumer(.finished(let buffer)):
            self.downstreamState = .consumed(.success(()))
            return self.eventLoop.makeSucceededFuture(buffer)
        case .waitingForConsumer(.failure(let error)):
            self.downstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        case .waitingForAll, .iteratingRows, .asyncSequence, .consumed:
            preconditionFailure("TDSRowStream can only be consumed once.")
        }
    }

    private func onRow0(
        _ onRow: @escaping @Sendable (TDSRow) throws -> Void
    ) -> EventLoopFuture<Void> {
        switch self.downstreamState {
        case .waitingForConsumer(.streaming(let buffer, let dataSource)):
            do {
                for row in buffer {
                    try onRow(row)
                }
            } catch {
                self.downstreamState = .consumed(.failure(error))
                dataSource.cancel(for: self)
                return self.eventLoop.makeFailedFuture(error)
            }

            let promise = self.eventLoop.makePromise(of: Void.self)
            self.debug("onRow waiting for live rows bufferedRows=\(buffer.count)")
            self.downstreamState = .iteratingRows(onRow, promise, dataSource)
            dataSource.request(for: self)
            return promise.futureResult
        case .waitingForConsumer(.finished(let buffer)):
            do {
                for row in buffer {
                    try onRow(row)
                }
            } catch {
                self.downstreamState = .consumed(.failure(error))
                return self.eventLoop.makeFailedFuture(error)
            }
            self.downstreamState = .consumed(.success(()))
            return self.eventLoop.makeSucceededVoidFuture()
        case .waitingForConsumer(.failure(let error)):
            self.downstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        case .waitingForAll, .iteratingRows, .asyncSequence, .consumed:
            preconditionFailure("TDSRowStream can only be consumed once.")
        }
    }

    private var stateDescription: String {
        switch self.downstreamState {
        case .waitingForConsumer(let bufferState):
            "waitingForConsumer(\(bufferState.description))"
        case .waitingForAll(let rows, _, _):
            "waitingForAll(rows: \(rows.count))"
        case .iteratingRows:
            "iteratingRows"
        case .asyncSequence:
            "asyncSequence"
        case .consumed(let result):
            "consumed(\(result))"
        }
    }

    private func debug(_ message: @autoclosure () -> String) {
        self.debugLog?(message())
    }

    private func debugRowProgress(_ message: @autoclosure () -> String) {
        let count = self.receivedRows
        if count <= 5 || count.isMultiple(of: 1000) {
            self.debug(
                "\(message()) receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows) state=\(self.stateDescription)"
            )
        }
    }

    private func debugYieldProgress(_ message: @autoclosure () -> String) {
        let count = self.yieldedRows
        if count <= 5 || count.isMultiple(of: 1000) {
            self.debug(
                "\(message()) receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows) state=\(self.stateDescription)"
            )
        }
    }

    private func executeActionBasedOnYieldResult(
        _ result: AsyncSequenceSource.YieldResult,
        dataSource: TDSRowsDataSource
    ) {
        switch result {
        case .dropped:
            break
        case .produceMore:
            dataSource.request(for: self)
        case .stopProducing:
            break
        }
    }
}

extension TDSRowStream: NIOAsyncSequenceProducerDelegate {
    public func produceMore() {
        self.eventLoop.execute {
            if case .asyncSequence(_, let dataSource) = self.downstreamState {
                dataSource.request(for: self)
            }
        }
    }

    public func didTerminate() {
        self.cancelFromConsumer()
    }
}

protocol TDSRowsDataSource: AnyObject {
    func request(for stream: TDSRowStream)
    func cancel(for stream: TDSRowStream)
}
