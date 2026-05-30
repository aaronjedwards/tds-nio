import NIOCore

/// A consumable stream of ``TDSRow`` values.
///
/// The stream can be consumed exactly once through ``asyncSequence()``, ``all()``,
/// or ``onRow(_:)``. It can either start with a completed buffer or be fed rows
/// by the channel handler as ROW tokens arrive.
public final class TDSRowStream: @unchecked Sendable {
    private enum BufferState {
        case streaming([TDSRow])
        case finished([TDSRow])
        case failure(Error)

        var description: String {
            switch self {
            case .streaming(let rows):
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
        case waitingForAll(rows: [TDSRow], EventLoopPromise<[TDSRow]>)
        case iteratingRows((TDSRow) throws -> Void, EventLoopPromise<Void>)
        case asyncSequence(AsyncThrowingStream<TDSRow, Error>.Continuation)
        case consumed(Result<Void, Error>)
    }

    private let eventLoop: EventLoop
    private let onCancel: (@Sendable () -> Void)?
    private let debugLog: (@Sendable (String) -> Void)?
    private var downstreamState: DownstreamState
    private var receivedRows = 0
    private var yieldedRows = 0

    public init(rows: [TDSRow], eventLoop: EventLoop) {
        self.eventLoop = eventLoop
        self.onCancel = nil
        self.debugLog = nil
        self.downstreamState = .waitingForConsumer(.finished(rows))
    }

    init(
        columns: [TDSColumn],
        eventLoop: EventLoop,
        onCancel: (@Sendable () -> Void)? = nil,
        debugLog: (@Sendable (String) -> Void)? = nil
    ) {
        self.eventLoop = eventLoop
        self.onCancel = onCancel
        self.debugLog = debugLog
        self.downstreamState = .waitingForConsumer(.streaming([]))
        self.debug("created live row stream columns=\(columns.count)")
    }

    public convenience init(resultSet: TDSResultSet, eventLoop: EventLoop) {
        self.init(rows: resultSet.rows, eventLoop: eventLoop)
    }

    public func asyncSequence() -> TDSRowSequence {
        let stream = AsyncThrowingStream<TDSRow, Error> { continuation in
            switch self.downstreamState {
            case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
                self.asyncSequence0(continuation)
            case .waitingForConsumer(.streaming):
                continuation.onTermination = { @Sendable [weak self] _ in
                    guard let self else { return }
                    self.cancelFromConsumer()
                }

                self.eventLoop.execute {
                    self.asyncSequence0(continuation)
                }
            case .waitingForAll, .iteratingRows, .asyncSequence, .consumed:
                preconditionFailure("TDSRowStream can only be consumed once.")
            }
        }
        return TDSRowSequence(stream)
    }

    func cancelFromConsumer() {
        self.eventLoop.execute {
            if case .asyncSequence = self.downstreamState {
                self.debug("consumer terminated async sequence; sending attention receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows)")
                self.downstreamState = .consumed(.success(()))
                self.onCancel?()
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
        self.eventLoop.preconditionInEventLoop()
        self.receivedRows += 1
        self.debugRowProgress("received row")

        switch self.downstreamState {
        case .waitingForConsumer(.streaming(var buffer)):
            buffer.append(row)
            self.downstreamState = .waitingForConsumer(.streaming(buffer))
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
            preconditionFailure("Received row after stream completion.")
        case .waitingForAll(var rows, let promise):
            rows.append(row)
            self.downstreamState = .waitingForAll(rows: rows, promise)
        case .iteratingRows(let onRow, let promise):
            do {
                try onRow(row)
                self.yieldedRows += 1
                self.debugYieldProgress("delivered row to onRow")
            } catch {
                self.debug("onRow threw; sending attention error=\(String(reflecting: error))")
                self.downstreamState = .consumed(.failure(error))
                promise.fail(error)
                self.onCancel?()
            }
        case .asyncSequence(let continuation):
            let result = continuation.yield(row)
            self.yieldedRows += 1
            self.debugYieldProgress("yielded row to async sequence result=\(result)")
        case .consumed:
            self.debug("dropping row after stream consumed receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows)")
            break
        }
    }

    func receive(completion result: Result<Void, Error>) {
        self.eventLoop.preconditionInEventLoop()
        self.debug("received completion result=\(result) state=\(self.stateDescription) receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows)")

        switch self.downstreamState {
        case .waitingForConsumer(.streaming(let buffer)):
            switch result {
            case .success:
                self.downstreamState = .waitingForConsumer(.finished(buffer))
            case .failure(let error):
                self.downstreamState = .waitingForConsumer(.failure(error))
            }
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
            preconditionFailure("Received duplicate stream completion.")
        case .waitingForAll(let rows, let promise):
            self.downstreamState = .consumed(result)
            switch result {
            case .success:
                self.debug("succeeding all() rows=\(rows.count)")
                promise.succeed(rows)
            case .failure(let error):
                promise.fail(error)
            }
        case .iteratingRows(_, let promise):
            self.downstreamState = .consumed(result)
            switch result {
            case .success:
                promise.succeed(())
            case .failure(let error):
                promise.fail(error)
            }
        case .asyncSequence(let continuation):
            self.downstreamState = .consumed(result)
            switch result {
            case .success:
                self.debug("finishing async sequence receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows)")
                continuation.finish()
            case .failure(let error):
                self.debug("finishing async sequence with error=\(String(reflecting: error))")
                continuation.finish(throwing: error)
            }
        case .consumed:
            break
        }
    }

    private func asyncSequence0(_ continuation: AsyncThrowingStream<TDSRow, Error>.Continuation) {
        switch self.downstreamState {
        case .waitingForConsumer(.streaming(let buffer)):
            self.eventLoop.preconditionInEventLoop()
            self.debug("creating live async sequence bufferedRows=\(buffer.count)")
            for row in buffer {
                let result = continuation.yield(row)
                self.yieldedRows += 1
                self.debugYieldProgress("yielded buffered row result=\(result)")
            }
            self.debug("async sequence now waiting for live rows")
            self.downstreamState = .asyncSequence(continuation)
        case .waitingForConsumer(.finished(let buffer)):
            self.debug("creating finished async sequence bufferedRows=\(buffer.count)")
            for row in buffer {
                let result = continuation.yield(row)
                self.yieldedRows += 1
                self.debugYieldProgress("yielded buffered row result=\(result)")
            }
            continuation.finish()
            self.downstreamState = .consumed(.success(()))
        case .waitingForConsumer(.failure(let error)):
            self.debug("creating failed async sequence error=\(String(reflecting: error))")
            continuation.finish(throwing: error)
            self.downstreamState = .consumed(.failure(error))
        case .waitingForAll, .iteratingRows, .asyncSequence, .consumed:
            preconditionFailure("TDSRowStream can only be consumed once.")
        }
    }

    private func all0() -> EventLoopFuture<[TDSRow]> {
        switch self.downstreamState {
        case .waitingForConsumer(.streaming(let buffer)):
            let promise = self.eventLoop.makePromise(of: [TDSRow].self)
            self.debug("all() waiting for live rows bufferedRows=\(buffer.count)")
            self.downstreamState = .waitingForAll(rows: buffer, promise)
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
        case .waitingForConsumer(.streaming(let buffer)):
            do {
                for row in buffer {
                    try onRow(row)
                }
            } catch {
                self.downstreamState = .consumed(.failure(error))
                self.onCancel?()
                return self.eventLoop.makeFailedFuture(error)
            }

            let promise = self.eventLoop.makePromise(of: Void.self)
            self.debug("onRow waiting for live rows bufferedRows=\(buffer.count)")
            self.downstreamState = .iteratingRows(onRow, promise)
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
        case .waitingForAll(let rows, _):
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
            self.debug("\(message()) receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows) state=\(self.stateDescription)")
        }
    }

    private func debugYieldProgress(_ message: @autoclosure () -> String) {
        let count = self.yieldedRows
        if count <= 5 || count.isMultiple(of: 1000) {
            self.debug("\(message()) receivedRows=\(self.receivedRows) yieldedRows=\(self.yieldedRows) state=\(self.stateDescription)")
        }
    }
}
