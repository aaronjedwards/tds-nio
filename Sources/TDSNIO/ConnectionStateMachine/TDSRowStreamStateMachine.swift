struct TDSRowStreamStateMachine {
    enum Action {
        case read
        case wait
    }

    private enum State {
        case waitingForRows([TDSRow])
        case waitingForRead([TDSRow])
        case waitingForDemand([TDSRow])
        case waitingForReadOrDemand([TDSRow])
        case failed
        case modifying
    }

    private var state: State

    init() {
        var buffer = [TDSRow]()
        buffer.reserveCapacity(32)
        self.state = .waitingForRows(buffer)
    }

    mutating func receivedRow(_ row: TDSRow) {
        switch self.state {
        case .waitingForRows(var buffer):
            self.state = .modifying
            buffer.append(row)
            self.state = .waitingForRows(buffer)
        case .waitingForRead(var buffer):
            self.state = .modifying
            buffer.append(row)
            self.state = .waitingForRead(buffer)
        case .waitingForDemand(var buffer):
            self.state = .modifying
            buffer.append(row)
            self.state = .waitingForDemand(buffer)
        case .waitingForReadOrDemand(var buffer):
            self.state = .modifying
            buffer.append(row)
            self.state = .waitingForReadOrDemand(buffer)
        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        case .failed:
            return
        }
    }

    mutating func channelReadComplete() -> [TDSRow]? {
        switch self.state {
        case .waitingForRows(let buffer),
            .waitingForRead(let buffer),
            .waitingForDemand(let buffer),
            .waitingForReadOrDemand(let buffer):
            guard !buffer.isEmpty else {
                self.state = .waitingForRead(buffer)
                return nil
            }
            var newBuffer = buffer
            newBuffer.removeAll(keepingCapacity: true)
            self.state = .waitingForReadOrDemand(newBuffer)
            return buffer
        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        case .failed:
            return nil
        }
    }

    mutating func requestRows() -> Action {
        switch self.state {
        case .waitingForDemand(let buffer):
            self.state = .waitingForRows(buffer)
            return .read
        case .waitingForReadOrDemand(let buffer):
            self.state = .waitingForRead(buffer)
            return .wait
        case .waitingForRead, .waitingForRows:
            return .wait
        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        case .failed:
            return .wait
        }
    }

    mutating func read() -> Action {
        switch self.state {
        case .waitingForRows:
            return .read
        case .waitingForRead(let buffer):
            self.state = .waitingForRows(buffer)
            return .read
        case .waitingForReadOrDemand(let buffer):
            self.state = .waitingForDemand(buffer)
            return .wait
        case .waitingForDemand:
            return .wait
        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        case .failed:
            return .wait
        }
    }

    mutating func end() -> [TDSRow] {
        switch self.state {
        case .waitingForRows(let buffer),
            .waitingForRead(let buffer),
            .waitingForDemand(let buffer),
            .waitingForReadOrDemand(let buffer):
            return buffer
        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        case .failed:
            return []
        }
    }

    mutating func fail() -> Action {
        switch self.state {
        case .waitingForRows, .waitingForRead, .waitingForDemand, .waitingForReadOrDemand:
            self.state = .failed
            return .wait
        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        case .failed:
            return .wait
        }
    }
}
