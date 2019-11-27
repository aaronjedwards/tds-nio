import Foundation
import NIO
import Logging

public final class TDSConnection {
    let channel: Channel
    
    public var eventLoop: EventLoop {
        return self.channel.eventLoop
    }
    
    public var closeFuture: EventLoopFuture<Void> {
        return channel.closeFuture
    }
    
    public var logger: Logger

    private var didClose: Bool

    public var isClosed: Bool {
        return !self.channel.isActive
    }
    
    init(channel: Channel, logger: Logger) {
        self.channel = channel
        self.logger = logger
        self.didClose = false
    }
    
    public func close() -> EventLoopFuture<Void> {
        guard !self.didClose else {
            return self.eventLoop.makeSucceededFuture(())
        }
        self.didClose = true
        
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.eventLoop.submit {
            switch self.channel.isActive {
            case true:
                promise.succeed(())
            case false:
                self.channel.close(mode: .all, promise: promise)
            }
        }.cascadeFailure(to: promise)
        return promise.futureResult
    }
    
    deinit {
        assert(self.didClose, "TDSConnection deinitialized before being closed.")
    }
}

