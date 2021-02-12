import NIO
import Logging

public protocol TDSDatabase {
    var logger: Logger { get }
    var eventLoop: EventLoop { get }
    func send(_ request: TDSRequest, logger: Logger) -> EventLoopFuture<Void>
}
