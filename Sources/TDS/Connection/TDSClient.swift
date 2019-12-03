import NIO

public protocol TDSClient {
    var eventLoop: EventLoop { get }
    func send(_ request: TDSRequest) -> EventLoopFuture<Void>
}
