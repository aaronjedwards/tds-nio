import NIO
import Logging

public protocol TDSRequest {
    func complete(message: inout ByteBuffer, allocator: ByteBufferAllocator) throws -> TDSRequestResponse
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket]
    func log(to logger: Logger)
}

protocol TDSTokenStreamRequest: TDSRequest {
    func handle(token: TDSToken) throws
}

public enum TDSRequestResponse {
    case done
    case `continue`
    case respond(with: [TDSPacket])
    case kickoffSSL
}

final class TDSRequestContext {
    var delegate: TDSRequest
    let promise: EventLoopPromise<Void>
    
    init(delegate: TDSRequest, promise: EventLoopPromise<Void>) {
        self.delegate = delegate
        self.promise = promise
    }
}

extension TDSConnection: TDSDatabase {
    public func send(_ request: TDSRequest, logger: Logger) -> EventLoopFuture<Void> {
        request.log(to: self.logger)
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let request = TDSRequestContext(delegate: request, promise: promise)
        self.channel.writeAndFlush(request).cascadeFailure(to: promise)
        return promise.futureResult
    }
}
