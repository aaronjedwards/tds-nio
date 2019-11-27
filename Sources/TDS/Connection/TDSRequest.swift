import NIO
import Logging

public protocol TDSRequest {
    // nil value ends the request
    func respond(to message: TDSMessage) throws -> [TDSMessage]?
    func start() throws -> [TDSMessage]
    func log(to logger: Logger)
}

final class TDSRequestContext {
    let delegate: TDSRequest
    let promise: EventLoopPromise<Void>
    var lastError: Error?
    
    init(delegate: TDSRequest, promise: EventLoopPromise<Void>) {
        self.delegate = delegate
        self.promise = promise
    }
}

final class TDSRequestHandler: ChannelDuplexHandler {
    typealias InboundIn = TDSMessage
    typealias OutboundIn = TDSRequestContext
    typealias OutboundOut = TDSMessage
    
    private var queue: [TDSRequestContext]
    let logger: Logger
    
    public init(logger: Logger) {
        self.queue = []
        self.logger = logger
    }
    
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        let message = self.unwrapInboundIn(data)
        guard self.queue.count > 0 else {
            // discard packet
            return
        }
        let request = self.queue[0]
        
        if let responses = try request.delegate.respond(to: message) {
            for response in responses {
                context.write(self.wrapOutboundOut(response), promise: nil)
            }
            context.flush()
        } else {
            self.queue.removeFirst()
            if let error = request.lastError {
                request.promise.fail(error)
            } else {
                request.promise.succeed(())
            }
        }
    }
    
    private func _write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) throws {
        let request = self.unwrapOutboundIn(data)
        self.queue.append(request)
        let messages = try request.delegate.start()
        for message in messages {
            context.write(self.wrapOutboundOut(message), promise: nil)
        }
        context.flush()
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        do {
            try self._channelRead(context: context, data: data)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        do {
            try self._write(context: context, data: data, promise: promise)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        for current in self.queue {
            current.promise.fail(TDSError.connectionClosed)
        }
        self.queue = []
        context.close(mode: mode, promise: promise)
    }
}
