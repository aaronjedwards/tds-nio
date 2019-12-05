import NIO
import NIOSSL
import NIOTLS
import Logging

extension TDSConnection: TDSClient {
    public func send(_ request: TDSRequest) -> EventLoopFuture<Void> {
        request.log(to: self.logger)
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let request = TDSRequestContext(delegate: request, promise: promise)
        self.channel.write(request).cascadeFailure(to: promise)
        self.channel.flush()
        return promise.futureResult
    }
}

public protocol TDSRequest {
    // nil value ends the request
    func respond(to message: TDSMessage) throws -> TDSMessage?
    func start() throws -> TDSMessage
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
    
    /// `TDSMessage` handlers
    var firstDecoder: ByteToMessageHandler<TDSMessageDecoder>
    var firstEncoder: MessageToByteHandler<TDSMessageEncoder>
    var tlsConfiguration: TLSConfiguration?
    var serverHostname: String?
    
    var sslClientHandler: NIOSSLClientHandler!
    
    var pipelineCoordinator: PipelineOrganizationHandler!
    
    enum State {
        case start
        case sentInitialTDSPreLogin
        case receivedTDSPreLoginResponse
        case sslHandshakeStarted
        case sslComplete
    }
    
    private var state = State.start
    
    private var queue: [TDSRequestContext]
    let logger: Logger
    
    public init(
        logger: Logger,
        _ firstDecoder: ByteToMessageHandler<TDSMessageDecoder>,
        _ firstEncoder: MessageToByteHandler<TDSMessageEncoder>,
        _ tlsConfiguration: TLSConfiguration? = nil,
        _ serverHostname: String? = nil
    ) {
        self.logger = logger
        self.queue = []
        self.firstDecoder = firstDecoder
        self.firstEncoder = firstEncoder
        self.tlsConfiguration = tlsConfiguration
        self.serverHostname = serverHostname
    }
    
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        let message = self.unwrapInboundIn(data)
        guard self.queue.count > 0 else {
            // discard packet
            return
        }
        
        let request = self.queue[0]
        
        switch state {
        case .sentInitialTDSPreLogin:
            switch message.headerType {
            case .preloginResponse:
                state = .receivedTDSPreLoginResponse
            default:
                break
            }
        default:
            break
        }
        
        if let response = try request.delegate.respond(to: message) {
            context.write(self.wrapOutboundOut(response), promise: nil)
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
        let message = try request.delegate.start()
        
        switch state {
        case .receivedTDSPreLoginResponse:
            // kick off ssl negotiation
            if case .sslKickoff = message.headerType {
                self.queue.removeLast()
                guard let tlsConfig = tlsConfiguration, let hostname = serverHostname else {
                    throw TDSError.protocol("Encryption was requested but an SSL Configuration was not provided.")
                }
                
                let sslContext = try! NIOSSLContext(configuration: tlsConfig)
                let sslHandler = try! NIOSSLClientHandler(context: sslContext, serverHostname: hostname)
                self.sslClientHandler = sslHandler
                
                let coordinator = PipelineOrganizationHandler(firstDecoder, firstEncoder, sslHandler)
                self.pipelineCoordinator = coordinator
                
                context.channel.pipeline.addHandler(coordinator, position: .before(self)).whenComplete { _ in
                    context.channel.pipeline.addHandler(sslHandler, position: .after(coordinator)).whenComplete { _ in
                        self.state = .sslHandshakeStarted
                    }
                }
            } else {
                fallthrough
            }
        default:
            switch message.headerType {
            case .prelogin:
                if case .start = state {
                    state = .sentInitialTDSPreLogin
                }
            default:
                break
            }
            
            context.write(self.wrapOutboundOut(message), promise: nil)
            context.flush()
        }
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
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let sslHandshakeComplete = event as? TLSUserEvent, case .handshakeCompleted = sslHandshakeComplete {
            // SSL Handshake complete
            // Remove pipeline coordinator and rearrange message encoder/decoder
            context.channel.pipeline.removeHandler(self.pipelineCoordinator).flatMap {
                return context.channel.pipeline.removeHandler(self.firstDecoder)
            }.flatMap { (Void) -> EventLoopFuture<Void> in
                return context.channel.pipeline.removeHandler(self.firstEncoder)
            }.flatMap {
                return context.channel.pipeline.addHandler(ByteToMessageHandler(TDSMessageDecoder()), position: .after(self.sslClientHandler))
            }.flatMap {
                return context.channel.pipeline.addHandler(MessageToByteHandler(TDSMessageEncoder()), position: .after(self.sslClientHandler))
            }.whenComplete { _ in
                print("Done w/ SSL Handshake and pipeline organization")
            }
        }
    }
}
