import NIO
import NIOSSL

public final class PipelineOrganizationHandler: ChannelDuplexHandler {
    public typealias InboundIn = TDSMessage
    public typealias InboundOut = ByteBuffer
    public typealias OutboundIn = ByteBuffer
    public typealias OutboundOut = TDSMessage
    
    /// `TDSMessage` decoders
    var firstDecoder: ByteToMessageHandler<TDSMessageDecoder>
    var secondDecoder: ByteToMessageHandler<TDSMessageDecoder>
    
    /// `TDSMessage` encoders
    var firstEncoder: MessageToByteHandler<TDSMessageEncoder>
    var secondEncoder: MessageToByteHandler<TDSMessageEncoder>
    
    var sslClientHandler: NIOSSLClientHandler
    
    enum State {
        case start
        case sentInitialTDSPreLogin
        case receivedTDSPreLoginResponse
        case sslHandlerAdded(SSLHandlerAddedState)
        case allDone
    }
    
    var state = State.start
    
    public init(
        _ firstDecoder: ByteToMessageHandler<TDSMessageDecoder>,
        _ secondDecoder: ByteToMessageHandler<TDSMessageDecoder>,
        _ firstEncoder: MessageToByteHandler<TDSMessageEncoder>,
        _ secondEncoder: MessageToByteHandler<TDSMessageEncoder>,
        _ sslClientHandler: NIOSSLClientHandler
    ) {
        self.firstDecoder = firstDecoder
        self.secondDecoder = secondDecoder
        self.firstEncoder = firstEncoder
        self.secondEncoder = secondEncoder
        self.sslClientHandler = sslClientHandler
    }
    
    
    // Inbound
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        switch self.state {
        case .sentInitialTDSPreLogin:
            let message = self.unwrapInboundIn(data)
            switch message.headerType {
            case .prelogin:
                self.state = .receivedTDSPreLoginResponse
                context.channel.pipeline.addHandler(self.sslClientHandler, position: .after(self)).whenComplete { _ in
                    let sslHandlerAddedState = SSLHandlerAddedState(inputBuffer: context.channel.allocator.buffer(capacity: 1024), outputBuffer: context.channel.allocator.buffer(capacity: 1024), outputPromise: context.eventLoop.makePromise())
                    self.state = .sslHandlerAdded(sslHandlerAddedState)
                }
            default:
                throw TDSError.protocol("Expected PRELOGIN SSL Handshake Response")
            }
        case .sslHandlerAdded(var sslHandlerAddedState):
            let message = self.unwrapInboundIn(data)
            switch message.headerType {
            case .prelogin:
                let message = try TDSMessage.PreloginSSLHandshakeMessage.init(message: message)
                sslHandlerAddedState.addReceivedData(message.sslPayload)
                self.state = .sslHandlerAdded(sslHandlerAddedState)
            default:
                throw TDSError.protocol("Expected PRELOGIN SSL Handshake Response")
            }
        default:
            break
        }
    }
    
    // Outbound
    private func _write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) throws {
        switch self.state {
        case .start:
            self.state = .sentInitialTDSPreLogin
        case .sslHandlerAdded(var sslHandlerAddedState):
            let recievedBuffer = self.unwrapOutboundIn(data)
            sslHandlerAddedState.addPendingOutputData(recievedBuffer)
            sslHandlerAddedState.outputPromise.futureResult.cascade(to: promise)
            self.state = .sslHandlerAdded(sslHandlerAddedState)
        default:
            break
        }
    }
    
    private func _flush(context: ChannelHandlerContext) throws {
        switch self.state {
        case .sslHandlerAdded(let sslHandlerAddedState):
            let message = try TDSMessage.PreloginSSLHandshakeMessage(sslPayload: sslHandlerAddedState.outputBuffer).message()
            context.writeAndFlush(self.wrapOutboundOut(message), promise: sslHandlerAddedState.outputPromise)
        default:
            break
        }
    }
    
    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        do {
            try self._channelRead(context: context, data: data)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    public func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        do {
            try self._write(context: context, data: data, promise: promise)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    public func flush(context: ChannelHandlerContext) {
        do {
            try self._flush(context: context)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
}

public struct SSLHandlerAddedState {
    var inputBuffer: ByteBuffer
    var outputBuffer: ByteBuffer
    var outputPromise: EventLoopPromise<Void>
    
    mutating func addReceivedData(_ buffer: ByteBuffer) {
        var buffer = buffer
        self.inputBuffer.writeBuffer(&buffer)
    }
    
    mutating func addPendingOutputData(_ buffer: ByteBuffer) {
        var buffer = buffer
        self.outputBuffer.writeBuffer(&buffer)
    }
}
