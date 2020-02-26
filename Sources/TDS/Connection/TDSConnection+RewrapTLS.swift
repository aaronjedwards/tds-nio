import NIO
import NIOSSL
import NIOTLS

public final class PipelineOrganizationHandler: ChannelDuplexHandler, RemovableChannelHandler {
    public typealias InboundIn = [TDSPacket]
    public typealias InboundOut = ByteBuffer
    public typealias OutboundIn = ByteBuffer
    public typealias OutboundOut = [TDSPacket]
    
    /// `TDSMessage` decoders/encoders
    var firstDecoder: ByteToMessageHandler<TDSMessageDecoder>
    var firstEncoder: MessageToByteHandler<TDSMessageEncoder>
    var secondEncoder: MessageToByteHandler<TDSMessageEncoder>?
    var secondDecoder: ByteToMessageHandler<TDSMessageDecoder>?
    var sslClientHandler: NIOSSLClientHandler
    
    enum State {
        case start
        case sslHandshake(SSLHandshakeState)
        case allDone
    }
    
    var state = State.start
    
    public init(
        _ firstDecoder: ByteToMessageHandler<TDSMessageDecoder>,
        _ firstEncoder: MessageToByteHandler<TDSMessageEncoder>,
        _ sslClientHandler: NIOSSLClientHandler
    ) {
        self.firstDecoder = firstDecoder
        self.firstEncoder = firstEncoder
        self.sslClientHandler = sslClientHandler
    }
    
    
    // Inbound
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        switch self.state {
        case .sslHandshake(var sslHandshakeState):
            let messages = self.unwrapInboundIn(data)
            
            // There MUST be 1 packet/message, otherwise the decoder failed us
            let packet = messages[0]
            
            switch packet.headerType {
            case .prelogin:
                let message = try TDSMessages.PreloginSSLHandshakeMessage(packet: packet)
                sslHandshakeState.addReceivedData(message.sslPayload)
                self.state = .sslHandshake(sslHandshakeState)
                context.fireChannelRead(self.wrapInboundOut(sslHandshakeState.inputBuffer))
                sslHandshakeState.inputBuffer.clear()
                state = .sslHandshake(sslHandshakeState)
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
            let sslHandshakeState = SSLHandshakeState(inputBuffer: context.channel.allocator.buffer(capacity: 1024), outputBuffer: context.channel.allocator.buffer(capacity: 1024), outputPromise: context.eventLoop.makePromise())
            updateSSLHandshakeState(sslHandshakeState, data: data, promise: promise)
        case .sslHandshake(let sslHandshakeState):
            updateSSLHandshakeState(sslHandshakeState, data: data, promise: promise)
        default:
            break
        }
    }
    
    private func _flush(context: ChannelHandlerContext) throws {
        switch self.state {
        case .sslHandshake(var sslHandshakeState):
            let message = TDSMessages.PreloginSSLHandshakeMessage(sslPayload: sslHandshakeState.outputBuffer)
            let packet = try TDSPacket(message: message, isLastPacket: true, allocator: context.channel.allocator)
            context.writeAndFlush(self.wrapOutboundOut([packet]), promise: sslHandshakeState.outputPromise)
            sslHandshakeState.outputBuffer.clear()
            state = .sslHandshake(sslHandshakeState)
        default:
            context.flush()
        }
    }
    
    private func updateSSLHandshakeState(_ sslHandshakeState: SSLHandshakeState, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let recievedBuffer = self.unwrapOutboundIn(data)
        var handshakeState = sslHandshakeState
        handshakeState.addPendingOutputData(recievedBuffer)
        handshakeState.outputPromise.futureResult.cascade(to: promise)
        self.state = .sslHandshake(handshakeState)
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

public struct SSLHandshakeState {
    var inputBuffer: ByteBuffer
    var outputBuffer: ByteBuffer
    var outputPromise: EventLoopPromise<Void>
    
    enum State {
        case start
        case clientHelloSent
        case serverHelloRecieved
        case keyExchangeSent
        case keyExchangeRecieved
    }
    
    var state = State.start
    
    mutating func addReceivedData(_ buffer: ByteBuffer) {
        var buffer = buffer
        self.inputBuffer.writeBuffer(&buffer)
    }
    
    mutating func addPendingOutputData(_ buffer: ByteBuffer) {
        var buffer = buffer
        self.outputBuffer.writeBuffer(&buffer)
    }
}
