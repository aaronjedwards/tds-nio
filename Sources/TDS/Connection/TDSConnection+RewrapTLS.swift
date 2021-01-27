import NIO
import NIOSSL
import NIOTLS
import Logging

public final class PipelineOrganizationHandler: ChannelDuplexHandler, RemovableChannelHandler {
    public typealias InboundIn = TDSPacket
    public typealias InboundOut = ByteBuffer
    public typealias OutboundIn = ByteBuffer
    public typealias OutboundOut = TDSPacket

    let logger: Logger

    /// `TDSMessage` decoders/encoders
    var firstDecoder: ByteToMessageHandler<TDSPacketDecoder>
    var firstEncoder: MessageToByteHandler<TDSPacketEncoder>
    var secondEncoder: MessageToByteHandler<TDSPacketEncoder>?
    var secondDecoder: ByteToMessageHandler<TDSPacketDecoder>?
    var sslClientHandler: NIOSSLClientHandler
    
    enum State {
        case start
        case sslHandshake(SSLHandshakeState)
        case allDone
    }
    
    var state = State.start
    
    public init(
        logger: Logger,
        _ firstDecoder: ByteToMessageHandler<TDSPacketDecoder>,
        _ firstEncoder: MessageToByteHandler<TDSPacketEncoder>,
        _ sslClientHandler: NIOSSLClientHandler
    ) {
        self.logger = logger
        self.firstDecoder = firstDecoder
        self.firstEncoder = firstEncoder
        self.sslClientHandler = sslClientHandler
    }
    
    private var storedPackets = [TDSPacket]()
    
    
    // Inbound
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        switch self.state {
        case .sslHandshake(var sslHandshakeState):
            let packet = self.unwrapInboundIn(data)
            
            switch packet.type {
            case .prelogin:
                storedPackets.append(packet)
                if (packet.header.status == .eom) {
                    let messageBuffer = ByteBuffer(from: storedPackets, allocator: context.channel.allocator)
                    sslHandshakeState.addReceivedData(messageBuffer)
                    logger.debug("Unpacking data from Prelogin TLS message")
                    self.state = .sslHandshake(sslHandshakeState)
                    context.fireChannelRead(self.wrapInboundOut(sslHandshakeState.inputBuffer))
                    sslHandshakeState.inputBuffer.clear()
                    state = .sslHandshake(sslHandshakeState)
                    storedPackets.removeAll(keepingCapacity: true)
                }
            default:
                throw TDSError.protocolError("Expected PRELOGIN SSL Handshake Response")
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
            let message = try TDSMessage(from: &sslHandshakeState.outputBuffer, ofType: .prelogin, allocator: context.channel.allocator)
            for packet in message.packets {
                context.write(self.wrapOutboundOut(packet), promise: sslHandshakeState.outputPromise)
            }
            context.flush()
            sslHandshakeState.outputBuffer.clear()
            state = .sslHandshake(sslHandshakeState)
            logger.debug("Flushed Prelogin TLS message")
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

    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        print(error.localizedDescription)
        context.fireErrorCaught(error)
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
