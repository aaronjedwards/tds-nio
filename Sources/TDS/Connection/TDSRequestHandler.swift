import NIOTLS

final class TDSRequestHandler: ChannelDuplexHandler {
    typealias InboundIn = TDSPacket
    typealias OutboundIn = TDSRequestContext
    typealias OutboundOut = TDSPacket
    
    /// References to the below decoder/encoder are included for the reorganization of the channel pipeline
    /// once the TLS handshake is finished. This is required because, per the TDS protocol, TLS payloads must be sent wrapped in a
    /// TDS message.
    var firstDecoder: ByteToMessageHandler<TDSPacketDecoder>
    var firstEncoder: MessageToByteHandler<TDSPacketEncoder>
    
    var tlsConfiguration: TLSConfiguration?
    var serverHostname: String?
    
    var sslClientHandler: NIOSSLClientHandler?
    
    var pipelineCoordinator: PipelineOrganizationHandler!
    
    enum State: Int {
        case start
        case sentPrelogin
        case sentTLSNegotiation
        case sentLoginWithCompleteAuth
        case sentLoginWithSpengo
        case sentLoginWithFedAuth
        case loggedIn
        case sentClientRequest
        case sentAttention
        case routingComplete
        case final
    }
    
    private var state = State.start
    
    private var queue: [TDSRequestContext]
    
    let logger: Logger
    
    var pendingRequest: TDSRequestContext? {
        get {
            self.queue.first
        }
    }
    
    var pendingRequestPackets: [TDSPacket]
    var tokenParser: TDSTokenParser
    
    public init(
        logger: Logger,
        
        _ firstDecoder: ByteToMessageHandler<TDSPacketDecoder>,
        _ firstEncoder: MessageToByteHandler<TDSPacketEncoder>,
        _ tlsConfiguration: TLSConfiguration? = nil,
        _ serverHostname: String? = nil
    ) {
        self.logger = logger
        self.queue = []
        self.firstDecoder = firstDecoder
        self.firstEncoder = firstEncoder
        self.tlsConfiguration = tlsConfiguration
        self.serverHostname = serverHostname
        self.pendingRequestPackets = []
        self.tokenParser = TDSTokenParser(logger: logger)
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        do {
            try self.readTDSPacket(context: context, data: data)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    /// Handles the incoming TDS Packet according the the current pending request type.
    ///
    /// If the pending request is a `TDSTokenStreamRequest`,  the packet is sent to a `TokenStreamParser` where the packet data is added
    /// to the parsers data buffer. The parser then attempts to parse as many tokens as possible from the current buffer. As they are parsed from the response stream, tokens
    /// are passed to a delegate method on the current `TDSTokenStreamRequest`.
    ///
    /// If the pending request is simply a `TDSRequest` (has a "tokenless" response from the server), the incoming packet is appended to an array of packets being tracked
    /// for the current request. This is done until an EOM packet is seen at which point the delegate handler is called on the current pending `TDSRequest`
    ///
    /// Regardless of the request type, the `complete` method is always fired when the entire requests response has been recieved. This allows the requests to initiate their
    /// own responses, that will then be dispatched.
    ///
    private func readTDSPacket(context: ChannelHandlerContext, data: NIOAny) throws {
        let packet = self.unwrapInboundIn(data)
        guard let request = self.pendingRequest else {
            // Unsolicited packet... discard
            return
        }
        
        do {
            if let delegate = request.delegate as? TDSTokenStreamRequest {
                tokenParser.parseTokens(packet.messageBuffer, onToken: delegate.handle)
            } else {
                pendingRequestPackets.append(packet)
            }
            
            if packet.isEom {
               try handleMessageCompletion(context: context, request: request, packet: packet)
            }
        } catch {
            cleanupRequest(request, error: error)
        }
    }
    
    /// Handles the response to an incoming packet for a given `TDSRequest`
    ///
    /// If the response is `.done` then the current pending request is considered complete and is cleaned up, a connection state
    /// transition occurs if neccessary, and an there is an attempt to write the next pending request.
    private func handleResponse(
        context: ChannelHandlerContext,
        request: TDSRequestContext,
        response: TDSRequestResponse,
        packet: TDSPacket
    ) throws {
        guard let request = self.pendingRequest else {
            return
        }
        switch response {
        case .kickoffSSL:
            guard case .sentPrelogin = state else {
                throw TDSError.protocolError("Unexpected state to initiate SSL kickoff. If encryption is negotiated, the SSL exchange should immediately follow the PRELOGIN phase.")
            }
            try sslKickoff(context: context)
        case .respond(let packets):
            try write(context: context, packets: packets, promise: nil)
            context.flush()
        case .continue:
            return
        case .done:
            cleanupRequest(request)
            try handleMessageCompleteStateTransition(context: context)
            writePendingTDSRequestIfReady(context: context, promise: nil)
        }
    }
    
    private func handleMessageCompletion(context: ChannelHandlerContext, request: TDSRequestContext, packet: TDSPacket) throws {
        var messageBuffer = ByteBuffer(from: pendingRequestPackets, allocator: context.channel.allocator)
        let response = try request.delegate.complete(message: &messageBuffer, allocator: context.channel.allocator)
        try handleResponse(context: context, request: request, response: response, packet: packet)
    }
    
    /// Adds the inbound request to the request queue and attempts to write the inbound request's packets if the connection is in a ready state.
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let request = self.unwrapOutboundIn(data)
        self.queue.append(request)
        writePendingTDSRequestIfReady(context: context, promise: promise)
    }
    
    func writePendingTDSRequestIfReady(context: ChannelHandlerContext, promise: EventLoopPromise<Void>?) {
        if let request = pendingRequest {
            do {
                let packets = try request.delegate.start(allocator: context.channel.allocator)
                try write(context: context, packets: packets, promise: promise)
                context.flush()
            } catch {
                self.errorCaught(context: context, error: error)
            }
        }
    }
    
    /// Writes a set of packets and performs a state transition based on the type of packet that is being sent,
    /// It is implied that all packets being sent are of the same type.
    private func write(context: ChannelHandlerContext, packets: [TDSPacket], promise: EventLoopPromise<Void>?) throws {
        var packets = packets
        guard let requestType = packets.first?.type else {
            return
        }
        
        if let last = packets.popLast() {
            for item in packets {
                context.write(self.wrapOutboundOut(item), promise: nil)
            }
            context.write(self.wrapOutboundOut(last), promise: promise)
            try handleInitiatedRequestStateTransition(context: context, type: requestType)
            
        } else {
            promise?.succeed(())
        }
    }
    
    private func handleMessageCompleteStateTransition(context: ChannelHandlerContext) throws {
        var nextState = state
        switch state {
        case .start:
            break
        case .sentPrelogin:
            break
        case .sentTLSNegotiation:
            break
        case .sentLoginWithCompleteAuth:
            nextState = .loggedIn
        case .sentLoginWithSpengo:
            break
        case .sentLoginWithFedAuth:
            break
        case .loggedIn:
            break
        case .sentClientRequest:
            nextState = .loggedIn
        case .sentAttention:
            break
        case .routingComplete:
            break
        case .final:
            break
        }
        
        transition(to: nextState)
    }
    
    private func handleInitiatedRequestStateTransition(context: ChannelHandlerContext, type: TDSPacket.HeaderType) throws {
        var nextState = state
        switch state {
        case .start:
            guard case .prelogin = type else {
                throw TDSError.invalidTransition("PRELOGIN message must be the first message sent and may only be sent once per connection.")
            }
            nextState = .sentPrelogin
        case .sentPrelogin, .sentTLSNegotiation:
            if case .tds7Login = type {
                nextState = .sentLoginWithCompleteAuth
            } else {
                throw TDSError.invalidTransition("LOGIN message must follow immediately after the PRELOGIN message or (if encryption is enabled) SSL negotiation and may only be sent once per connection.")
            }
            break
        case .sentLoginWithCompleteAuth:
            break
        case .sentLoginWithSpengo:
            break
        case .sentLoginWithFedAuth:
            break
        case .loggedIn:
            nextState = .sentClientRequest
        case .sentClientRequest:
            guard case .attentionSignal = type else {
                throw TDSError.invalidTransition("Another request is already in progress.")
            }
        case .sentAttention:
            break
        case .routingComplete:
            break
        case .final:
            break
        }
        
        transition(to: nextState)
    }
    
    private func transition(to nextState: State) {
        guard state != nextState else {
            // transition to the same state
            return
        }
        logger.debug("Transitioning from \(state) to \(nextState)")
        state = nextState
    }
    
    private func cleanupRequest(_ request: TDSRequestContext, error: Error? = nil) {
        self.queue.removeFirst()
        self.pendingRequestPackets.removeAll(keepingCapacity: true)
        if let error = error {
            request.promise.fail(error)
        } else {
            request.promise.succeed(())
        }
    }
    
    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        context.close(mode: mode, promise: promise)
        
        for current in self.queue {
            current.promise.fail(TDSError.connectionClosed)
        }
        self.queue = []
    }
    
    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        print(error.localizedDescription)
        context.fireErrorCaught(error)
    }
    
    /// SSL/TLS Negotiation
    
    private func sslKickoff(context: ChannelHandlerContext) throws {
        guard let tlsConfig = tlsConfiguration else {
            throw TDSError.protocolError("Encryption was requested but a TLS Configuration was not provided.")
        }
        
        let sslContext = try! NIOSSLContext(configuration: tlsConfig)
        let sslHandler = try! NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname)
        self.sslClientHandler = sslHandler
        
        let coordinator = PipelineOrganizationHandler(logger: logger, firstDecoder, firstEncoder, sslHandler)
        self.pipelineCoordinator = coordinator
        
        context.channel.pipeline.addHandler(coordinator, position: .before(self)).whenComplete { _ in
            context.channel.pipeline.addHandler(sslHandler, position: .after(coordinator)).whenComplete { _ in
                self.transition(to: .sentTLSNegotiation)
            }
        }
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let sslHandler = sslClientHandler, let sslHandshakeComplete = event as? TLSUserEvent, case .handshakeCompleted = sslHandshakeComplete {
            // SSL Handshake complete
            // Remove pipeline coordinator and rearrange message encoder/decoder
            
            let future = EventLoopFuture.andAllSucceed([
                context.channel.pipeline.removeHandler(self.pipelineCoordinator),
                context.channel.pipeline.removeHandler(self.firstDecoder),
                context.channel.pipeline.removeHandler(self.firstEncoder),
                context.channel.pipeline.addHandler(ByteToMessageHandler(TDSPacketDecoder(logger: logger)), position: .after(sslHandler)),
                context.channel.pipeline.addHandler(MessageToByteHandler(TDSPacketEncoder(logger: logger)), position: .after(sslHandler))
            ], on: context.eventLoop)
            
            future.whenSuccess {_ in
                self.logger.debug("Done w/ SSL Handshake and pipeline organization")
                if let request = self.pendingRequest {
                    self.cleanupRequest(request)
                }
            }
            
            future.whenFailure { error in
                self.errorCaught(context: context, error: error)
            }
        }
    }
}
