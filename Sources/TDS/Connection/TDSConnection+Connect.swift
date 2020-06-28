import Logging
import NIO
import NIOSSL
import Foundation

extension TDSConnection {
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        on eventLoop: EventLoop
    ) -> EventLoopFuture<TDSConnection> {
        let bootstrap = ClientBootstrap(group: eventLoop)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
        
        let logger = Logger(label: "swift-tds")
//        logger.logLevel = Logger.Level.debug
        
        // TDSMessage decoders
        let firstDecoder = ByteToMessageHandler(TDSMessageDecoder(logger: logger))
        let firstEncoder = MessageToByteHandler(TDSMessageEncoder(logger: logger))
        return bootstrap.connect(to: socketAddress).flatMap { channel in
            return channel.pipeline.addHandlers([
                firstDecoder,
                firstEncoder,
                TDSRequestHandler(logger: logger, firstDecoder, firstEncoder, tlsConfiguration, serverHostname),
                TDSErrorHandler(logger: logger)
            ]).map {
                return TDSConnection(channel: channel, logger: logger)
            }
        }.flatMap { conn in
            return eventLoop.makeSucceededFuture(conn)
        }
    }
}

private final class TDSErrorHandler: ChannelInboundHandler {
    typealias InboundIn = Never
    
    let logger: Logger
    init(logger: Logger) {
        self.logger = logger
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.logger.error("Uncaught error: \(error)")
        context.close(promise: nil)
        context.fireErrorCaught(error)
        
    }
}

