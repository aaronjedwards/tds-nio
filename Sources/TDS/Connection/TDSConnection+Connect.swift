import Logging
import NIO
import NIOSSL
import Foundation

extension TDSConnection {
    /// Note about TLS Support:
    ///
    /// If a `TLSConfiguration` is provided, it will be used to negotiate encryption, signaling to the server that encryption is enabled (ENCRYPT_ON).
    /// If no `TLSConfiguration` is provided, it is assumed that a standard configuration will work and signals to the server that encryption is enabled (ENCRYPT_ON).
    /// If the user explicitly passes a nil `TLSconfiguration`, it will signal to the server that encryption is not supported (ENCRYPT_NOT_SUP).
    ///
    /// Supporting the case for only encrypting login packets provides little benefit and makes it impossible to provide a default (valid) TLSConfiguration.
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = .forClient(),
        serverHostname: String? = nil,
        on eventLoop: EventLoop
    ) -> EventLoopFuture<TDSConnection> {
        let bootstrap = ClientBootstrap(group: eventLoop)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
        
        let logger = Logger(label: "swift-tds")
        
        // TDSMessage decoders
        let firstDecoder = ByteToMessageHandler(TDSPacketDecoder(logger: logger))
        let firstEncoder = MessageToByteHandler(TDSPacketEncoder(logger: logger))
        
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
            return conn.prelogin(shouldNegotiateEncryption: tlsConfiguration != nil ? true : false)
                .flatMapError { error in
                    conn.close().flatMapThrowing {
                        throw error
                    }
                }.map {
                    return conn
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

