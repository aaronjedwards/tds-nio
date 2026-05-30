import NIOCore

/// Wraps TLS handshake bytes in TDS PRELOGIN packets and unwraps PRELOGIN
/// packet payloads back into TLS handshake bytes.
///
/// SQL Server's initial TLS handshake is transported inside TDS packets during
/// PRELOGIN negotiation. Once the TLS handshake has completed, this handler is
/// removed and normal TDS packets flow through the TLS handler.
final class TDSPreloginTLSHandler: ChannelDuplexHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = self.unwrapInboundIn(data)

        do {
            while let payload = try Self.unwrapPreloginPacket(from: &buffer) {
                context.fireChannelRead(self.wrapInboundOut(payload))
            }
        } catch {
            context.fireErrorCaught(error)
        }
    }

    func write(
        context: ChannelHandlerContext,
        data: NIOAny,
        promise: EventLoopPromise<Void>?
    ) {
        var tlsBytes = self.unwrapOutboundIn(data)
        var packet = context.channel.allocator.buffer(
            capacity: tlsBytes.readableBytes + TDSPacket.headerLength
        )
        packet.moveWriterIndex(forwardBy: TDSPacket.headerLength)
        packet.writeBuffer(&tlsBytes)
        packet.prepareSend(
            packetType: .prelogin,
            statusFlags: [.eom],
            payloadLength: UInt16(packet.readableBytes - TDSPacket.headerLength)
        )
        context.write(self.wrapOutboundOut(packet), promise: promise)
    }

    private static func unwrapPreloginPacket(
        from buffer: inout ByteBuffer
    ) throws -> ByteBuffer? {
        let startIndex = buffer.readerIndex
        guard buffer.readableBytes >= TDSPacket.headerLength else {
            return nil
        }
        guard
            let typeByte = buffer.getInteger(at: startIndex, as: UInt8.self),
            let packetType = TDSPacket.MessageType(rawValue: typeByte),
            let length = buffer.getInteger(
                at: startIndex + 2,
                endianness: .big,
                as: UInt16.self
            )
        else {
            return nil
        }
        guard packetType == .prelogin else {
            throw TDSPartialDecodingError.unknownMessageIDReceived(messageID: typeByte)
        }
        guard buffer.readableBytes >= Int(length) else {
            return nil
        }

        buffer.moveReaderIndex(forwardBy: TDSPacket.headerLength)
        guard let payload = buffer.readSlice(length: Int(length) - TDSPacket.headerLength) else {
            throw TDSPartialDecodingError.expectedAtLeastNRemainingBytes(
                Int(length) - TDSPacket.headerLength,
                actual: buffer.readableBytes
            )
        }
        return payload
    }
}
