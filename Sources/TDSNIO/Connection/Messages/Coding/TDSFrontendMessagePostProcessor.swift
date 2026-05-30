import NIOCore

final class TDSFrontendMessagePostProcessor: ChannelOutboundHandler {
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let headerLength = TDSPacket.headerLength
    private var packetLength: Int

    init(packetLength: Int = TDSPacket.defaultPacketLength) {
        self.packetLength = TDSPacket.clampedPacketLength(packetLength)
    }

    func triggerUserOutboundEvent(
        context: ChannelHandlerContext,
        event: Any,
        promise: EventLoopPromise<Void>?
    ) {
        if case TDSSQLEvent.packetSizeChanged(let packetLength) = event {
            self.packetLength = TDSPacket.clampedPacketLength(packetLength)
            promise?.succeed(())
        } else {
            context.triggerUserOutboundEvent(event, promise: promise)
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        var buffer = self.unwrapOutboundIn(data)

        if buffer.readableBytes > self.packetLength {
            var temporaryBuffer = context.channel.allocator
                .buffer(capacity: self.packetLength)
            // skip over the header, we will write it later
            temporaryBuffer.moveWriterIndex(forwardBy: TDSPacket.headerLength)

            // pluck out the messsage type and status from the provided packet header
            let packetTypeByte =
                buffer
                .getInteger(at: 0, as: UInt8.self)!
            let originalStatusByte =
                buffer
                .getInteger(at: MemoryLayout<UInt8>.size, as: UInt8.self)!

            // Any value originally entered for the packet id is ignored and reset to 0
            var packetId = 0

            // ignore the header, because we need to create a new one for each
            // slice with the size of the slice.
            buffer.moveReaderIndex(to: TDSPacket.headerLength)

            let maxContentSize = self.packetLength - TDSPacket.headerLength
            while buffer.readableBytes > 0 {
                var slice: ByteBuffer
                let final: Bool
                var statusByte = originalStatusByte & ~TDSPacket.StatusFlag.eom.rawValue
                if buffer.readableBytes > maxContentSize {
                    slice = buffer.readSlice(length: maxContentSize)!
                    final = false
                } else {
                    slice = buffer.readSlice(length: buffer.readableBytes)!
                    final = true
                    statusByte = originalStatusByte | TDSPacket.StatusFlag.eom.rawValue
                }
                temporaryBuffer.writeBuffer(&slice)
                temporaryBuffer.prepareSend(
                    packetTypeByte: packetTypeByte,
                    statusByte: statusByte,
                    payloadLength: UInt16(temporaryBuffer.readableBytes - TDSPacket.headerLength),
                    packetId: UInt8(packetId % 256)
                )
                context.writeAndFlush(
                    self.wrapOutboundOut(temporaryBuffer), promise: final ? promise : nil
                )
                if !final {
                    temporaryBuffer.clear(minimumCapacity: self.packetLength)
                    temporaryBuffer.moveWriterIndex(forwardBy: headerLength)
                    packetId += 1
                }
            }
            return
        }

        context.writeAndFlush(self.wrapOutboundOut(buffer), promise: promise)
    }

}
