import NIOCore

struct TDSBackendMessageDecoder: ByteToMessageDecoder {

    static let headerSize = 8

    struct Container {
        var flags: UInt8 = 0
        var messages: TinySequence<TDSBackendMessage>
    }
    typealias InboundOut = TinySequence<Container>

    private let context: Context

    /// Used for message testing, to get expected messages one after another
    /// instead of retrieving random amounts, depending on the stream.
    ///
    /// Does not affect production build performance, as taking this route
    /// is only possible in debug builds.
    private let sendSingleMessages: Bool

    final class Context {
        var packetStatus: TDSPacket.Status = []
        var columns: [TDSBackendMessage.ColMetadata.Column] = []
        var altColumns: [UInt16: [TDSBackendMessage.AltMetadata.Column]] = [:]
        var dataClassificationVersion: UInt8 = 1
        var partialPacketType: TDSPacket.MessageType?
        var partialPayload: ByteBuffer?

        init() {}
    }

    init(context: Context) {
        self.context = context
        self.sendSingleMessages = false
    }

    #if DEBUG
        /// For testing only!
        init() {
            self.context = .init()
            self.sendSingleMessages = true
        }
    #endif

    mutating func decode(
        context: ChannelHandlerContext, buffer: inout ByteBuffer
    ) throws -> DecodingState {
        while let (message, needMoreData) = try decodeMessage(from: &buffer) {
            #if DEBUG
                if sendSingleMessages {
                    for part in message {
                        context.fireChannelRead(self.wrapInboundOut([part]))
                    }
                } else {
                    context.fireChannelRead(self.wrapInboundOut(message))
                }
            #else
                context.fireChannelRead(self.wrapInboundOut(message))
            #endif
            if buffer.readableBytes > 0 || needMoreData {
                return .needMoreData
            } else {
                buffer = buffer.slice()
                return .continue
            }
        }
        return .needMoreData
    }

    private func decodeMessage(
        from buffer: inout ByteBuffer
    ) throws -> (InboundOut, needMoreData: Bool)? {
        var msgs: InboundOut?
        var needMoreData = true
        while let (messages, stillNeedMoreData) = try self.decodeMessage0(from: &buffer) {
            needMoreData = stillNeedMoreData
            buffer = buffer.slice()
            if messages.messages.isEmpty {
                continue
            } else if msgs != nil {
                msgs!.append(messages)
            } else {
                msgs = [messages]
            }
        }
        if let msgs {
            return (msgs, needMoreData)
        }
        return nil
    }

    private func decodeMessage0(
        from buffer: inout ByteBuffer
    ) throws -> (Container, needMoreData: Bool)? {
        let startReaderIndex = buffer.readerIndex

        let length: Int?
        length = buffer.getInteger(
            at: startReaderIndex + 2,
            endianness: .big,
            as: UInt16.self
        ).map(Int.init)

        let packetStatus =
            buffer.getInteger(
                at: startReaderIndex + 1,
                as: UInt8.self
            ) ?? 0

        guard
            let length,
            buffer.readableBytes >= Self.headerSize,
            buffer.readableBytes >= length,
            let typeByte = buffer.getInteger(
                at: startReaderIndex,
                as: UInt8.self
            ),
            let type = TDSPacket.MessageType(rawValue: typeByte),
            var packet = buffer.readSlice(length: length)
        else {
            return nil
        }

        packet.moveReaderIndex(forwardBy: Self.headerSize)

        do {
            let status = TDSPacket.Status(rawValue: packetStatus)
            var payload = try self.assemblePayload(
                packet,
                type: type,
                status: status
            )
            guard status.contains(.eom) else {
                return (Container(flags: packetStatus, messages: []), true)
            }

            self.context.packetStatus = status
            let (messages, lastPacket) = try TDSBackendMessage.decode(
                from: &payload, of: type,
                context: self.context
            )
            return (Container(flags: packetStatus, messages: messages), !lastPacket)
        } catch let error as TDSPartialDecodingError {
            buffer.moveReaderIndex(to: startReaderIndex)
            let completeMessage = buffer.readSlice(length: length)!
            throw
                TDSMessageDecodingError
                .withPartialError(
                    error,
                    packetID: type.rawValue,
                    messageBytes: completeMessage
                )
        } catch {
            preconditionFailure(
                "Expected to only see `TDSMessageDecodingError`s here."
            )
        }
    }

    private func assemblePayload(
        _ payload: ByteBuffer,
        type: TDSPacket.MessageType,
        status: TDSPacket.Status
    ) throws -> ByteBuffer {
        if var partialPayload = self.context.partialPayload {
            guard self.context.partialPacketType == type else {
                self.context.partialPayload = nil
                self.context.partialPacketType = nil
                throw TDSPartialDecodingError.unknownMessageIDReceived(messageID: type.rawValue)
            }

            var payload = payload
            partialPayload.writeBuffer(&payload)
            if status.contains(.eom) {
                self.context.partialPayload = nil
                self.context.partialPacketType = nil
                return partialPayload
            } else {
                self.context.partialPayload = partialPayload
                return ByteBuffer()
            }
        } else if status.contains(.eom) {
            return payload
        } else {
            self.context.partialPacketType = type
            self.context.partialPayload = payload
            return ByteBuffer()
        }
    }
}
