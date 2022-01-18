import NIO

extension TDSMessage {

    static public func serializeAllHeaders(_ buffer: inout ByteBuffer) {
        let startWriterIndex = buffer.writerIndex
        // skip TotalLength for now
        buffer.moveWriterIndex(forwardBy: 4)

        // TransactionDescriptor
        buffer.writeInteger(18 as DWord, endianness: .little) // HeaderLength
        buffer.writeInteger(0x02 as UShort, endianness: .little) // HeaderType
        buffer.writeInteger(0 as ULongLong, endianness: .little) // TransactionDescriptor
        buffer.writeInteger(1 as DWord, endianness: .little) // OutstandingRequestCount

        buffer.setInteger(DWord(buffer.writerIndex - startWriterIndex), at: startWriterIndex, endianness: .little)
        return
    }
}
