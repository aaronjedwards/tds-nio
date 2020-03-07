import NIO

extension TDSMessages {

    static public func serializeAllHeaders(_ buffer: inout ByteBuffer) {
        let startWriterIndex = buffer.writerIndex
        // skip TotalLength for now
        buffer.moveWriterIndex(forwardBy: 4)

        // TransactionDescriptor
        buffer.writeInteger(18 as UInt32, endianness: .little) // HeaderLength
        buffer.writeInteger(0x02 as UInt16, endianness: .little) // HeaderType
        buffer.writeInteger(0 as UInt64) // TransactionDescriptor
        buffer.writeInteger(1 as UInt32) // OutstandingRequestCount

        buffer.setInteger(UInt32(buffer.writerIndex - startWriterIndex), at: startWriterIndex, endianness: .little)

        print(buffer.debugDescription)
        return
    }
}
