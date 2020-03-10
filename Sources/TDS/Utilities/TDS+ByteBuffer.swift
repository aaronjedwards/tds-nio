import NIO

extension ByteBuffer {
    mutating func writeUTF16String(_ str: String, endianness: Endianness = .little) {
        for character in str.utf16 {
            self.writeInteger(character, endianness: endianness)
        }
    }
}
