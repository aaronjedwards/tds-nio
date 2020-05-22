import NIO

extension ByteBuffer {
    mutating func writeUTF16String(_ str: String, endianness: Endianness = .little) {
        for character in str.utf16 {
            self.writeInteger(character, endianness: endianness)
        }
    }

    mutating func readBVarchar() -> String? {
        guard
            let bytes = self.readByte(),
            let utf16 = self.readUTF16String(length: Int(bytes))
        else {
            return nil
        }
        return utf16
    }

    mutating func readUSVarchar() -> String? {
        guard
            let bytes = self.readUShort(),
            let utf16 = self.readUTF16String(length: Int(bytes))
        else {
            return nil
        }
        return utf16
    }

    mutating func readBVarbyte() -> [Byte]? {
        guard
            let numBytes = self.readByte(),
            let bytes = self.readBytes(length: Int(numBytes))
        else {
            return nil
        }
        return bytes
    }

    mutating func readUSVarbyte() -> [Byte]? {
        guard
            let numBytes = self.readUShort(),
            let bytes = self.readBytes(length: Int(numBytes))
        else {
            return nil
        }
        return bytes
    }

    mutating func readLVarbyte() -> [Byte]? {
        guard
            let numBytes = self.readULong(),
            let bytes = self.readBytes(length: Int(numBytes))
        else {
            return nil
        }
        return bytes
    }

    mutating func readUTF16String(length: Int) -> String? {
        guard
            let bytes = self.readBytes(length: length * 2),
            let utf16 = String(bytes: bytes, encoding: .utf16LittleEndian)
        else {
            return nil
        }
        return utf16
    }

    mutating func readByte() -> Byte? {
        guard let val = self.readInteger(as: Byte.self) else {
            return nil
        }
        return val
    }

    mutating func readUShort(endianess: Endianness = .little) -> UShort? {
        guard let val = self.readInteger(endianness: endianess, as: UShort.self) else {
            return nil
        }
        return val
    }

    mutating func readULong(endianess: Endianness = .little) -> ULong? {
        guard let val = self.readInteger(endianness: endianess, as: ULong.self) else {
            return nil
        }
        return val
    }

    mutating func readLong(endianess: Endianness = .little) -> Long? {
        guard let val = self.readInteger(endianness: endianess, as: Long.self) else {
            return nil
        }
        return val
    }

    mutating func readDWord(endianess: Endianness = .big) -> DWord? {
        guard let val = self.readInteger(endianness: endianess, as: DWord.self) else {
            return nil
        }
        return val
    }

    mutating func readULongLong(endianess: Endianness = .little) -> ULongLong? {
        guard let val = self.readInteger(endianness: endianess, as: ULongLong.self) else {
            return nil
        }
        return val
    }

    mutating func readByteLen(endianness: Endianness = .little) -> ByteLen? {
        guard let val = self.readInteger(endianness: .little, as: ByteLen.self) else {
            return nil
        }
        return val
    }

    mutating func readUShortLen(endianness: Endianness = .little) -> UShortLen? {
        guard let val = self.readInteger(endianness: .little, as: UShortLen.self) else {
            return nil
        }
        return val
    }

    mutating func readUShortCharBinLen(endianness: Endianness = .little) -> UShortCharBinLen? {
        guard let val = self.readInteger(endianness: .little, as: UShortCharBinLen.self) else {
            return nil
        }
        return val
    }

    mutating func readLongLen(endianness: Endianness = .little) -> LongLen? {
        guard let val = self.readInteger(endianness: .little, as: LongLen.self) else {
            return nil
        }
        return val
    }
}
