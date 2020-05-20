import NIO

extension ByteBuffer {
    mutating func writeUTF16String(_ str: String, endianness: Endianness = .little) {
        for character in str.utf16 {
            self.writeInteger(character, endianness: endianness)
        }
    }

    mutating func readBVarchar() -> String? {
        guard
            let bytes = self.readInteger(as: UInt8.self),
            let utf16 = self.readUTF16String(length: Int(bytes))
        else {
            return nil
        }
        return utf16
    }

    mutating func readUSVarchar() -> String? {
        guard
            let bytes = self.readInteger(as: UInt16.self),
            let utf16 = self.readUTF16String(length: Int(bytes))
        else {
            return nil
        }
        return utf16
    }

    mutating func readBVarbyte() -> [UInt8]? {
        guard
            let numBytes = self.readInteger(as: UInt8.self),
            let bytes = self.readBytes(length: Int(numBytes))
        else {
            return nil
        }
        return bytes
    }

    mutating func readUSVarbyte() -> [UInt8]? {
        guard
            let numBytes = self.readInteger(as: UInt16.self),
            let bytes = self.readBytes(length: Int(numBytes))
        else {
            return nil
        }
        return bytes
    }

    mutating func readLVarbyte() -> [UInt8]? {
        guard
            let numBytes = self.readInteger(as: UInt32.self),
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
}
