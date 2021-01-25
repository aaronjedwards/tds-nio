import NIO

extension ByteBuffer {
    mutating func writeUTF16String(_ str: String, endianness: Endianness = .little) {
        for character in str.utf16 {
            self.writeInteger(character, endianness: endianness)
        }
    }

    mutating func writeDouble(_ double: Double) {
        self.writeInteger(double.bitPattern)
    }

    mutating func readBVarchar() -> String? {
        guard
            let bytes = self.readByte(),
            let utf16 = self.readUTF16String(length: Int(bytes) * 2)
        else {
            return nil
        }
        return utf16
    }

    mutating func readUSVarchar() -> String? {
        guard
            let bytes = self.readUShort(),
            let utf16 = self.readUTF16String(length: Int(bytes) * 2)
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
            let bytes = self.readBytes(length: length),
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

    mutating func readFloat(endianness: Endianness = .big) -> Float? {
       precondition(MemoryLayout<Float>.size == MemoryLayout<UInt32>.size)
        return self.readInteger(endianness: endianness, as: UInt32.self).map { Float(bitPattern: $0) }
   }

    mutating func readDouble(endianness: Endianness = .big) -> Double? {
       precondition(MemoryLayout<Double>.size == MemoryLayout<UInt64>.size)
       return self.readInteger(endianness: endianness, as: UInt64.self).map { Double(bitPattern: $0) }
   }
    
    mutating func readSmallMoney() -> Double? {
        guard let value = self.readInteger(endianness: .little, as: Int32.self) else {
            return nil
        }
        return Double(value) / Double(10000)
    }

    mutating func readMoney() -> Double? {
        guard
            let high = self.readInteger(endianness: .little, as: UInt32.self),
            let low = self.readInteger(endianness: .little, as: UInt32.self)
        else {
            return nil
        }
        
        let value = Int64(high) << 32 | Int64(low)
        return Double(value) / Double(10000)
    }

    mutating func read3ByteInt() -> UInt32? {
        guard let bytes = self.readBytes(length: 3) else {
            return nil
        }
        
        var value: UInt32 = 0
        value += numericCast(bytes[0]) << 16
        value += numericCast(bytes[1]) << 8
        value += numericCast(bytes[2]) << 0
        
        return value
    }
    
    mutating func read5ByteInt() -> UInt64? {
        guard let bytes = self.readBytes(length: 5) else {
            return nil
        }
        
        var value: UInt64 = 0
        value += numericCast(bytes[0]) << 32
        value += numericCast(bytes[1]) << 24
        value += numericCast(bytes[2]) << 16
        value += numericCast(bytes[3]) << 8
        value += numericCast(bytes[4]) << 0
        
        return value
    }
    
    mutating func readByteLengthInteger<I: FixedWidthInteger>(length: Int, as: I.Type = I.self) -> I? {
        guard let bytes = self.readBytes(length: length) else {
            return nil
        }
        
        var value: I = 0
        for i in 0...bytes.count - 1 {
            value += numericCast(bytes[i]) << (i * 8)
        }
        
        return value
    }
}

internal extension Sequence where Element == UInt8 {
    func hexdigest() -> String {
        return reduce("") { $0 + String(format: "%02x", $1) }
    }
}
