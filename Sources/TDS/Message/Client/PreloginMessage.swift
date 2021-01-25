import NIO

extension TDSMessage {
    /// `PRELOGIN`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/60f56408-0188-4cd5-8b90-25c6f2423868
    public struct PreloginMessage: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType {
            return .prelogin
        }
        
        public static let messageLength: Byte = 0x1A // (26 bytes)
        
        public var version: String
        public var encryption: PreloginEncryption?
        
        public init(version: String, encryption: PreloginEncryption?) {
            self.version = version
            self.encryption = encryption
        }
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            // Token List: 0x09 - 0x0E (6 bytes)
            //
            // Follows the form of:
            // - Token (1 byte)
            // - Offset from start of packet (2 btyes)
            // - Length in # of bytes (2 bytes)
            
            // Version (Required)
            buffer.writeBytes([
                0x00,
                0x00, 0x0B,
                0x00, 0x06,
            ])
            
            // Encryption
            if encryption != nil {
                buffer.writeBytes([
                    0x01,
                    0x00, 0x11,
                    0x00, 0x01,
                ])
            }
            
            // TODO - Add support for other options
            
            buffer.writeBytes([
                0xff // Terminator
            ])
            
            // Data
            
            // Version Data
            buffer.writeBytes([
                0x09, 0x00, 0x00, 0x00,     // UL_VERSION (9.0.0)
                0x00, 0x00,                 // US_SUBBUILD (0)
            ])
            
            // Encryption Data
            if let enc = encryption {
                buffer.writeBytes([
                    enc.rawValue
                ])
            }
        }
    }
}

public struct PreloginOption {
    /// `PL_OPTION_TOKEN`
    var token: TDSMessage.PreloginToken
    /// `PL_OFFSET`
    var offset: UShort
    /// `PL_OPTION_LENGTH`
    var length: UShort
}

extension TDSMessage {
    public enum PreloginToken: Byte {
        /// VERSION
        case version = 0x00
        
        /// ENCRYPTION
        case encryption = 0x01
        
        /// INSTOPT
        case instOpt = 0x02
        
        /// THREADID
        case threadId = 0x03
        
        /// MARS
        case mars = 0x04
        
        /// TRACEID
        case traceId = 0x05
        
        // FEDAUTHREQUIRED
        case fedAuthRequired = 0x06
        
        // NONCEOPT
        case nonceOpt = 0x07
        
        // TERMINATOR
        case terminator = 0xFF
    }
}

extension TDSMessage {
    public enum PreloginEncryption: Byte {
        case encryptOff = 0x00
        case encryptOn = 0x01
        case encryptNotSup = 0x02
        case encryptReq = 0x03
        case encryptClientCertOff = 0x80
        case encryptClientCertOn = 0x81
        case encryptClientCertReq = 0x83
    }
}
