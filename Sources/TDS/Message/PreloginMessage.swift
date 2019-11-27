import NIO

/// `PRELOGIN`
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/60f56408-0188-4cd5-8b90-25c6f2423868
import NIO

extension TDSMessage {
    /// Authentication request returned by the server.
    public struct Prelogin: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType {
            return .preLogin
        }
        
        public static let messageLength: Byte = 0x14 // (20 bytes)
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            // Packet Header: 0x00 - 0x08 (8 bytes)
            buffer.writeBytes([
                Prelogin.headerType.value,                             // Type
                0x01,                                   // Status
                0x00, Prelogin.messageLength,    // Length
                0x00, 0x00,                             // SPID
                0x00,                                   // PacketID (Unused)
                0x00                                    // Window (Unused)
            ])
            
            // Token List: 0x09 - 0x0E (6 bytes)
            //
            // Follows the form of:
            // - Token (1 byte)
            // - Offset from start of packet (2 btyes)
            // - Length in # of bytes (2 bytes)
            buffer.writeBytes([
                // Version (Required)
                0x00,
                0x00, 0x06,
                0x00, 0x06,
                // TODO - Add support for other options
                0xff // Terminator
            ])
            
            // Data: 0x0f - 0x14 (6 bytes)
            buffer.writeBytes([
                // Version Data
                0x09, 0x00, 0x00, 0x00,     // UL_VERSION (9.0.0)
                0x00, 0x00                  // US_SUBBUILD (0)
                // TODO - Add support for other options
            ])
        }
    }
}
