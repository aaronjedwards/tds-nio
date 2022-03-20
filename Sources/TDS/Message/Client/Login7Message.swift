import Logging
import NIO
import Foundation
#if os(iOS)
import UIKit.UIDevice
#endif

extension TDSMessages {
    /// `LOGIN7`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/773a62b6-ee89-4c02-9e5e-344882630aac
    public struct Login7Message: TDSMessagePayload {
        public static var packetType: TDSPacket.HeaderType {
            return .tds7Login
        }

        static var clientPID = UInt32(ProcessInfo.processInfo.processIdentifier)
        
        var username: String
        var password: String
        var serverName: String
        var database: String
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            
            var hostName = ""
            
            #if os(macOS)
            hostName = Host.current().name ?? ""
            #endif
            
            // iOS hostname requires parsing the device name and formatting for hostname equivalant.
            #if os(iOS)
            let device = UIDevice().name.replacingOccurrences(of: " ", with: "-")
            hostName = device + ".local"
            #endif
            
            // Each basic field needs to serialize the length & offset
            let basicFields = [
                (hostName, false),
                (username, false),
                (password, true),
                ("", false),
                (serverName, false),
                ("", false), // unused field
                ("swift-tds", false),
                ("", false),
                (database, false)
            ]
            
            // ClientID serializes inbetween `basicFields` and `extendedFields`
            let clientId: [UInt8] = [0x00, 0x50, 0x8b, 0xe3, 0xb7, 0x8f]
            
            // Each extended field needs to serialize the length & offset
            let extendedFields = [
                ("", false),
                ("", false),
                ("", true)
            ]
            
            // Stores the position and skips an UInt32 so the length can be added later
            let login7HeaderPosition = buffer.writerIndex
            buffer.moveWriterIndex(forwardBy: 4)
            
            buffer.writeBytes([
                0x02, 0x00, 0x09, 0x72, // TDS version
                0x00, 0x10, 0x00, 0x00, // Packet length negotiation
                0x00, 0x00, 0x00, 0x01, // Client version, 0x07 in example
            ])
            
            buffer.writeInteger(Self.clientPID)
            buffer.writeInteger(0 as UInt32) // Connection ID
            buffer.writeInteger(0xE0 as UInt8) // Flags1
            buffer.writeInteger(0x03 as UInt8) // Flags2
            buffer.writeInteger(0 as UInt8) // Flags
            buffer.writeInteger(0 as UInt8) // Flags3
            buffer.writeInteger(0 as UInt32) // Timezone
            buffer.writeBytes([0x09, 0x04, 0x00, 0x00]) // ClientLCID
            
            var offsetLengthsPosition = buffer.writerIndex
            buffer.moveWriterIndex(forwardBy: basicFields.count * 4)
            buffer.writeBytes(clientId)
            
            buffer.moveWriterIndex(forwardBy: extendedFields.count * 4)
            
            buffer.writeInteger(0 as UInt32) // SSPI
            
            func writeField(_ string: String, isPassword: Bool) {
                let utf16 = string.utf16
                
                // TODO: Will someone realistically add 64KB of data in a string here?
                // Is that a risk?
                buffer.setInteger(UInt16(buffer.writerIndex - login7HeaderPosition), at: offsetLengthsPosition, endianness: .little)
                offsetLengthsPosition += 2
                buffer.setInteger(UInt16(utf16.count), at: offsetLengthsPosition, endianness: .little)
                offsetLengthsPosition += 2
                
                if isPassword {
                    for character in utf16 {
                        let newHighBits = (character << 4) & 0b1111000011110000
                        let newLowBits = (character >> 4) & 0b0000111100001111
                        buffer.writeInteger((newHighBits | newLowBits) ^ 0xA5A5, endianness: .little)
                    }
                } else {
                    for character in utf16 {
                        buffer.writeInteger(character, endianness: .little)
                    }
                }
            }
            
            for (field, isPassword) in basicFields {
                writeField(field, isPassword: isPassword)
            }
            
            offsetLengthsPosition += clientId.count
            
            for (field, isPassword) in extendedFields {
                writeField(field, isPassword: isPassword)
            }
            
            buffer.setInteger(UInt32(buffer.writerIndex - login7HeaderPosition), at: login7HeaderPosition, endianness: .little)
            return
        }
    }
}
