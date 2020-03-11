import Logging
import NIO
import Foundation

extension TDSMessages {
    /// `LOGIN7`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/773a62b6-ee89-4c02-9e5e-344882630aac
    public struct Login7Message: TDSPacketType {
        private enum Login7MessageField {
            case hostname(value: String)
            case username(value: String)
            case password(value: String)
            case appName(value: String)
            case serverName(value: String)
            case clientInterfaceName(value: String)
            case language(value: String)
            case database(value: String)
            case sspiData(value: String)
            case atchDBFile(value: String)
            case changePassword(value: String)
            case unused(value: String)
            var lengthLimit: UInt16 {
                switch self {
                case .atchDBFile:
                    return 260
                default:
                    return 128
                }
            }
            var isPassword: Bool {
                switch self {
                case .password, .changePassword:
                    return true
                default:
                    return false
                }
            }
            var fieldName: String {
                switch self {
                case .hostname:
                    return "hostname"
                case .username:
                    return "username"
                case .password:
                    return "password"
                case .appName:
                    return "appName"
                case .serverName:
                    return "serverName"
                case .clientInterfaceName:
                    return "clientInterfaceName"
                case .language:
                    return "language"
                case .database:
                    return "database"
                case .sspiData:
                    return "sspiData"
                case .atchDBFile:
                    return "atchDBFile"
                case .changePassword:
                    return "changePassword"
                case .unused:
                    return "unused"
                }
            }
            func validatedValue() throws -> String.UTF16View {
                let validated: String.UTF16View
                switch self {
                case let .hostname(value), let .username(value),
                     let .password(value), let .appName(value),
                     let .serverName(value), let .clientInterfaceName(value),
                     let .language(value), let .database(value), let .sspiData(value),
                     let .atchDBFile(value), let .changePassword(value), let .unused(value):
                    validated = value.utf16
                }
                guard validated.count < self.lengthLimit else {
                    throw TDSError.invalidConnectionOptionValueLength(fieldName: self.fieldName,
                                                                      limit: self.lengthLimit)
                }
                return validated
            }
        }
        
        public static var headerType: TDSPacket.HeaderType {
            return .tds7Login
        }
        
        static var clientPID: UInt32 = UInt32(ProcessInfo.processInfo.processIdentifier)
        
        var hostname: String
        var username: String
        var password: String
        var appName: String
        var serverName: String
        var clientInterfaceName: String
        var language: String
        var database: String
        var sspiData: String
        var atchDBFile: String = "" // the filename for a database that is to be attached during the connection process
        var changePassword: String = ""
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            // Each basic field needs to serialize the length & offset
            let basicFields: [Login7MessageField] = [
                .hostname(value: hostname),
                .username(value: username),
                .password(value: password),
                .appName(value: appName),
                .serverName(value: serverName),
                .unused(value: ""), // unused field
                .clientInterfaceName(value: clientInterfaceName),
                .language(value: language),
                .database(value: database)
            ]
            
            // ClientID serializes inbetween `basicFields` and `extendedFields`
            let clientId: [UInt8] = [0x00, 0x50, 0x8b, 0xe3, 0xb7, 0x8f]
            
            // Each extended field needs to serialize the length & offset
            let extendedFields: [Login7MessageField] = [
                .sspiData(value: sspiData),
                .atchDBFile(value: atchDBFile),
                .changePassword(value: changePassword)
            ]
            
            let sspiLong: UInt32 = 0
            
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
            
            func write(field: Login7MessageField) throws {
                let utf16 = try field.validatedValue()
                
                buffer.setInteger(UInt16(buffer.writerIndex - login7HeaderPosition), at: offsetLengthsPosition, endianness: .little)
                offsetLengthsPosition += 2
                buffer.setInteger(UInt16(utf16.count), at: offsetLengthsPosition, endianness: .little)
                offsetLengthsPosition += 2
                
                if field.isPassword {
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
            
            for field in basicFields {
                try write(field: field)
            }
            
            offsetLengthsPosition += clientId.count
            
            for field in extendedFields {
                try write(field: field)
            }
            
            buffer.setInteger(UInt32(buffer.writerIndex - login7HeaderPosition), at: login7HeaderPosition, endianness: .little)
            return
        }
    }
}
