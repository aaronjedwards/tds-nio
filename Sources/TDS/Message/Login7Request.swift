import Logging
import NIO
import Foundation

extension TDSConnection {
    public func login(username: String, password: String, database: String = "master") -> EventLoopFuture<Void> {
        let auth = TDSMessages.Login7Request(
            hostname: "localhost",
            username: username,
            password: password,
            appName: "TDSTester",
            serverName: "",
            clientInterfaceName: "SwiftTDS",
            language: "",
            database: database,
            sspiData: "")
        return self.send(Login7Request(login: auth))
    }
}

struct Login7Request: TDSRequest {
    let login: TDSMessages.Login7Request
    
    func respond(to message: TDSMessage, allocator: ByteBufferAllocator) throws -> TDSMessage? {
        var messageBuffer = message.firstPacket.messageBuffer
        
        guard
            let token = messageBuffer.readInteger(as: UInt8.self),
            let tokenType = TDSMessages.TokenType(rawValue: token)
        else {
            throw TDSError.protocolError("Invalid token type in Login7 response")
        }
        
        switch tokenType {
        case .error:
            throw TDSError.invalidCredentials
        }
        
        return nil
    }
    
    func start(allocator: ByteBufferAllocator) throws -> TDSMessage {
        let packet = try TDSPacket(message: login, isLastPacket: true, allocator: allocator)
        return TDSMessage(packets: [packet])
    }
    
    func log(to logger: Logger) {
        logger.log(level: .debug, "Logging in as \(login.username)")
    }
}

extension TDSMessages {
    enum TokenType: UInt8 {
        case error = 0xaa
    }
    
    struct Login7Response {
    }
    
    /// `LOGIN7`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/773a62b6-ee89-4c02-9e5e-344882630aac
    public struct Login7Request: TDSPacketType {
        public static var headerType: TDSPacket.HeaderType {
            return .tds7Login
        }
        
        static var tdsVersion: TDSVersion = .tds7_4
        static var packetSize: DWord = DWord(TDSPacket.defaultPacketLength)
        static var clientPID: DWord = DWord(ProcessInfo.processInfo.processIdentifier)
        static var connectionId: DWord = 100
        
        var hostname: String
        var username: String
        var password: String
        var appName: String
        var serverName: String
        var clientInterfaceName: String
        var language: String
        var database: String
        var sspiData: String
        var atchDBFile: String = "" // TODO: What is this?
        var changePassword: String = ""
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            // Each basic field needs to serialize the length & offset
            let basicFields = [
                hostname,
                username,
                password,
                appName,
                serverName,
                "", // unused field
                clientInterfaceName,
                language,
                database
            ]
            
            // ClientID serializes inbetween `basicFields` and `extendedFields`
            let clientId: [UInt8] = [0x00, 0x50, 0x8b, 0xe3, 0xb7, 0x8f]
            
            // Each extended field needs to serialize the length & offset
            let extendedFields = [
                sspiData,
                atchDBFile,
                changePassword
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
            
            func writeField(_ string: String) {
                let utf16 = string.utf16
                
                // TODO: Will someone realistically add 64KB of data in a string here?
                // Is that a risk?
                buffer.setInteger(UInt16(buffer.writerIndex - TDSPacket.Header.length), at: offsetLengthsPosition, endianness: .little)
                offsetLengthsPosition += 2
                buffer.setInteger(UInt16(utf16.count), at: offsetLengthsPosition, endianness: .little)
                offsetLengthsPosition += 2
                
                for character in utf16 {
                    buffer.writeInteger(character, endianness: .little)
                }
            }
            
            for field in basicFields {
                writeField(field)
            }
            
            offsetLengthsPosition += clientId.count
            
            for field in extendedFields {
                writeField(field)
            }
            
            buffer.setInteger(UInt32(buffer.writerIndex - login7HeaderPosition), at: login7HeaderPosition, endianness: .little)
            return
        }
    }
}

public struct Login {
    /// Header
    
    /// Total length of the LOGIN7 structure exluding the header
    var length: DWord
    
    /// Highest TDS Version
    var tdsVersion: TDSVersion
    var packetSize: DWord
    var clientPID: DWord
    var connectionId: DWord
    var optionFlags1: Byte
    var optionFlags2: Byte
    var typeFlags: Byte
    var optionFlags3: Byte

    /// This field is not used and can be set to zero.
    static let clientTimeZone: Long = 0
    
    /// Note The ClientLCID value is no longer used to set language parameters and is ignored.
    var clientLCID: ULong = 0
    
    /// Data
    var hostname: String
    var username: String
    var password: String
    var appName: String
    var serverName: String
    var clientInterfaceName: String
    var language: String
    var database: String
    var clientId: String
    var sspiData: String
    var attachDBFile: String
    var changePassword: String
}

public struct OffsetLength {
    /// ibHostName & cchHostName
    var hostnameOffset: UShort
    var hostnameLength: UShort
    
    /// ibUserName & cchUserName
    var usernameOffset: UShort
    var usernameLength: UShort
    
    /// ibPassword & cchPassword
    var passwordOffset: UShort
    var passwordLength: UShort
    
    /// ibAppName & cchAppName
    var appNameOffset: UShort
    var appNameLength: UShort
    
    /// ibServerName & cchServerName
    var serverNameOffset: UShort
    var serverNameLength: UShort
    
    /// (ibUnused / ibExtension) & (cbUnused / cbExtension)
    var unusedOrExtensionOffset: UShort
    var unusedOrExtensionLength: UShort
    
    /// ibCltIntName & cchCltIntName
    var clientInterfaceNameOffset: UShort
    var clientInterfaceNameLength: UShort
    
    /// ibLanguage & cchLanguage
    var languageOffset: UShort
    var languageLength: UShort
    
    /// ibDatabase & cchDatabase
    var databaseOffset: UShort
    var databaseLength: UShort
    
    /// ClientID
    var ClientId: [Byte]
    
    /// ibSSPI & cbSSPI
    var sspiOffset: UShort
    var sspiLength: UShort
    
    /// ibAtchDBFile & cchAtchDBFile
    var attachDbFileOffset: UShort
    var attachDbFileLength: UShort
    
    /// ibChangePassword & cchChangePassword
    var changePasswordOffset: UShort
    var changePasswordLength: UShort
    
    /// cbSSPILong
    var cbSSPILong: DWord
}

public enum TDSVersion: DWord {
    case tds7_4 = 0x04000074
}
