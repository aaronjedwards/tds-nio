import NIO
import Foundation

extension TDSMessage {
    /// `LOGIN7`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/773a62b6-ee89-4c02-9e5e-344882630aac
    public struct Login7Message: TDSMessageType {
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
        
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            // Packet Header: 0x00 - 0x08 (8 bytes)
            buffer.writeBytes([
                Login7Message.headerType.value,         // Type
                0x01,                                   // Status
                0x00, PreloginMessage.messageLength,    // Length
                0x00, 0x00,                             // SPID
                0x00,                                   // PacketID (Unused)
                0x00                                    // Window (Unused)
            ])
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
