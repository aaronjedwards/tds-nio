import Logging
import NIO
import Foundation

// Used to log Hex value of buffers during testing
extension Data {
    struct HexEncodingOptions: OptionSet {
        let rawValue: Int
        static let upperCase = HexEncodingOptions(rawValue: 1 << 0)
    }

    func hexEncodedString(options: HexEncodingOptions = []) -> String {
        let format = options.contains(.upperCase) ? "%02hhX" : "%02hhx"
        return self.map { String(format: format, $0) }.joined()
    }

}

extension TDSMessages {
    /// `RPC Request`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/619c43b6-9495-4a58-9e49-a4950db245b3
    /// - Parameter procName: Name of procedure to be called
    /// - Parameter parameters: Array of input and output parameters for procedure
    public struct RPCMessage: TDSMessagePayload {
        public static let packetType: TDSPacket.HeaderType = .rpc
        
        var procName: String
        var inputParameters: [RPCInputParameter]? = nil
        var outputParameters: [RPCOutputParameter]? = nil
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            TDSMessage.serializeAllHeaders(&buffer)
            buffer.writeInteger(UShort(procName.count), endianness: .little, as: UShort.self) // Set proc name count
            buffer.writeUTF16String(procName)
            buffer.writeInteger(0 as UShort) // Option Flags
            
            try inputParameters?.forEach { try addInputParameter(param: $0, buffer: &buffer) }
            try outputParameters?.forEach { try addOutputParameter(param: $0, buffer: &buffer)}
            
            return
        }
        
        fileprivate func addInputParameter(param: RPCInputParameter, buffer: inout ByteBuffer) throws {
            
            let name = "@" + param.name
            buffer.writeInteger(Byte(name.count)) // Name length
            buffer.writeUTF16String(name) // Parameter name
            buffer.writeInteger(0 as Byte) // Status flags
            
            // End of function if no data passed with parameter
            guard let data = param.data else { return }
            
            // TODO: Incomplete data types
            switch(data.data) {
            case let s as String:
                try addStringData(str: s, type: data.dataType, buffer: &buffer)
            case let _ as Int:
                throw TDSError.protocolError("Int input not setup for use")
            case let _ as Float:
                throw TDSError.protocolError("Float input not setup for use")
            case let _ as Date:
                throw TDSError.protocolError("Date input not setup for use")
            default:
                throw TDSError.protocolError("Parameter type not configured for use")
                
            }
        }
        
        fileprivate func addStringData(str: String, type: TDSDataType, buffer: inout ByteBuffer) throws {
            buffer.writeInteger(UShort(str.count), endianness: .little, as: UShort.self) // String length
            
            //TODO: Define these flags. Should they be user options?
            // Flags 09 04 D0 00 34
            buffer.writeInteger(0x09 as Byte)
            buffer.writeInteger(0x04 as Byte)
            buffer.writeInteger(0xd0 as Byte)
            buffer.writeInteger(0x00 as Byte)
            buffer.writeInteger(0x34 as Byte)
            
            buffer.writeInteger(UShort(str.count), endianness: .little, as: UShort.self) // String length again for some reason?
            
            switch(type) {
            case .charLegacy, .varcharLegacy, .char, .varchar, .text:// UTF-8 Encoding
                buffer.writeUTF8String(str)
            case .nvarchar, .nchar, .nText:// UTF-16 Encoding
                buffer.writeUTF16String(str)
            default:
                throw TDSError.protocolError("\(type) not compatable with type String")
            }
            
        }
        
        fileprivate func addOutputParameter(param: RPCOutputParameter, buffer: inout ByteBuffer) throws {
            
            let name = "@" + param.name
            buffer.writeInteger(Byte(name.count)) // Name length
            buffer.writeUTF16String(name) // Paramter name
            buffer.writeInteger(1 as Byte) // Status Flags

        }
    }
}


/// Input parameter of RPC and data to be sent.
/// - Parameter name: Name of in parameter.
/// - Parameter data: Data to be sent with input parameter. Nil for out parameters.
public struct RPCInputParameter {
    var name: String
    var data: RPCParamData? = nil
}

/// Output parameter of RPC
/// - Parameter name: Name out parameter.
public struct RPCOutputParameter {
    var name: String
}


/// Data to be sent with input paramter. Default datatype Varchar.
public struct RPCParamData {
    var data: Any
    var dataType: TDSDataType = .varchar
}
