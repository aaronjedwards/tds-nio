import Logging
import NIO

/// Input parameter of RPC and data to be sent.
/// - Parameter name: Name of in parameter.
/// - Parameter data: Data to be sent with input parameter. Nil for out parameters.
public struct RPCInputParameter {
    var name: String
    var inputValue: RPCParamData? = nil
}

/// Data to be sent with input paramter. Default datatype Varchar.
/// - Parameter name: Name out parameter.
/// - Parameter dataType: Datatype of data being sent with input parameter.
public struct RPCParamData {
    var value: Any
    var valueType: TDSDataType
}

/// Output parameter of RPC
/// - Parameter name: Name out parameter.
/// - Parameter dataType: Datatype of output Parameter.
public struct RPCOutputParameter {
    var name: String
    var dataType: TDSDataType
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
            buffer.writeBytes([0x00, 0x00]) // Option Flags
            try inputParameters?.forEach { try addInputParameter(param: $0, buffer: &buffer) }
            try outputParameters?.forEach { try addOutputParameter(param: $0, buffer: &buffer)}
            
            return
        }
        
        fileprivate func addInputParameter(param: RPCInputParameter, buffer: inout ByteBuffer) throws {
            
            let name = "@" + param.name
            buffer.writeInteger(Byte(name.count)) // Name length
            buffer.writeUTF16String(name) // Parameter name
            buffer.writeInteger(0 as Byte) // Status flags. fByRefValue = 0, fDefaultValue = 0
            
            // End of function if no data passed with parameter
            guard let inputValue = param.inputValue else { return }
            
            try addData(value: inputValue.value, type: inputValue.valueType, buffer: &buffer)
            
        }
        
        fileprivate func addData(value: Any, type: TDSDataType, buffer: inout ByteBuffer) throws {
            
            // SQLSERVER returns unknown data type for .int 0x38. Tedius overcomes this by mapping .Int to .Intn with type 0x26 and data length 4 Bytes.
            // TODO: This is a bit hacky. It would probably be better to have an internal Typedata enum and public facing enum to resolve these issues
            var typeCopy = type
            if typeCopy == .int {
                typeCopy = TDSDataType.intn
                buffer.writeBytes([typeCopy.rawValue])
            } else {
                buffer.writeBytes([type.rawValue])
            }
            
            
            // Switch on data type to set encoding and maximal length
            switch(value) {
            case let s as String:
                // TYPE_INFO Value type maximal length
                buffer.writeInteger(UShort(s.count), endianness: .little)
                
                // Collation
                // LCID: 0x0409 Latin1_General_CI_AS
                // Ignore case: True
                // Ignore accent: False
                // Ignore kana: True
                // Ignore width: True
                // Binary: False
                // Version: 0
                // SortId: 52
                buffer.writeBytes([0x09, 0x04, 0xd0, 0x00, 0x34])
                // String length
                buffer.writeInteger(UShort(s.count), endianness: .little)
                
                switch(typeCopy) {
                case .charLegacy, .varcharLegacy, .char, .varchar, .text:// UTF-8 Encoding
                    buffer.writeUTF8String(s)
                case .nvarchar, .nchar, .nText:// UTF-16 Encoding
                    buffer.writeUTF16String(s)
                default:
                    throw TDSError.protocolError("\(type) not implemented for String value")
                }
            case let i as Int:
                switch(typeCopy) {
                case .tinyInt:
                    // TYPE_INFO Value type maximal length
                    buffer.writeInteger(Byte(1))
                    // String length
                    buffer.writeInteger(Byte(1))
                    // Write value
                    buffer.writeInteger(Byte(i))
                case .smallInt:
                    // TYPE_INFO Value type maximal length
                    buffer.writeInteger(Byte(2))
                    // String length
                    buffer.writeInteger(Byte(2))
                    // Write value
                    buffer.writeInteger(Short(i), endianness: .little)
                    
                case .intn:
                    // TYPE_INFO Value type maximal length
                    buffer.writeInteger(Byte(4))
                    // String length
                    buffer.writeInteger(Byte(4))
                    // Write value
                    buffer.writeInteger(Long(i), endianness: .little)
                    
                case .bigInt:
                    // TYPE_INFO Value type maximal length
                    buffer.writeInteger(Byte(8))
                    // String length
                    buffer.writeInteger(Byte(8))
                    // Write value
                    buffer.writeInteger(LongLong(i), endianness: .little)
                    
                default:
                    throw TDSError.protocolError("\(type) not implemented for Int value")
                }
                // TODO: Implement passing Floats and Date as parameters
//            case let f as Float:
//                throw TDSError.protocolError(" Type Float not implemented")
//                switch(type) {
//                case .float:
//                    // TYPE_INFO Value type maximal length
//                    buffer.writeInteger(Byte(8))
//                    // String length
//                    buffer.writeInteger(Byte(8))
//                    // Write value
//
//                case .real:
//                    // TYPE_INFO Value type maximal length
//                    buffer.writeInteger(Byte(4))
//                    // String length
//                    buffer.writeInteger(Byte(4))
//                    // Write value
//
//                default:
//                    throw TDSError.protocolError("\(type) not implemented for Float value")
//                }
//            case let d as Date:
//                TDSError.protocolError("Paramdata value of type Date not implemented")
            default:
                throw TDSError.protocolError("Unknown data type passed as value")
            }
            
        }
    }
    
}

fileprivate func addOutputParameter(param: RPCOutputParameter, buffer: inout ByteBuffer) throws {
    
    let name = "@" + param.name
    buffer.writeInteger(Byte(name.count)) // Name length
    buffer.writeUTF16String(name) // Paramter name
    buffer.writeInteger(Byte(1)) // Status Flags. fByRefValue = 1, fDefaultValue = 0
    // SQLSERVER returns unknown data type for .int 0x38. Tedius overcomes this by mapping .Int to .Intn with type 0x26 and data length 4 Bytes.
    // TODO: This is a bit hacky. It would probably be better to have an internal Typedata enum and public facing enum to resolve these issues
    var typeCopy = param.dataType
    if typeCopy == .int {
        typeCopy = TDSDataType.intn
        buffer.writeBytes([typeCopy.rawValue])
    } else {
        buffer.writeBytes([typeCopy.rawValue])
    }
    
    switch(typeCopy) {
    case .charLegacy, .varcharLegacy, .char, .varchar, .text, .nvarchar, .nchar, .nText:// UTF-8 Encoding
        // TYPE_INFO Value type maximal length
        buffer.writeInteger(UShort(8000), endianness: .little)
        
        // Collation
        // LCID: 0x0409 Latin1_General_CI_AS
        // Ignore case: True
        // Ignore accent: False
        // Ignore kana: True
        // Ignore width: True
        // Binary: False
        // Version: 0
        // SortId: 52
        buffer.writeBytes([0x09, 0x04, 0xd0, 0x00, 0x34])
        // No output data so set length to CHARBIN_NULL
        buffer.writeBytes([0xFF, 0xFF])
    case .tinyInt:
        // TYPE_INFO Value type maximal length
        buffer.writeInteger(Byte(1))
        // String length
        buffer.writeInteger(Byte(0))
    case .smallInt:
        // TYPE_INFO Value type maximal length
        buffer.writeInteger(Byte(2))
        // String length
        buffer.writeInteger(Byte(0))
    case .intn:
        // TYPE_INFO Value type maximal length
        buffer.writeInteger(Byte(4))
        // String length
        buffer.writeInteger(Byte(0))
        
    case .bigInt:
        // TYPE_INFO Value type maximal length
        buffer.writeInteger(Byte(8))
        // String length
        buffer.writeInteger(Byte(0))
        
    default:
        throw TDSError.protocolError("\(typeCopy) not implemented for Int value")
    }
}
