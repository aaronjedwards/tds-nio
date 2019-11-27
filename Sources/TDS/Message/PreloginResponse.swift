import NIO

extension TDSMessage {
    /// Authentication request returned by the server.
    public struct PreloginResponse: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType {
            return .preLoginResponse
        }
        
        public var version: String
        
        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> PreloginResponse {
            var _buffer = buffer
            var preloginOptions: [PreloginOption] = []
            var readOptions = true
            
            // Parse *PRELOGIN_OPTION
            while readOptions {
                // Check if we have parsed at least the required VERSION token
                if preloginOptions.count > 0 {
                    // Peek into buffer to see if the TERMINATOR token is present.
                    // This indicates that there are no more PRELOGIN_OPTIONs
                    var peek = _buffer
                    if let terminator = peek.readInteger(as: Byte.self), terminator == 0xFF {
                        // Found TERMINATOR token, stop parsing PRELOGIN_OPTION
                        _buffer = peek
                        readOptions = false
                        break
                    }
                }
                
                // Read PRELOGIN_OPTION
                guard
                    let token = _buffer.readInteger(as: Byte.self),
                    let offset = _buffer.readInteger(as: UShort.self),
                    let length = _buffer.readInteger(as: UShort.self)
                    else {
                        throw TDSError.protocol("Invalid Prelogin Response: Invalid *PRELOGIN_OPTION segment.")
                }
                
                let option = PreloginOption(token: token, offset: offset, length: length)
                preloginOptions.append(option)
            }
            
            // Parse PL_OPTION_DATA
            // Reset _buffer
            _buffer = buffer
            
            // Parse VERSION
            guard
                let versionOption = preloginOptions.first(where: { $0.token == 0x00 }),
                var versionData = _buffer.getSlice(at: Int(versionOption.offset), length: Int(versionOption.length))
                else {
                    throw TDSError.protocol("Invalid Prelogin Response: Missing required VERSION option.")
            }
            
            guard
                let version = versionData.readInteger(as: ULong.self),
                let subBuild = versionData.readInteger(as: UShort.self)
                else {
                    throw TDSError.protocol("Invalid Prelogin Response: Invalid VERSION option data.")
            }
            
            guard let lastOption = preloginOptions.last else {
                throw TDSError.protocol("Invalid Prelogin Response: Should be at least 1 PRELOGIN_OPTION.")
            }
            
            // Read all bytes that were a part of this message from the buffer
            let totalLength = Int(lastOption.offset + lastOption.length)
            _ = buffer.readBytes(length: totalLength)
            
            let response = PreloginResponse(version: "\(version).\(subBuild)")
            return response
        }
    }
}

public struct PreloginOption {
    /// `PL_OPTION_TOKEN`
    var token: Byte
    /// `PL_OFFSET`
    var offset: UShort
    /// `PL_OPTION_LENGTH`
    var length: UShort
}
