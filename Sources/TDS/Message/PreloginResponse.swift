import NIO

extension TDSMessages {
    /// `PRELOGIN`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/60f56408-0188-4cd5-8b90-25c6f2423868
    public struct PreloginResponse: TDSPacketType {
        public static var headerType: TDSPacket.HeaderType {
            return .preloginResponse
        }
        
        public var body: Prelogin
        
        public init(version: String, encryption: PreloginEncryption?) {
            body = Prelogin(version: version, encryption: encryption)
        }
        
        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> PreloginResponse {
            var _buffer = buffer
            var preloginOptions: [PreloginOption] = []
            var readOptions = true
            
            // Parse *PRELOGIN_OPTION
            while readOptions {
                // Check if we have parsed at least the required VERSION token
                guard let mappedToken = _buffer.readInteger(as: Byte.self).map(PreloginToken.init), let token = mappedToken else {
                    throw TDSError.protocol("Invalid Prelogin Response: Invalid PL_OPTION_TOKEN value.")
                }
                
                if preloginOptions.count > 0 {
                    // Check if the token is the TERMINATOR token.
                    // This indicates that there are no more PRELOGIN_OPTIONs
                    if token == PreloginToken.terminator {
                        readOptions = false
                        break
                    }
                }
                
                // Read PRELOGIN_OPTION
                guard
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
                preloginOptions.first(where: { $0.token == PreloginToken.version }) != nil
                else {
                    throw TDSError.protocol("Invalid Prelogin Response: Missing required VERSION option.")
            }
            
            var versionValue: String?
            var encryption: PreloginEncryption?
            
            for option in preloginOptions {
                guard
                    var optionData = _buffer.getSlice(at: Int(option.offset), length: Int(option.length))
                    else {
                        throw TDSError.protocol("Invalid Prelogin Response: Error while parsing PL_OPTION_DATA")
                }
                
                switch option.token {
                case .version:
                    // Parse VERSION
                    guard
                        let majorVersion = optionData.readInteger(as: Byte.self),
                        let minorVersion = optionData.readInteger(as: Byte.self),
                        let buildNumber = optionData.readInteger(as: UShort.self),
                        let subBuild = optionData.readInteger(as: UShort.self)
                        else {
                            throw TDSError.protocol("Invalid Prelogin Response: Invalid VERSION option data.")
                    }
                    
                    versionValue = "\(majorVersion).\(minorVersion).\(buildNumber).\(subBuild)"
                case .encryption:
                    // Parse VERSION
                    guard
                        let encryptionValue = optionData.readInteger(as: Byte.self).map(PreloginEncryption.init)
                        else {
                            throw TDSError.protocol("Invalid Prelogin Response: Invalid ENCRYPTION option data.")
                    }
                    
                    encryption = encryptionValue
                    
                default:
                    break
                }
            }
            
            guard let lastOption = preloginOptions.last else {
                throw TDSError.protocol("Invalid Prelogin Response: Should be at least 1 PRELOGIN_OPTION.")
            }
            
            // Read all bytes that were a part of this message from the buffer
            let totalLength = Int(lastOption.offset + lastOption.length)
            _ = buffer.readBytes(length: totalLength)
            
            guard let version = versionValue else {
                throw TDSError.protocol("Invalid Prelogin Response: Missing required VERSION data.")
            }
            
            let response = PreloginResponse(version: version, encryption: encryption)
            return response
        }
    }
}
