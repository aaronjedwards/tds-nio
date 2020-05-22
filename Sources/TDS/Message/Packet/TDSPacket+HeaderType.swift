import NIO

/// Message Type. A 1-byte unsigned char
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/9b4a463c-2634-4a4b-ac35-bebfff2fb0f7
extension TDSPacket {
    public struct HeaderType: ExpressibleByIntegerLiteral, Equatable, CustomStringConvertible {
        /// SQL Batch
        public static let sqlBatch: HeaderType = 0x01
        
        /// Pre-TDS7 Login
        public static let preTDS7Login: HeaderType = 0x02
        
        /// RPC
        public static let rpc: HeaderType = 0x03
        
        /// Tabular Result
        public static let tabularResult: HeaderType = 0x04

        /// Login Response
        public static let loginResponse: HeaderType = 0x04
        
        /// Attention Signal
        public static let attentionSignal: HeaderType = 0x06
        
        /// Bulk Load
        public static let bulkLoadData: HeaderType = 0x07
        
        /// Federated Authentication Token
        public static let federatedAuthenticationToken: HeaderType = 0x08
        
        /// Transaction Manager Request
        public static let transactionManagerRequest: HeaderType = 0x0E
        
        /// TDS7 Login
        public static let tds7Login: HeaderType = 0x10
        
        /// SSPI
        public static let sspi: HeaderType = 0x11
        
        /// Pre-Login
        public static let prelogin: HeaderType = 0x12
        
        /// Pre-Login Response
        public static let preloginResponse: HeaderType = 0x04
        
        /// Non-Protocol SSL kickoff
        public static let sslKickoff: HeaderType = 0x99
        
        public let value: Byte
        
        /// See `CustomStringConvertible`.
        public var description: String {
            return String(format: "%02X", value)
        }
        
        /// See `ExpressibleByIntegerLiteral`.
        public init(integerLiteral value: UInt8) {
            self.value = value
        }
    }
}

extension ByteBuffer {
    mutating func write(headerType: TDSPacket.HeaderType) {
        self.writeInteger(headerType.value)
    }
}
