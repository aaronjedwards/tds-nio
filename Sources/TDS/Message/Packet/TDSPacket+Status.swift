import NIO

/// Message Type. A 1-byte unsigned char
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/ce398f9a-7d47-4ede-8f36-9dd6fc21ca43
extension TDSPacket {
    public struct Status: ExpressibleByIntegerLiteral, Equatable, CustomStringConvertible {
        /// Normal message
        public static let normal = 0x00
        /// End of message
        public static let eom = 0x01
        /// From client to server
        public static let ignoreThisEvent = 0x02
        /// RESETCONNECTION
        public static let resetConnection = 0x08
        /// RESETCONNECTIONSKIPTRAN
        public static let resetConnectionSkipTran = 0x10
        
        public let value: UInt8
        
        /// See `CustomStringConvertible`.
        public var description: String {
            return String(Character(Unicode.Scalar(value)))
        }
        
        /// See `ExpressibleByIntegerLiteral`.
        public init(integerLiteral value: UInt8) {
            self.value = value
        }
    }
}
