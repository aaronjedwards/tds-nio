import Logging
import NIO
import Foundation

extension TDSMessages {
    /// `LOGIN7`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protoparsecols/ms-tds/773a62b6-ee89-4c02-9e5e-344882630aac
    public struct LoginResponse: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType {
            return .loginResponse
        }

        var tokens: [Token]

        static public func parse(from buffer: inout ByteBuffer) throws -> TDSMessages.LoginResponse {
            let tokens = try TDSMessages.parseTokenDataStream(messageBuffer: &buffer)
            return .init(tokens: tokens)
        }
    }
}

