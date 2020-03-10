import Logging
import NIO
import Foundation

extension TDSMessages {
    public struct TabularResponse: TDSPacketType {
        public static var headerType: TDSPacket.HeaderType = .tabularResult

        var tokens: [Token]

        static public func parse(from buffer: inout ByteBuffer) throws -> TDSMessages.TabularResponse {
            let tokens = try TDSMessages.parseTokenDataStream(messageBuffer: &buffer)
            return .init(tokens: tokens)
        }
    }
}

