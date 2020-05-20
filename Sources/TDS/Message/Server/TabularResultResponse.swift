import Logging
import NIO
import Foundation

extension TDSMessages {
    public struct TabularResultResponse: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType = .tabularResult

        var tokens: [Token]

        static public func parse(from buffer: inout ByteBuffer) throws -> TDSMessages.TabularResultResponse {
            let tokens = try TDSMessages.parseTokenDataStream(messageBuffer: &buffer)
            return .init(tokens: tokens)
        }
    }
}

