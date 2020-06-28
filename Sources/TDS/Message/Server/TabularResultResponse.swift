import Logging
import NIO
import Foundation

extension TDSMessage {
    public struct TabularResultResponse: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType = .tabularResult

        var tokens: [TDSToken]

        static public func parse(from buffer: inout ByteBuffer) throws -> TDSMessage.TabularResultResponse {
            let tokens = try TDSMessage.parseTokenDataStream(messageBuffer: &buffer)
            return .init(tokens: tokens)
        }
    }
}

