import Logging
import NIO
import Foundation

extension TDSMessages {
    public struct TabularResponse: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType {
            return .tabularResult
        }

        var tokens: [TDSToken]

        static public func parse(from buffer: inout ByteBuffer) throws -> TDSMessages.TabularResponse {
            let parser = TokenStreamParser()
            let tokens = try parser.parse(from: &buffer)
            return .init(tokens: tokens)
        }
    }
}

