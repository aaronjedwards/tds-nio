import Logging
import NIO
import Foundation

extension TDSMessages {
    /// `Remote Procedure Call`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/26327437-aa3c-4e96-9bba-73a6e862ba21
    public struct RPCMessage: TDSMessagePayload {
        public static let packetType: TDSPacket.HeaderType = .sqlBatch

        var sqlTextOrProcedure: String
        var parameters: [String: TDSData]

        public func serialize(into buffer: inout ByteBuffer) throws {
            TDSMessage.serializeAllHeaders(&buffer)
            buffer.writeUTF16String(sqlTextOrProcedure)
            return
        }
    }
}
