import Logging
import NIO
import Foundation

extension TDSMessages {
    /// `SQLBatch`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/f2026cd3-9a46-4a3f-9a08-f63140bcbbe3
    public struct RawSqlBatchMessage: TDSMessageType {
        public static let headerType: TDSPacket.HeaderType = .sqlBatch

        var sqlText: String

        public func serialize(into buffer: inout ByteBuffer) throws {
            TDSMessages.serializeAllHeaders(&buffer)
            buffer.writeUTF16String(sqlText)
            return
        }
    }
}
