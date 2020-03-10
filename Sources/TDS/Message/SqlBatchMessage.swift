import Logging
import NIO
import Foundation

extension TDSMessages {
    /// `LOGIN7`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/773a62b6-ee89-4c02-9e5e-344882630aac
    public struct SqlBatchMessage: TDSMessageType {
        public static var headerType: TDSPacket.HeaderType {
            return .sqlBatch
        }

        var sqlText: String

        public func serialize(into buffer: inout ByteBuffer) throws {
            TDSMessages.serializeAllHeaders(&buffer)
            for character in sqlText.utf16 {
                buffer.writeInteger(character, endianness: .little)
            }
            return
        }
    }
}
