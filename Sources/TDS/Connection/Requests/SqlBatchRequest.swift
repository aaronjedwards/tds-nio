import Logging
import NIO
import Foundation

extension TDSConnection {
    public func sql(_ sqlText: String) -> EventLoopFuture<Void> {
        let message = TDSMessages.SqlBatchMessage(sqlText: sqlText)
        return self.send(SqlBatchRequest(sqlBatch: message))
    }
}

struct SqlBatchRequest: TDSRequest {
    let sqlBatch: TDSMessages.SqlBatchMessage

    func respond(to message: TDSMessage, allocator: ByteBufferAllocator) throws -> TDSMessage? {
        var messageBuffer = try ByteBuffer(unpackingDataFrom: message, allocator: allocator)
        let response = try TDSMessages.TabularResponse.parse(from: &messageBuffer)

        // TODO: pass row data to upper layer
        // Below prints results of "SELECT @@VERSION" test query
        guard
            let rowToken = response.tokens[1] as? TDSMessages.RowToken,
            let data = rowToken.colData.first?.data,
            let sqlServerVersion = String(bytes: data, encoding: .utf16LittleEndian)
        else {
            throw TDSError.protocolError("Error attempting to extract row value for test sql result.")
        }

        print(sqlServerVersion)
        return nil
    }

    func start(allocator: ByteBufferAllocator) throws -> TDSMessage {
        let message = try TDSMessage(packetType: sqlBatch, allocator: allocator)
        return message
    }

    func log(to logger: Logger) {

    }
}

