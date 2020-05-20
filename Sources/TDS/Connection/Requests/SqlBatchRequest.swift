import Logging
import NIO
import Foundation

extension TDSConnection {
    public func rawSql(_ sqlText: String) -> EventLoopFuture<[String]> {
        var rows: [String] = []
        let message = TDSMessages.RawSqlBatchMessage(sqlText: sqlText)
        return query(message) { rows.append($0) }.map { return rows }
    }

    func query(_ message: TDSMessages.RawSqlBatchMessage, _ onRow: @escaping (String) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(sqlBatch: message, onRow)
        return self.send(request)
    }
}

class RawSqlBatchRequest: TDSRequest {
    let sqlBatch: TDSMessages.RawSqlBatchMessage
    var onRow: (String) throws -> ()

    init(sqlBatch: TDSMessages.RawSqlBatchMessage, _ onRow: @escaping (String) throws -> ()) {
        self.sqlBatch = sqlBatch
        self.onRow = onRow
    }

    func respond(to message: TDSMessage, allocator: ByteBufferAllocator) throws -> TDSMessage? {
        var messageBuffer = try ByteBuffer(unpackingDataFrom: message, allocator: allocator)
        let response = try TDSMessages.TabularResultResponse.parse(from: &messageBuffer)

        let rowTokens = response.tokens.filter { $0.type == .row }

        // TODO: The following is an incomplete implementation of extracting data from rowTokens
        for token in rowTokens {

            var rowData: [String] = []
            guard let row = token as? TDSMessages.RowToken else {
                throw TDSError.protocolError("Error while reading row results.")
            }

            for colData in row.colData {
                if let data = String(bytes: colData.data, encoding: .utf16LittleEndian) {
                    rowData.append(data)
                }
            }

            try onRow(rowData.joined(separator: ", "))
        }

        return nil
    }

    func start(allocator: ByteBufferAllocator) throws -> TDSMessage {
        return try TDSMessage(packetType: sqlBatch, allocator: allocator)
    }

    func log(to logger: Logger) {

    }
}

