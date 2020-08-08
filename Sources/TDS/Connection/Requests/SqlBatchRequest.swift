import Logging
import NIO
import Foundation

extension TDSConnection {
    public func rawSql(_ sqlText: String) -> EventLoopFuture<[TDSRow]> {
        var rows: [TDSRow] = []
        let message = TDSMessage.RawSqlBatchMessage(sqlText: sqlText)
        return query(message) { rows.append($0) }.map { return rows }
    }

    func query(_ message: TDSMessage.RawSqlBatchMessage, _ onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(sqlBatch: message, onRow)
        return self.send(request, logger: logger)
    }
}

class RawSqlBatchRequest: TDSRequest {
    let sqlBatch: TDSMessage.RawSqlBatchMessage
    var onRow: (TDSRow) throws -> ()
    var rowLookupTable: TDSRow.LookupTable?

    init(sqlBatch: TDSMessage.RawSqlBatchMessage, _ onRow: @escaping (TDSRow) throws -> ()) {
        self.sqlBatch = sqlBatch
        self.onRow = onRow
    }

    func respond(to message: TDSMessage, allocator: ByteBufferAllocator) throws -> TDSMessage? {
        var messageBuffer = try ByteBuffer(unpackingDataFrom: message, allocator: allocator)
        let response = try TDSMessage.TabularResultResponse.parse(from: &messageBuffer)

        // TODO: The following is an incomplete implementation of extracting data from rowTokens
        for token in response.tokens {
            switch token.type {
            case .row:
                guard let rowToken = token as? TDSTokens.RowToken else {
                    throw TDSError.protocolError("Error while reading row results.")
                }
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let row = TDSRow(dataRow: rowToken, lookupTable: rowLookupTable)
                try onRow(row)

                try onRow(row)
            case .colMetadata:
                guard let colMetadataToken = token as? TDSTokens.ColMetadataToken else {
                    throw TDSError.protocolError("Error reading column metadata token.")
                }
                rowLookupTable = TDSRow.LookupTable(colMetadata: colMetadataToken)
            default:
                break
            }
        }

        return nil
    }

    func start(allocator: ByteBufferAllocator) throws -> TDSMessage {
        return try TDSMessage(packetType: sqlBatch, allocator: allocator)
    }

    func log(to logger: Logger) {

    }
}

