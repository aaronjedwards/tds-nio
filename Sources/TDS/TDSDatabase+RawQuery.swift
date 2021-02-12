import Logging
import NIO
import Foundation

extension TDSDatabase {
    public func rawSql(_ sqlText: String) -> EventLoopFuture<[TDSRow]> {
        var rows: [TDSRow] = []
        return rawSql(sqlText, onRow: { rows.append($0) }).map { rows }
    }
    
    public func rawSql(_ sqlText: String, onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(sqlText: sqlText, logger: logger, onRow)
        return self.send(request, logger: logger)
    }
}

class RawSqlBatchRequest: TDSTokenStreamRequest {
    let sqlText: String
    var onRow: (TDSRow) throws -> ()
    var rowLookupTable: TDSRow.LookupTable?
    
    private let logger: Logger
    private let tokenParser: TDSTokenParser

    init(sqlText: String, logger: Logger, _ onRow: @escaping (TDSRow) throws -> ()) {
        self.sqlText = sqlText
        self.onRow = onRow
        self.logger = logger
        self.tokenParser = TDSTokenParser(logger: logger)
    }

    func handle(token: TDSToken) throws {
        // TODO: The following is an incomplete implementation of handling parsed tokens
        switch token.type {
        case .row:
            let rowToken = try TDSTokens.row(token)
            guard let rowLookupTable = self.rowLookupTable else { fatalError() }
            let row = TDSRow(dataRow: rowToken, lookupTable: rowLookupTable)
            try onRow(row)
        case .colMetadata:
            let colMetadataToken = try TDSTokens.colMetadata(token)
            rowLookupTable = TDSRow.LookupTable(colMetadata: colMetadataToken)
        default:
            break
        }
    }

    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let payload = TDSMessages.RawSqlBatchMessage(sqlText: sqlText)
        return try TDSMessage(payload: payload, allocator: allocator).packets
    }
    
    func complete(message: inout ByteBuffer, allocator: ByteBufferAllocator) throws -> TDSRequestResponse {
        return .done
    }

    func log(to logger: Logger) {
        logger.debug("Sending Raw SQL Batch: \(sqlText).")
    }
}

