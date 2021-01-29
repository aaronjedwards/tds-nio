import Logging
import NIO
import Foundation

extension TDSConnection {
    public func rawSql(_ sqlText: String) -> EventLoopFuture<[TDSRow]> {
        var rows: [TDSRow] = []
        return rawSql(sqlText, onRow: { rows.append($0) }).map { rows }
    }
    
    public func rawSql(_ sqlText: String, onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(sqlBatch: TDSMessages.RawSqlBatchMessage(sqlText: sqlText), logger: logger, onRow)
        return self.send(request, logger: logger)
    }


    func query(_ message: TDSMessages.RawSqlBatchMessage, _ onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(sqlBatch: message, logger: logger, onRow)
        return self.send(request, logger: logger)
    }
}

class RawSqlBatchRequest: TDSRequest {
    let sqlBatch: TDSMessages.RawSqlBatchMessage
    var onRow: (TDSRow) throws -> ()
    var rowLookupTable: TDSRow.LookupTable?
    
    private let logger: Logger
    private let tokenParser: TDSTokenParser

    init(sqlBatch: TDSMessages.RawSqlBatchMessage, logger: Logger, _ onRow: @escaping (TDSRow) throws -> ()) {
        self.sqlBatch = sqlBatch
        self.onRow = onRow
        self.logger = logger
        self.tokenParser = TDSTokenParser(logger: logger)
    }

    func handle(packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        // Add packet to token parser stream
        let parsedTokens = tokenParser.writeAndParseTokens(packet.messageBuffer)
        try handleParsedTokens(parsedTokens)
        guard packet.header.status == .eom else {
            return .continue
        }

        return .done
    }

    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        return try TDSMessage(payload: sqlBatch, allocator: allocator).packets
    }

    func log(to logger: Logger) {

    }
    
    func handleParsedTokens(_ tokens: [TDSToken]) throws {
        // TODO: The following is an incomplete implementation of extracting data from rowTokens
        for token in tokens {
            switch token.type {
            case .row:
                guard let rowToken = token as? TDSTokens.RowToken else {
                    throw TDSError.protocolError("Error while reading row results.")
                }
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let row = TDSRow(dataRow: rowToken, lookupTable: rowLookupTable)
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
    }
}

