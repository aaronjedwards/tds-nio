import NIO
import Logging

extension TDSDatabase {
    public func query(
        _ sql: String,
        _ parameters: [String: TDSData] = [:]
    ) -> EventLoopFuture<TDSQueryResult> {
        var rows: [TDSRow] = []
        return self.query(sql, parameters) {
            rows.append($0)
        }.map {
            .init(rows: rows)
        }
    }

    public func query(
        _ sql: String,
        _ parameters: [String: TDSData] = [:],
        onRow: @escaping (TDSRow) throws -> ()
    ) -> EventLoopFuture<Void> {
        let query = TDSParameterizedQuery(
            query: sql,
            parameters: parameters,
            logger: logger,
            onRow: onRow
        )
        return self.send(query, logger: self.logger)
    }
}

public struct TDSQueryResult {
    public let rows: [TDSRow]
}

extension TDSQueryResult: Collection {
    public typealias Index = Int
    public typealias Element = TDSRow

    public var startIndex: Int {
        self.rows.startIndex
    }

    public var endIndex: Int {
        self.rows.endIndex
    }

    public subscript(position: Int) -> TDSRow {
        self.rows[position]
    }

    public func index(after i: Int) -> Int {
        self.rows.index(after: i)
    }
}

private final class TDSParameterizedQuery: TDSTokenStreamRequest {
    let query: String
    let parameters: [String: TDSData]
    var onRow: (TDSRow) throws -> ()
    var rowLookupTable: TDSRow.LookupTable?
    var logger: Logger

    init(
        query: String,
        parameters: [String: TDSData],
        logger: Logger,
        onRow: @escaping (TDSRow) throws -> ()
    ) {
        self.query = query
        self.parameters = parameters
        self.onRow = onRow
        self.logger = logger
    }
    
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        guard self.parameters.count <= Int16.max else {
            throw TDSError.protocolError("Parameter count must be <= \(Int16.max).")
        }
        
        var preparedParams = [String: TDSData]()
        
        // Add parameter for the sql statement and parameter definitions
        preparedParams["sql"] = .init(string: query)
        preparedParams["params"] = makeParamsParameter(with: parameters)
        
        // Add original parameters
        for param in parameters {
            preparedParams[param.key] = param.value
        }
        
        let message = TDSMessages.RPCMessage(sqlTextOrProcedure: "sp_executesql", parameters: preparedParams)
        return try TDSMessage(payload: message, allocator: allocator).packets
    }
    
    private func makeParamsParameter(with parameters: [String: TDSData]) -> TDSData {
        let paramString = parameters.map { param in
            let p = "@\(param.key) \(param.value.declaration)"
            // TODO: Account for "OUTPUT" parameters
            return p
        }.joined(separator: ", ")
        return .init(string: paramString)
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
    
    func complete(message: inout ByteBuffer, allocator: ByteBufferAllocator) throws -> TDSRequestResponse {
        return .done
    }
    
    func log(to logger: Logger) {
        self.logger = logger
        logger.debug("\(self.query) \(self.parameters)")
    }
}
