import Logging
import NIO
import Foundation

extension TDSConnection {

    /// Execute remote procedure using proc name
    /// - Parameter procName: Procedure name
    /// - Returns: Array of TDSRow structs [TDSRow]
    public func rpc(_ procName: String, _ inputParameters: [RPCInputParameter]?, _ outputParameters: [RPCOutputParameter]?) -> EventLoopFuture<[TDSRow]> {
        var rows: [TDSRow] = []
        return rpc(procName, inputParameters, outputParameters, onRow: { rows.append($0) }).map { rows }
    }
    
    /// Execute remote procedure. Requires implmentation of custom handling of onRow parameter called every time a TDSRow is parsed from server response.
    /// - Parameters:
    ///   - procName: Procedure name
    ///   - onRow: @escaping parameter called every time a TDSRow Struct is parsed from server response and throws on parse error
    /// - Returns: Void
    public func rpc(_ procName: String, _ inputParameters: [RPCInputParameter]?, _ outputParameters: [RPCOutputParameter]?, onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RPCRequest(messagePayload: TDSMessages.RPCMessage(procName: procName, inputParameters: inputParameters, outputParameters: outputParameters), logger: logger, onRow)
        return self.send(request, logger: logger)
    }
    
}

/// Request object for creation of RPC message and response handling.
class RPCRequest: TDSRequest {
    let messagePayload: TDSMessages.RPCMessage
    var onRow: (TDSRow) throws -> ()
    var rowLookupTable: TDSRow.LookupTable?
    
    private let logger: Logger
    private let tokenParser: TDSTokenParser

    init(messagePayload: TDSMessages.RPCMessage, logger: Logger, _ onRow: @escaping (TDSRow) throws -> ()) {
        self.messagePayload = messagePayload
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
        return try TDSMessage(payload: messagePayload, allocator: allocator).packets
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
                
            case .done:
                guard let doneToken = token as? TDSTokens.DoneToken else {
                    throw TDSError.protocolError("Error while parsing done token")
                }
            case .returnStatus:
                throw TDSError.protocolError("Error while parsing Return Status Token")
            case .returnValue:
                throw TDSError.protocolError("Error while parsing Return Value Token")
            case .error:
                guard let errorToken = token as? TDSTokens.ErrorInfoToken else {
                    throw TDSError.protocolError("Error reading error token.")
                }
                throw TDSError.errorToken(errorToken.messageText)
            default:
                break
            }
        }
    }
}

