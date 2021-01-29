
public struct TDSRow: CustomStringConvertible {
    final class LookupTable {
        let colMetadata: TDSTokens.ColMetadataToken

        struct Value {
            let index: Int
            let colData: TDSTokens.ColMetadataToken.ColumnData
        }

        private var _storage: [String: Value]?
        var storage: [String: Value] {
            if let existing = self._storage {
                return existing
            } else {
                let all = self.colMetadata.colData.enumerated().map { (index, colData) in
                    return (colData.colName, Value(index: index, colData: colData))
                }
                let storage = [String: Value](all) { a, b in
                    // take the first value
                    return a
                }
                self._storage = storage
                return storage
            }
        }

        init(
            colMetadata: TDSTokens.ColMetadataToken
        ) {
            self.colMetadata = colMetadata
        }

        func lookup(column: String) -> Value? {
            if let value = self.storage[column] {
                return value
            } else {
                return nil
            }
        }
    }

    public let dataRow: TDSTokens.RowToken

    public var columnMetadata: TDSTokens.ColMetadataToken {
        self.lookupTable.colMetadata
    }

    let lookupTable: LookupTable

    public func column(_ column: String) -> TDSData? {
        guard let entry = self.lookupTable.lookup(column: column) else {
            return nil
        }

        return TDSData(
            metadata: entry.colData,
            value: dataRow.colData[entry.index].data
        )
    }

    public var description: String {
        var row: [String: TDSData] = [:]
        for col in self.columnMetadata.colData {
            row[col.colName] = self.column(col.colName)
        }
        return row.description
    }
}
