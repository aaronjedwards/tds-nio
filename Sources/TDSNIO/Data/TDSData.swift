//===----------------------------------------------------------------------===//
//
// This source file is part of the TDSNIO open source project
//
// Copyright (c) 2026 Aaron Edwards and the TDSNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
// See CONTRIBUTORS.md for the list of TDSNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

public import Foundation

public struct TDSColumn: Sendable, Hashable {
    public struct Metadata: Sendable, Hashable {
        public struct UDTInfo: Sendable, Hashable {
            public var databaseName: String
            public var schemaName: String
            public var typeName: String
            public var assemblyQualifiedName: String

            public init(
                databaseName: String,
                schemaName: String,
                typeName: String,
                assemblyQualifiedName: String
            ) {
                self.databaseName = databaseName
                self.schemaName = schemaName
                self.typeName = typeName
                self.assemblyQualifiedName = assemblyQualifiedName
            }
        }

        public struct XMLInfo: Sendable, Hashable {
            public var databaseName: String
            public var owningSchema: String
            public var schemaCollection: String

            public init(databaseName: String, owningSchema: String, schemaCollection: String) {
                self.databaseName = databaseName
                self.owningSchema = owningSchema
                self.schemaCollection = schemaCollection
            }
        }

        public struct SensitivityClassification: Sendable, Hashable {
            public var labelName: String
            public var labelID: String
            public var informationTypeName: String
            public var informationTypeID: String
            public var rank: Int32?

            public init(
                labelName: String,
                labelID: String,
                informationTypeName: String,
                informationTypeID: String,
                rank: Int32? = nil
            ) {
                self.labelName = labelName
                self.labelID = labelID
                self.informationTypeName = informationTypeName
                self.informationTypeID = informationTypeID
                self.rank = rank
            }
        }

        public var userType: UInt32
        public var flags: UInt16
        public var length: UInt64?
        public var collation: [UInt8]
        public var precision: UInt8?
        public var scale: UInt8?
        public var tableName: String?
        public var baseTableName: String?
        public var tableNumber: UInt8?
        public var baseColumnName: String?
        public var isExpression: Bool
        public var isKey: Bool
        public var isHidden: Bool
        public var isOrderBy: Bool
        public var alternateOperation: UInt8?
        public var alternateOperand: UInt16?
        public var sensitivityClassifications: [SensitivityClassification]
        public var udtInfo: UDTInfo?
        public var xmlInfo: XMLInfo?

        public init(
            userType: UInt32 = 0,
            flags: UInt16 = 0,
            length: UInt64? = nil,
            collation: [UInt8] = [],
            precision: UInt8? = nil,
            scale: UInt8? = nil,
            tableName: String? = nil,
            baseTableName: String? = nil,
            tableNumber: UInt8? = nil,
            baseColumnName: String? = nil,
            isExpression: Bool = false,
            isKey: Bool = false,
            isHidden: Bool = false,
            isOrderBy: Bool = false,
            alternateOperation: UInt8? = nil,
            alternateOperand: UInt16? = nil,
            sensitivityClassifications: [SensitivityClassification] = [],
            udtInfo: UDTInfo? = nil,
            xmlInfo: XMLInfo? = nil
        ) {
            self.userType = userType
            self.flags = flags
            self.length = length
            self.collation = collation
            self.precision = precision
            self.scale = scale
            self.tableName = tableName
            self.baseTableName = baseTableName
            self.tableNumber = tableNumber
            self.baseColumnName = baseColumnName
            self.isExpression = isExpression
            self.isKey = isKey
            self.isHidden = isHidden
            self.isOrderBy = isOrderBy
            self.alternateOperation = alternateOperation
            self.alternateOperand = alternateOperand
            self.sensitivityClassifications = sensitivityClassifications
            self.udtInfo = udtInfo
            self.xmlInfo = xmlInfo
        }
    }

    public var name: String
    public var dataType: TDSDataType
    public var metadata: Metadata

    public init(name: String, dataType: TDSDataType, metadata: Metadata = .init()) {
        self.name = name
        self.dataType = dataType
        self.metadata = metadata
    }
}

public struct TDSRow: Sendable, Hashable {
    public var columns: [TDSColumn]
    public var values: [TDSData]

    public subscript(_ column: String) -> TDSData? {
        guard let index = self.firstIndex(ofColumn: column), index < self.values.count else {
            return nil
        }
        return self.values[index]
    }

    public func contains(_ column: String) -> Bool {
        self.firstIndex(ofColumn: column) != nil
    }

    public func firstIndex(ofColumn column: String) -> Int? {
        self.columns.firstIndex { $0.name == column }
    }

    public func cell(at index: Int) -> TDSCell? {
        guard index >= 0, index < self.columns.count, index < self.values.count else {
            return nil
        }
        let column = self.columns[index]
        return TDSCell(
            value: self.values[index],
            dataType: column.dataType,
            columnName: column.name,
            columnIndex: index,
            columnMetadata: column.metadata
        )
    }

    public func cell(named column: String) -> TDSCell? {
        guard let index = self.firstIndex(ofColumn: column) else {
            return nil
        }
        return self.cell(at: index)
    }

    public func decode<T: TDSDecodable>(
        _ type: T.Type = T.self,
        file: String = #fileID,
        line: Int = #line
    ) throws -> T {
        try self.decode(column: 0, as: type, file: file, line: line)
    }

    public func decode<A: TDSDecodable, B: TDSDecodable>(
        _ type: (A, B).Type,
        file: String = #fileID,
        line: Int = #line
    ) throws -> (A, B) {
        (
            try self.decode(column: 0, as: A.self, file: file, line: line),
            try self.decode(column: 1, as: B.self, file: file, line: line)
        )
    }

    public func decode<A: TDSDecodable, B: TDSDecodable, C: TDSDecodable>(
        _ type: (A, B, C).Type,
        file: String = #fileID,
        line: Int = #line
    ) throws -> (A, B, C) {
        (
            try self.decode(column: 0, as: A.self, file: file, line: line),
            try self.decode(column: 1, as: B.self, file: file, line: line),
            try self.decode(column: 2, as: C.self, file: file, line: line)
        )
    }

    public func decode<A: TDSDecodable, B: TDSDecodable, C: TDSDecodable, D: TDSDecodable>(
        _ type: (A, B, C, D).Type,
        file: String = #fileID,
        line: Int = #line
    ) throws -> (A, B, C, D) {
        (
            try self.decode(column: 0, as: A.self, file: file, line: line),
            try self.decode(column: 1, as: B.self, file: file, line: line),
            try self.decode(column: 2, as: C.self, file: file, line: line),
            try self.decode(column: 3, as: D.self, file: file, line: line)
        )
    }

    public func decode<
        A: TDSDecodable,
        B: TDSDecodable,
        C: TDSDecodable,
        D: TDSDecodable,
        E: TDSDecodable
    >(
        _ type: (A, B, C, D, E).Type,
        file: String = #fileID,
        line: Int = #line
    ) throws -> (A, B, C, D, E) {
        (
            try self.decode(column: 0, as: A.self, file: file, line: line),
            try self.decode(column: 1, as: B.self, file: file, line: line),
            try self.decode(column: 2, as: C.self, file: file, line: line),
            try self.decode(column: 3, as: D.self, file: file, line: line),
            try self.decode(column: 4, as: E.self, file: file, line: line)
        )
    }

    public func decode<T: TDSDecodable>(
        column: String,
        as type: T.Type = T.self,
        file: String = #fileID,
        line: Int = #line
    ) throws -> T {
        guard let cell = self.cell(named: column) else {
            throw TDSDecodingError.missingColumn(column, file: file, line: line)
        }
        return try cell.decode(type, file: file, line: line)
    }

    public func decode<T: TDSDecodable>(
        column index: Int,
        as type: T.Type = T.self,
        file: String = #fileID,
        line: Int = #line
    ) throws -> T {
        guard let cell = self.cell(at: index) else {
            throw TDSDecodingError.missingColumnIndex(index, file: file, line: line)
        }
        return try cell.decode(type, file: file, line: line)
    }

    public func makeRandomAccess() -> TDSRandomAccessRow {
        TDSRandomAccessRow(self)
    }
}

/// A random-access row of ``TDSCell`` values.
///
/// `TDSRow` keeps the protocol payload as column metadata plus raw ``TDSData`` values. This wrapper mirrors
/// A random-access row shape: initialization is O(n), and subsequent cell lookup by index or name is O(1).
public struct TDSRandomAccessRow: Sendable, Hashable {
    private var columns: [TDSColumn]
    private var values: [TDSData]
    private var lookupTable: [String: Int]

    public init(_ row: TDSRow) {
        self.columns = row.columns
        self.values = row.values
        self.lookupTable = [:]
        self.lookupTable.reserveCapacity(row.columns.count)
        for (index, column) in row.columns.enumerated() {
            self.lookupTable[column.name] = index
        }
    }

    public subscript(name: String) -> TDSCell {
        guard let index = self.lookupTable[name] else {
            fatalError(#"A column "\#(name)" does not exist."#)
        }
        return self[index]
    }

    public func contains(_ column: String) -> Bool {
        self.lookupTable[column] != nil
    }

    public func decode<T: TDSDecodable>(
        column: String,
        as type: T.Type = T.self,
        file: String = #fileID,
        line: Int = #line
    ) throws -> T {
        guard let index = self.lookupTable[column] else {
            throw TDSDecodingError.missingColumn(column, file: file, line: line)
        }
        return try self.decode(column: index, as: type, file: file, line: line)
    }

    public func decode<T: TDSDecodable>(
        column index: Int,
        as type: T.Type = T.self,
        file: String = #fileID,
        line: Int = #line
    ) throws -> T {
        guard index >= 0, index < self.endIndex else {
            throw TDSDecodingError.missingColumnIndex(index, file: file, line: line)
        }
        return try self[index].decode(type, file: file, line: line)
    }
}

extension TDSRandomAccessRow: RandomAccessCollection {
    public typealias Element = TDSCell
    public typealias Index = Int

    public var startIndex: Int { 0 }
    public var endIndex: Int { Swift.min(self.columns.count, self.values.count) }
    public var count: Int { self.endIndex }

    public subscript(index: Int) -> TDSCell {
        guard index >= self.startIndex, index < self.endIndex else {
            preconditionFailure("index out of bounds")
        }
        let column = self.columns[index]
        return TDSCell(
            value: self.values[index],
            dataType: column.dataType,
            columnName: column.name,
            columnIndex: index,
            columnMetadata: column.metadata
        )
    }
}

public struct TDSCell: Sendable, Hashable {
    public var value: TDSData
    public var dataType: TDSDataType
    public var columnName: String
    public var columnIndex: Int
    public var columnMetadata: TDSColumn.Metadata

    public init(
        value: TDSData,
        dataType: TDSDataType,
        columnName: String,
        columnIndex: Int,
        columnMetadata: TDSColumn.Metadata = .init()
    ) {
        self.value = value
        self.dataType = dataType
        self.columnName = columnName
        self.columnIndex = columnIndex
        self.columnMetadata = columnMetadata
    }

    public func decode<T: TDSDecodable>(
        _ type: T.Type = T.self,
        file: String = #fileID,
        line: Int = #line
    ) throws -> T {
        do {
            return try T.decode(from: self.value)
        } catch var error as TDSDecodingError {
            error.columnName = self.columnName
            error.columnIndex = self.columnIndex
            error.dataType = self.dataType
            error.file = file
            error.line = line
            throw error
        }
    }
}

extension TDSRow: RandomAccessCollection {
    public typealias Element = TDSData
    public typealias Index = Array<TDSData>.Index

    public var startIndex: Index {
        self.values.startIndex
    }

    public var endIndex: Index {
        self.values.endIndex
    }

    public subscript(position: Index) -> TDSData {
        self.values[position]
    }

    public func index(after i: Index) -> Index {
        self.values.index(after: i)
    }

    public func index(before i: Index) -> Index {
        self.values.index(before: i)
    }
}

public struct TDSQueryResult: Sendable, Hashable {
    public var columns: [TDSColumn]
    public var rows: [TDSRow]
    public var rowsAffected: UInt64?
    public var offsets: [TDSOffset]
    public var alternateResultSets: [TDSAlternateResultSet]
    public var returnStatus: Int32?
    public var outputParameters: [TDSOutputParameter]
    public var resultSets: [TDSResultSet]

    public init(
        columns: [TDSColumn],
        rows: [TDSRow],
        rowsAffected: UInt64?,
        offsets: [TDSOffset] = [],
        alternateResultSets: [TDSAlternateResultSet] = [],
        returnStatus: Int32?,
        outputParameters: [TDSOutputParameter],
        resultSets: [TDSResultSet]
    ) {
        self.columns = columns
        self.rows = rows
        self.rowsAffected = rowsAffected
        self.offsets = offsets
        self.alternateResultSets = alternateResultSets
        self.returnStatus = returnStatus
        self.outputParameters = outputParameters
        self.resultSets = resultSets
    }

    public var rowSequence: TDSRowSequence {
        TDSRowSequence(self.rows)
    }
}

public struct TDSResultSet: Sendable, Hashable {
    public var columns: [TDSColumn]
    public var rows: [TDSRow]
    public var rowsAffected: UInt64?
    public var offsets: [TDSOffset]
    public var alternateResultSets: [TDSAlternateResultSet]

    public init(
        columns: [TDSColumn],
        rows: [TDSRow],
        rowsAffected: UInt64?,
        offsets: [TDSOffset] = [],
        alternateResultSets: [TDSAlternateResultSet] = []
    ) {
        self.columns = columns
        self.rows = rows
        self.rowsAffected = rowsAffected
        self.offsets = offsets
        self.alternateResultSets = alternateResultSets
    }

    public var rowSequence: TDSRowSequence {
        TDSRowSequence(self.rows)
    }
}

public struct TDSOffset: Sendable, Hashable {
    public var identifier: UInt16
    public var offset: UInt16

    public init(identifier: UInt16, offset: UInt16) {
        self.identifier = identifier
        self.offset = offset
    }
}

public struct TDSAlternateResultSet: Sendable, Hashable {
    public var id: UInt16
    public var byColumns: [UInt16]
    public var columns: [TDSColumn]
    public var rows: [TDSRow]

    public init(id: UInt16, byColumns: [UInt16], columns: [TDSColumn], rows: [TDSRow] = []) {
        self.id = id
        self.byColumns = byColumns
        self.columns = columns
        self.rows = rows
    }
}

public struct TDSOutputParameter: Sendable, Hashable {
    public var ordinal: UInt16
    public var name: String
    public var status: UInt8
    public var userType: UInt32
    public var flags: UInt16
    public var dataType: TDSDataType
    public var metadata: TDSColumn.Metadata
    public var value: TDSData

    public init(
        ordinal: UInt16,
        name: String,
        status: UInt8,
        userType: UInt32,
        flags: UInt16,
        dataType: TDSDataType,
        metadata: TDSColumn.Metadata = .init(),
        value: TDSData
    ) {
        self.ordinal = ordinal
        self.name = name
        self.status = status
        self.userType = userType
        self.flags = flags
        self.dataType = dataType
        self.metadata = metadata
        self.value = value
    }

    public func decode<T: TDSDecodable>(
        _ type: T.Type = T.self,
        file: String = #fileID,
        line: Int = #line
    ) throws -> T {
        do {
            return try T.decode(from: self.value)
        } catch var error as TDSDecodingError {
            error.columnName = self.name
            error.columnIndex = Int(self.ordinal)
            error.dataType = self.dataType
            error.file = file
            error.line = line
            throw error
        }
    }
}

extension TDSQueryResult {
    public func outputParameter(at ordinal: UInt16) -> TDSOutputParameter? {
        self.outputParameters.first { $0.ordinal == ordinal }
    }

    public func outputParameter(named name: String) -> TDSOutputParameter? {
        let normalizedName = name.hasPrefix("@") ? String(name.dropFirst()) : name
        return self.outputParameters.first {
            let parameterName = $0.name.hasPrefix("@") ? String($0.name.dropFirst()) : $0.name
            return parameterName == normalizedName
        }
    }

    public func decodeOutputParameter<T: TDSDecodable>(
        named name: String,
        as type: T.Type = T.self,
        file: String = #fileID,
        line: Int = #line
    ) throws -> T {
        guard let parameter = self.outputParameter(named: name) else {
            throw TDSDecodingError.missingOutputParameter(name, file: file, line: line)
        }
        return try parameter.decode(type, file: file, line: line)
    }
}

public enum TDSData: Sendable, Hashable {
    case null
    case typedNull(TDSSQLType)
    case bool(Bool)
    case tinyInt(UInt8)
    case smallInt(Int16)
    case int32(Int32)
    case int(Int64)
    case float(Float)
    case double(Double)
    case decimal(String)
    case money(String)
    case date(TDSDate)
    case time(TDSTime)
    case datetime(TDSDateTime)
    case datetime2(TDSDateTime)
    case datetimeOffset(TDSDateTimeOffset)
    case guid(TDSGUID)
    case string(String)
    case bytes([UInt8])
    case xml([UInt8])
    case json([UInt8])
    case table(TDSTableValuedParameter)

    public static func null(of type: TDSSQLType) -> TDSData {
        .typedNull(type)
    }
}

public enum TDSSQLType: Sendable, Hashable {
    case bit
    case tinyInt
    case smallInt
    case int
    case bigInt
    case real
    case float
    case decimal(precision: UInt8 = 38, scale: UInt8 = 0)
    case money
    case date
    case time(scale: UInt8 = 7)
    case datetime
    case datetime2(scale: UInt8 = 7)
    case datetimeOffset(scale: UInt8 = 7)
    case uniqueIdentifier
    case char(maxBytes: UInt16)
    case varchar(maxBytes: UInt16 = UInt16.max)
    case nchar(maxBytes: UInt16)
    case nvarchar(maxBytes: UInt16 = UInt16.max)
    case binary(maxBytes: UInt16)
    case varbinary(maxBytes: UInt16 = UInt16.max)
    case xml
    case json
}

public struct TDSGUID: Sendable, Hashable {
    public var stringValue: String

    public init(_ stringValue: String) {
        self.stringValue = stringValue.lowercased()
    }

    public init(_ uuid: UUID) {
        self.init(uuid.uuidString)
    }

    public var uuidValue: UUID? {
        UUID(uuidString: self.stringValue)
    }
}

public struct TDSDate: Sendable, Hashable {
    public var year: Int
    public var month: Int
    public var day: Int

    public init(year: Int, month: Int, day: Int) {
        self.year = year
        self.month = month
        self.day = day
    }

    public init(_ date: Date, timeZone: TimeZone = TimeZone(secondsFromGMT: 0)!) {
        let components = Calendar.tdsGregorian(timeZone: timeZone).dateComponents([.year, .month, .day], from: date)
        self.init(
            year: components.year ?? 1,
            month: components.month ?? 1,
            day: components.day ?? 1
        )
    }

    public func dateValue(timeZone: TimeZone = TimeZone(secondsFromGMT: 0)!) -> Date? {
        Calendar.tdsGregorian(timeZone: timeZone).date(
            from: DateComponents(
                timeZone: timeZone,
                year: self.year,
                month: self.month,
                day: self.day
            ))
    }
}

public struct TDSTime: Sendable, Hashable {
    public var hour: Int
    public var minute: Int
    public var second: Int
    public var nanosecond: Int
    public var scale: UInt8

    public init(hour: Int, minute: Int, second: Int, nanosecond: Int, scale: UInt8) {
        self.hour = hour
        self.minute = minute
        self.second = second
        self.nanosecond = nanosecond
        self.scale = scale
    }

    public init(_ date: Date, timeZone: TimeZone = TimeZone(secondsFromGMT: 0)!, scale: UInt8 = 7) {
        let components = Calendar.tdsGregorian(timeZone: timeZone).dateComponents(
            [.hour, .minute, .second, .nanosecond],
            from: date
        )
        self.init(
            hour: components.hour ?? 0,
            minute: components.minute ?? 0,
            second: components.second ?? 0,
            nanosecond: Self.normalizeNanosecond(components.nanosecond ?? 0, scale: scale),
            scale: scale
        )
    }

    private static func normalizeNanosecond(_ nanosecond: Int, scale: UInt8) -> Int {
        let clampedScale = min(Int(scale), 9)
        let divisor = Int(pow(10.0, Double(9 - clampedScale)))
        return nanosecond / divisor * divisor
    }
}

public struct TDSDateTime: Sendable, Hashable {
    public var date: TDSDate
    public var time: TDSTime

    public init(date: TDSDate, time: TDSTime) {
        self.date = date
        self.time = time
    }

    public init(_ date: Date, timeZone: TimeZone = TimeZone(secondsFromGMT: 0)!, scale: UInt8 = 7) {
        self.init(
            date: TDSDate(date, timeZone: timeZone),
            time: TDSTime(date, timeZone: timeZone, scale: scale)
        )
    }

    public func dateValue(timeZone: TimeZone = TimeZone(secondsFromGMT: 0)!) -> Date? {
        Calendar.tdsGregorian(timeZone: timeZone).date(
            from: DateComponents(
                timeZone: timeZone,
                year: self.date.year,
                month: self.date.month,
                day: self.date.day,
                hour: self.time.hour,
                minute: self.time.minute,
                second: self.time.second,
                nanosecond: self.time.nanosecond
            ))
    }
}

public struct TDSDateTimeOffset: Sendable, Hashable {
    public var dateTime: TDSDateTime
    public var offsetMinutes: Int

    public init(dateTime: TDSDateTime, offsetMinutes: Int) {
        self.dateTime = dateTime
        self.offsetMinutes = offsetMinutes
    }

    public init(_ date: Date, offsetMinutes: Int = 0, scale: UInt8 = 7) {
        let timeZone = TimeZone(secondsFromGMT: offsetMinutes * 60) ?? TimeZone(secondsFromGMT: 0)!
        self.init(
            dateTime: TDSDateTime(date, timeZone: timeZone, scale: scale),
            offsetMinutes: offsetMinutes
        )
    }

    public func dateValue() -> Date? {
        let timeZone = TimeZone(secondsFromGMT: self.offsetMinutes * 60) ?? TimeZone(secondsFromGMT: 0)!
        return self.dateTime.dateValue(timeZone: timeZone)
    }
}

extension Calendar {
    fileprivate static func tdsGregorian(timeZone: TimeZone) -> Calendar {
        var calendar = Calendar(identifier: .gregorian)
        calendar.locale = Locale(identifier: "en_US_POSIX")
        calendar.timeZone = timeZone
        return calendar
    }
}

public enum TDSDataType: UInt8, Sendable, Hashable {
    case null = 0x1F
    case int1 = 0x30
    case bit = 0x32
    case int2 = 0x34
    case int4 = 0x38
    case datetime4 = 0x3A
    case float4 = 0x3B
    case money = 0x3C
    case datetime = 0x3D
    case float8 = 0x3E
    case money4 = 0x7A
    case int8 = 0x7F
    case image = 0x22
    case text = 0x23
    case nText = 0x63
    case sqlVariant = 0x62
    case intN = 0x26
    case decimalN = 0x6A
    case numericN = 0x6C
    case bitN = 0x68
    case floatN = 0x6D
    case moneyN = 0x6E
    case datetimeN = 0x6F
    case dateN = 0x28
    case timeN = 0x29
    case datetime2N = 0x2A
    case datetimeOffsetN = 0x2B
    case guid = 0x24
    case legacyVarBin = 0x25
    case legacyVarChar = 0x27
    case legacyBinary = 0x2D
    case legacyChar = 0x2F
    case legacyDecimal = 0x37
    case legacyNumeric = 0x3F
    case bigVarBin = 0xA5
    case bigVarChar = 0xA7
    case bigBinary = 0xAD
    case bigChar = 0xAF
    case nVarChar = 0xE7
    case nChar = 0xEF
    case udt = 0xF0
    case xml = 0xF1
    case json = 0xF4
}
