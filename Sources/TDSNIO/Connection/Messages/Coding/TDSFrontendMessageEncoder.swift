import Crypto
import NIOCore
import Foundation

struct TDSFrontendMessageEncoder {

    private enum State {
        case flushed
        case writable
    }

    enum PreloginEncryption: Byte {
        case encryptOff = 0x00
        case encryptOn = 0x01
        case encryptNotSup = 0x02
        case encryptReq = 0x03
        case encryptClientCertOff = 0x80
        case encryptClientCertOn = 0x81
        case encryptClientCertReq = 0x83
    }

    private var buffer: ByteBuffer
    private var state: State = .writable
    private var transactionDescriptor: [UInt8] = []
    private var resetConnectionOnNextRequest = false

    private struct LoginField {
        var bytes: [UInt8]
        var length: UInt16
        var bytesPerLengthUnit: Int
    }

    init(buffer: ByteBuffer) {
        self.buffer = buffer
    }

    mutating func flush() -> ByteBuffer {
        self.state = .flushed
        return self.buffer
    }

    mutating func prelogin(encryption: PreloginEncryption?) {
        self.clearIfNeeded()
        self.startRequest()

        var options: [(token: UInt8, data: [UInt8])] = [
            (0x00, [
            0x09, 0x00, 0x00, 0x00,  // UL_VERSION (9.0.0)
            0x00, 0x00,  // US_SUBBUILD (0)
            ]),
            (0x02, [0x00]),  // Default instance.
            (0x03, Self.processIDBytes()),
            (0x04, [0x00]),  // MARS off.
        ]
        if let enc = encryption {
            options.insert((0x01, [enc.rawValue]), at: 1)
        }

        var dataOffset = UInt16(options.count * 5 + 1)
        for option in options {
            self.buffer.writeInteger(option.token)
            self.buffer.writeInteger(dataOffset, endianness: .big)
            self.buffer.writeInteger(UInt16(option.data.count), endianness: .big)
            dataOffset += UInt16(option.data.count)
        }
        self.buffer.writeInteger(0xFF as UInt8)

        for option in options {
            self.buffer.writeBytes(option.data)
        }

        self.endRequest(packetType: .prelogin)
    }

    mutating func login(configuration: TDSConnection.Configuration) {
        self.clearIfNeeded()
        self.startRequest()

        let loginStart = self.buffer.writerIndex
        self.buffer.moveWriterIndex(forwardBy: 4)
        let featureExt = configuration.protocolVersion.supportsFeatureExt ? Self.loginFeatureExtBytes() : []

        self.buffer.writeInteger(configuration.protocolVersion.wireValue, endianness: .little)
        self.buffer.writeInteger(UInt32(configuration.packetSize), endianness: .little)
        self.buffer.writeInteger(0x0000_0001 as UInt32, endianness: .little)
        self.buffer.writeInteger(UInt32(ProcessInfo.processInfo.processIdentifier), endianness: .little)
        self.buffer.writeInteger(1 as UInt32, endianness: .little)
        self.buffer.writeInteger(0xE0 as UInt8)
        self.buffer.writeInteger(Self.loginOptionFlags2(for: configuration.authentication))
        self.buffer.writeInteger(Self.loginTypeFlags(for: configuration.applicationIntent))
        self.buffer.writeInteger(featureExt.isEmpty ? 0 : 0x10 as UInt8)
        self.buffer.writeInteger(1 as UInt32, endianness: .little)
        self.buffer.writeInteger(0x0000_0409 as UInt32, endianness: .little)

        let username: String
        let password: String
        let sspiBytes: [UInt8]
        switch configuration.authentication {
        case .sqlServer:
            username = configuration.username
            password = configuration.password
            sspiBytes = []
        case .sspi(let initialToken):
            username = ""
            password = ""
            sspiBytes = initialToken
        }

        var fields: [LoginField] = [
            Self.loginStringField(configuration.clientHostName),
            Self.loginStringField(username),
            Self.loginStringField(password, password: true),
            Self.loginStringField(configuration.applicationName),
            Self.loginStringField(configuration.host),
            featureExt.isEmpty ? Self.loginStringField("") : .init(
                bytes: [0, 0, 0, 0],
                length: 4,
                bytesPerLengthUnit: 1
            ),
            Self.loginStringField("TDSNIO"),
            Self.loginStringField(configuration.language ?? ""),
            Self.loginStringField(configuration.database ?? ""),
        ]
        var attachDatabaseFile = Self.loginStringField("")
        var changePassword = Self.loginStringField("", password: true)

        let offsetTableStart = self.buffer.writerIndex
        self.buffer.moveWriterIndex(forwardBy: fields.count * 4)
        self.buffer.writeBytes(configuration.clientID)
        let sspiOffsetPosition = self.buffer.writerIndex
        self.buffer.writeInteger(0 as UInt16, endianness: .little)
        self.buffer.writeInteger(0 as UInt16, endianness: .little)
        let attachDatabaseFileOffsetPosition = self.buffer.writerIndex
        self.buffer.writeInteger(0 as UInt16, endianness: .little)
        self.buffer.writeInteger(0 as UInt16, endianness: .little)
        let changePasswordOffsetPosition = self.buffer.writerIndex
        self.buffer.writeInteger(0 as UInt16, endianness: .little)
        self.buffer.writeInteger(0 as UInt16, endianness: .little)
        let sspiLongLengthPosition = self.buffer.writerIndex
        self.buffer.writeInteger(0 as UInt32, endianness: .little)
        var boundedFields = fields + [attachDatabaseFile, changePassword]
        Self.boundLoginFieldsToOffsetRange(&boundedFields, firstVariableOffset: self.buffer.writerIndex - loginStart)
        fields = Array(boundedFields.prefix(fields.count))
        attachDatabaseFile = boundedFields[fields.count]
        changePassword = boundedFields[fields.count + 1]

        var offsetTablePosition = offsetTableStart
        var extensionBlockPosition: Int?
        for (index, field) in fields.enumerated() {
            let fieldOffset = self.buffer.writerIndex - loginStart
            self.buffer.setInteger(UInt16(fieldOffset), at: offsetTablePosition, endianness: .little)
            self.buffer.setInteger(field.length, at: offsetTablePosition + 2, endianness: .little)
            offsetTablePosition += 4
            if !featureExt.isEmpty && index == 5 {
                extensionBlockPosition = self.buffer.writerIndex
            }
            self.buffer.writeBytes(field.bytes)
        }

        let sspiOffset = self.buffer.writerIndex - loginStart
        self.buffer.writeBytes(sspiBytes)
        let attachDatabaseFileOffset = self.buffer.writerIndex - loginStart
        self.buffer.writeBytes(attachDatabaseFile.bytes)
        let changePasswordOffset = self.buffer.writerIndex - loginStart
        self.buffer.writeBytes(changePassword.bytes)

        if !featureExt.isEmpty {
            let featureExtOffset = self.buffer.writerIndex - loginStart
            self.buffer.writeBytes(featureExt)
            if let extensionBlockPosition {
                self.buffer.setInteger(UInt32(featureExtOffset), at: extensionBlockPosition, endianness: .little)
            }
        }

        self.buffer.setInteger(
            sspiBytes.isEmpty ? 0 : UInt16(clamping: sspiOffset),
            at: sspiOffsetPosition,
            endianness: .little
        )
        self.buffer.setInteger(UInt16(clamping: sspiBytes.count), at: sspiOffsetPosition + 2, endianness: .little)
        self.buffer.setInteger(UInt16(clamping: attachDatabaseFileOffset), at: attachDatabaseFileOffsetPosition, endianness: .little)
        self.buffer.setInteger(attachDatabaseFile.length, at: attachDatabaseFileOffsetPosition + 2, endianness: .little)
        self.buffer.setInteger(UInt16(clamping: changePasswordOffset), at: changePasswordOffsetPosition, endianness: .little)
        self.buffer.setInteger(changePassword.length, at: changePasswordOffsetPosition + 2, endianness: .little)
        self.buffer.setInteger(UInt32(clamping: sspiBytes.count), at: sspiLongLengthPosition, endianness: .little)
        self.buffer.setInteger(UInt32(self.buffer.writerIndex - loginStart), at: loginStart, endianness: .little)

        self.endRequest(packetType: .tds7Login)
    }

    mutating func sqlBatch(_ sql: String) {
        self.clearIfNeeded()
        self.startRequest()
        self.allHeaders()
        for codeUnit in sql.utf16 {
            self.buffer.writeInteger(codeUnit, endianness: .little)
        }
        self.endRequest(packetType: .sqlBatch)
    }

    mutating func rpc(_ rpc: TDSRPC) {
        self.clearIfNeeded()
        self.startRequest()
        self.allHeaders()
        self.usVarchar(rpc.procedure)
        self.buffer.writeInteger(0 as UInt16, endianness: .little) // OptionFlags

        for parameter in rpc.parameters {
            self.bVarchar(parameter.name)
            self.buffer.writeInteger(parameter.isOutput ? 0x01 : 0x00 as UInt8)
            self.parameterValue(parameter.value)
        }

        self.endRequest(packetType: .rpc)
    }

    mutating func transactionManagerRequest(_ request: TDSTransactionManagerRequest) {
        self.clearIfNeeded()
        self.startRequest()
        self.allHeaders()
        self.buffer.writeInteger(request.requestType, endianness: .little)

        switch request.payload {
        case .varBytes(let bytes):
            self.usVarbyte(bytes)
        case .begin(let isolationLevel, let name):
            self.buffer.writeInteger(isolationLevel.rawValue)
            self.bVarbyte(name)
        case .commitOrRollback(let name, let beginAfterwards):
            self.bVarbyte(name)
            if let beginAfterwards {
                self.buffer.writeInteger(0x01 as UInt8)
                self.buffer.writeInteger(beginAfterwards.isolationLevel.rawValue)
                self.bVarbyte(beginAfterwards.name)
            } else {
                self.buffer.writeInteger(0x00 as UInt8)
            }
        case .savepoint(let name):
            self.bVarbyte(name)
        case .none:
            break
        }

        self.endRequest(packetType: .transactionManagerRequest)
    }

    mutating func bulkLoad(_ request: TDSBulkLoadRequest) {
        self.clearIfNeeded()
        self.startRequest()
        self.bulkLoadColumnMetadata(request.columns)

        for row in request.rows {
            self.buffer.writeInteger(0xD1 as UInt8)
            for (index, column) in request.columns.enumerated() {
                let value = index < row.count ? row[index] : .null
                self.bulkLoadValue(value, for: column)
            }
        }

        self.buffer.writeInteger(0xFD as UInt8)
        self.buffer.writeInteger(0 as UInt16, endianness: .little)
        self.buffer.writeInteger(0 as UInt16, endianness: .little)
        self.buffer.writeInteger(UInt64(request.rows.count), endianness: .little)
        self.endRequest(packetType: .bulkLoadData)
    }

    mutating func setTransactionDescriptor(_ bytes: [UInt8]) {
        self.transactionDescriptor = bytes
    }

    mutating func markResetConnectionOnNextRequest() {
        self.resetConnectionOnNextRequest = true
    }

    mutating func attention() {
        self.clearIfNeeded()
        self.startRequest()
        self.endRequest(packetType: .attentionSignal)
    }

    mutating func sspi(_ bytes: [UInt8]) {
        self.clearIfNeeded()
        self.startRequest()
        self.buffer.writeBytes(bytes)
        self.endRequest(packetType: .sspi)
    }

    mutating func federatedAuthenticationToken(
        token: [UInt8],
        nonce: [UInt8]?
    ) {
        self.clearIfNeeded()
        self.startRequest()

        let nonceLength = nonce?.count ?? 0
        self.buffer.writeInteger(UInt32(4 + token.count + nonceLength), endianness: .little)
        self.buffer.writeInteger(UInt32(token.count), endianness: .little)
        self.buffer.writeBytes(token)
        if let nonce {
            self.buffer.writeBytes(nonce)
        }

        self.endRequest(packetType: .federatedAuthenticationToken)
    }

    // MARK: - Private Methods -

    private mutating func clearIfNeeded() {
        switch self.state {
        case .flushed:
            self.state = .writable
            self.buffer.clear()
        case .writable:
            break
        }
    }

    /// Starts a new request with a placeholder for the header, which is set at the end of the request via
    /// ``endRequest``
    private mutating func startRequest() {
        self.buffer.reserveCapacity(TDSPacket.headerLength)
        self.buffer.moveWriterIndex(forwardBy: TDSPacket.headerLength)
    }

    private mutating func allHeaders() {
        let startWriterIndex = self.buffer.writerIndex
        self.buffer.moveWriterIndex(forwardBy: 4)
        self.buffer.writeInteger(18 as UInt32, endianness: .little)
        self.buffer.writeInteger(0x02 as UInt16, endianness: .little)
        if self.transactionDescriptor.count == 8 {
            self.buffer.writeBytes(self.transactionDescriptor)
        } else {
            self.buffer.writeInteger(0 as UInt64, endianness: .little)
        }
        self.buffer.writeInteger(1 as UInt32, endianness: .little)
        self.buffer.setInteger(
            UInt32(self.buffer.writerIndex - startWriterIndex),
            at: startWriterIndex,
            endianness: .little
        )
    }

    private mutating func bulkLoadColumnMetadata(_ columns: [TDSBulkLoadRequest.Column]) {
        self.buffer.writeInteger(0x81 as UInt8)
        self.buffer.writeInteger(UInt16(columns.count), endianness: .little)
        for column in columns {
            self.buffer.writeInteger(column.userType, endianness: .little)
            self.buffer.writeInteger(column.flags, endianness: .little)
            switch column.dataType {
            case .int:
                self.buffer.writeInteger(TDSDataType.intN.rawValue)
                self.buffer.writeInteger(8 as UInt8)
            case .bit:
                self.buffer.writeInteger(TDSDataType.bitN.rawValue)
                self.buffer.writeInteger(1 as UInt8)
            case .nVarChar(let maxBytes, let collation):
                self.buffer.writeInteger(TDSDataType.nVarChar.rawValue)
                self.buffer.writeInteger(maxBytes, endianness: .little)
                self.buffer.writeBytes(collation)
            case .varBinary(let maxBytes):
                self.buffer.writeInteger(TDSDataType.bigVarBin.rawValue)
                self.buffer.writeInteger(maxBytes, endianness: .little)
            }
            self.bVarchar(column.name)
        }
    }

    private mutating func bulkLoadValue(
        _ value: TDSData,
        for column: TDSBulkLoadRequest.Column
    ) {
        switch column.dataType {
        case .int:
            guard case .int(let int) = value else {
                self.buffer.writeInteger(0 as UInt8)
                return
            }
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(int, endianness: .little)
        case .bit:
            guard case .bool(let bool) = value else {
                self.buffer.writeInteger(0 as UInt8)
                return
            }
            self.buffer.writeInteger(1 as UInt8)
            self.buffer.writeInteger(bool ? UInt8(1) : UInt8(0))
        case .nVarChar(let maxBytes, _):
            guard case .string(let string) = value else {
                self.buffer.writeInteger(UInt16.max, endianness: .little)
                return
            }
            self.writeUSVarCharValue(string, maxBytes: maxBytes)
        case .varBinary(let maxBytes):
            guard case .bytes(let bytes) = value else {
                self.buffer.writeInteger(UInt16.max, endianness: .little)
                return
            }
            self.writeUSVarByteValue(bytes, maxBytes: maxBytes)
        }
    }

    private mutating func bVarchar(_ value: String) {
        let utf16 = Array(value.utf16.prefix(Int(UInt8.max)))
        self.buffer.writeInteger(UInt8(utf16.count))
        self.writeUTF16(utf16)
    }

    private mutating func usVarchar(_ value: String) {
        let utf16 = Array(value.utf16.prefix(Int(UInt16.max)))
        self.buffer.writeInteger(UInt16(utf16.count), endianness: .little)
        self.writeUTF16(utf16)
    }

    private mutating func bVarbyte(_ value: [UInt8]) {
        let bytes = value.prefix(Int(UInt8.max))
        self.buffer.writeInteger(UInt8(bytes.count))
        self.buffer.writeBytes(bytes)
    }

    private mutating func usVarbyte(_ value: [UInt8]) {
        self.writeUSVarByteValue(value, maxBytes: UInt16.max)
    }

    private mutating func writeUTF16(_ value: String) {
        self.writeUTF16(value.utf16)
    }

    private mutating func writeUTF16<S: Sequence>(_ value: S) where S.Element == UInt16 {
        for codeUnit in value {
            self.buffer.writeInteger(codeUnit, endianness: .little)
        }
    }

    private mutating func writeUSVarCharValue(_ value: String, maxBytes: UInt16) {
        let maxCodeUnits = Int(maxBytes) / 2
        let utf16 = Array(value.utf16.prefix(maxCodeUnits))
        self.buffer.writeInteger(UInt16(utf16.count * 2), endianness: .little)
        self.writeUTF16(utf16)
    }

    private mutating func writeUSVarByteValue(_ value: [UInt8], maxBytes: UInt16) {
        let bytes = value.prefix(Int(maxBytes))
        self.buffer.writeInteger(UInt16(bytes.count), endianness: .little)
        self.buffer.writeBytes(bytes)
    }

    private static func utf16Bytes(_ value: String) -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.reserveCapacity(value.utf16.count * 2)
        for codeUnit in value.utf16 {
            bytes.append(UInt8(codeUnit & 0x00FF))
            bytes.append(UInt8(codeUnit >> 8))
        }
        return bytes
    }

    private static func processIDBytes() -> [UInt8] {
        let processID = UInt32(ProcessInfo.processInfo.processIdentifier)
        return [
            UInt8(processID & 0x0000_00FF),
            UInt8((processID & 0x0000_FF00) >> 8),
            UInt8((processID & 0x00FF_0000) >> 16),
            UInt8((processID & 0xFF00_0000) >> 24),
        ]
    }

    private static func loginStringField(
        _ value: String,
        password: Bool = false
    ) -> LoginField {
        var bytes: [UInt8] = []
        let utf16 = Array(value.utf16.prefix(Int(UInt16.max)))
        bytes.reserveCapacity(utf16.count * 2)
        for codeUnit in utf16 {
            let encoded: UInt16
            if password {
                let swapped = ((codeUnit << 4) & 0xF0F0) | ((codeUnit >> 4) & 0x0F0F)
                encoded = swapped ^ 0xA5A5
            } else {
                encoded = codeUnit
            }
            bytes.append(UInt8(encoded & 0x00FF))
            bytes.append(UInt8(encoded >> 8))
        }
        return .init(bytes: bytes, length: UInt16(utf16.count), bytesPerLengthUnit: 2)
    }

    private static func boundLoginFieldsToOffsetRange(_ fields: inout [LoginField], firstVariableOffset: Int) {
        var nextOffset = firstVariableOffset
        for index in fields.indices {
            let remainingBytes = max(0, Int(UInt16.max) - nextOffset)
            if fields[index].bytes.count > remainingBytes {
                let units = remainingBytes / fields[index].bytesPerLengthUnit
                let byteCount = units * fields[index].bytesPerLengthUnit
                fields[index].bytes = Array(fields[index].bytes.prefix(byteCount))
                fields[index].length = UInt16(units)
            }
            nextOffset += fields[index].bytes.count
        }
    }

    private static func loginFeatureExtBytes() -> [UInt8] {
        var bytes: [UInt8] = []
        Self.appendFeatureExt(id: 0x09, data: [0x02], to: &bytes) // DATACLASSIFICATION v2.
        Self.appendFeatureExt(id: 0x0D, data: [0x01], to: &bytes) // JSONSUPPORT v1.
        bytes.append(0xFF)
        return bytes
    }

    private static func appendFeatureExt(id: UInt8, data: [UInt8], to bytes: inout [UInt8]) {
        bytes.append(id)
        let length = UInt32(data.count)
        bytes.append(UInt8(length & 0x0000_00FF))
        bytes.append(UInt8((length & 0x0000_FF00) >> 8))
        bytes.append(UInt8((length & 0x00FF_0000) >> 16))
        bytes.append(UInt8((length & 0xFF00_0000) >> 24))
        bytes.append(contentsOf: data)
    }

    private static func loginTypeFlags(for intent: TDSConnection.Configuration.ApplicationIntent) -> UInt8 {
        switch intent {
        case .readWrite:
            return 0x00
        case .readOnly:
            return 0x20
        }
    }

    private static func loginOptionFlags2(
        for authentication: TDSConnection.Configuration.Authentication
    ) -> UInt8 {
        var flags: UInt8 = 0x03
        if case .sspi = authentication {
            flags |= 0x80
        }
        return flags
    }

    private mutating func parameterValue(_ value: TDSData) {
        switch value {
        case .null:
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .typedNull(let type):
            self.typedNullParameterValue(type)
        case .bool(let bool):
            self.buffer.writeInteger(TDSDataType.bitN.rawValue)
            self.buffer.writeInteger(1 as UInt8)
            self.buffer.writeInteger(1 as UInt8)
            self.buffer.writeInteger(bool ? 1 : 0 as UInt8)
        case .tinyInt(let value):
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(1 as UInt8)
            self.buffer.writeInteger(1 as UInt8)
            self.buffer.writeInteger(value)
        case .smallInt(let value):
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(2 as UInt8)
            self.buffer.writeInteger(2 as UInt8)
            self.buffer.writeInteger(value, endianness: .little)
        case .int32(let value):
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(4 as UInt8)
            self.buffer.writeInteger(4 as UInt8)
            self.buffer.writeInteger(value, endianness: .little)
        case .int(let value):
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(value, endianness: .little)
        case .float(let value):
            self.buffer.writeInteger(TDSDataType.floatN.rawValue)
            self.buffer.writeInteger(4 as UInt8)
            self.buffer.writeInteger(4 as UInt8)
            self.buffer.writeInteger(value.bitPattern, endianness: .little)
        case .double(let value):
            self.buffer.writeInteger(TDSDataType.floatN.rawValue)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(value.bitPattern, endianness: .little)
        case .decimal(let value):
            let decimal = Self.encodeDecimal(value)
            self.buffer.writeInteger(TDSDataType.decimalN.rawValue)
            self.buffer.writeInteger(17 as UInt8)
            self.buffer.writeInteger(decimal.precision)
            self.buffer.writeInteger(decimal.scale)
            self.buffer.writeInteger(17 as UInt8)
            self.buffer.writeInteger(decimal.isNegative ? UInt8(0) : UInt8(1))
            self.buffer.writeBytes(decimal.magnitude)
        case .money(let value):
            let scaledValue = Self.encodeFixedScaleDecimal(value, scale: 4)
            self.buffer.writeInteger(TDSDataType.moneyN.rawValue)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(UInt32((UInt64(bitPattern: scaledValue) >> 32) & 0xFFFF_FFFF), endianness: .little)
            self.buffer.writeInteger(UInt32(UInt64(bitPattern: scaledValue) & 0xFFFF_FFFF), endianness: .little)
        case .date(let value):
            self.buffer.writeInteger(TDSDataType.dateN.rawValue)
            self.buffer.writeInteger(3 as UInt8)
            self.writeDate(value)
        case .time(let value):
            self.buffer.writeInteger(TDSDataType.timeN.rawValue)
            self.buffer.writeInteger(value.scale)
            self.buffer.writeInteger(Self.timeStorageLength(scale: value.scale))
            self.writeTime(value)
        case .datetime2(let value):
            self.buffer.writeInteger(TDSDataType.datetime2N.rawValue)
            self.buffer.writeInteger(value.time.scale)
            self.buffer.writeInteger(Self.timeStorageLength(scale: value.time.scale) + 3)
            self.writeTime(value.time)
            self.writeDate(value.date)
        case .datetimeOffset(let value):
            let utcDateTime = value.dateValue().map {
                TDSDateTime($0, scale: value.dateTime.time.scale)
            } ?? value.dateTime
            self.buffer.writeInteger(TDSDataType.datetimeOffsetN.rawValue)
            self.buffer.writeInteger(value.dateTime.time.scale)
            self.buffer.writeInteger(Self.timeStorageLength(scale: value.dateTime.time.scale) + 5)
            self.writeTime(utcDateTime.time)
            self.writeDate(utcDateTime.date)
            self.buffer.writeInteger(Int16(value.offsetMinutes), endianness: .little)
        case .datetime(let value):
            self.buffer.writeInteger(TDSDataType.datetimeN.rawValue)
            self.buffer.writeInteger(8 as UInt8)
            let days = Self.daysSince0001(
                year: value.date.year,
                month: value.date.month,
                day: value.date.day
            ) - Self.daysBeforeYear(1900)
            self.buffer.writeInteger(Int32(days), endianness: .little)
            let seconds = value.time.hour * 3600 + value.time.minute * 60 + value.time.second
            let ticks = seconds * 300 + value.time.nanosecond * 300 / 1_000_000_000
            self.buffer.writeInteger(UInt32(ticks), endianness: .little)
        case .guid(let value):
            self.buffer.writeInteger(TDSDataType.guid.rawValue)
            self.buffer.writeInteger(16 as UInt8)
            self.buffer.writeInteger(16 as UInt8)
            self.writeGUID(value)
        case .string(let value):
            let bytes = Self.utf16Bytes(value)
            self.buffer.writeInteger(TDSDataType.nVarChar.rawValue)
            if bytes.count > 8_000 {
                self.buffer.writeInteger(UInt16.max, endianness: .little)
                self.buffer.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
                self.writePLPBytes(bytes)
                return
            }
            let byteLength = UInt16(bytes.count)
            self.buffer.writeInteger(max(byteLength, 2), endianness: .little)
            self.buffer.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
            self.buffer.writeInteger(byteLength, endianness: .little)
            self.buffer.writeBytes(bytes)
        case .bytes(let value):
            self.buffer.writeInteger(TDSDataType.bigVarBin.rawValue)
            if value.count > 8_000 {
                self.buffer.writeInteger(UInt16.max, endianness: .little)
                self.writePLPBytes(value)
                return
            }
            self.buffer.writeInteger(UInt16(value.count), endianness: .little)
            self.buffer.writeInteger(UInt16(value.count), endianness: .little)
            self.buffer.writeBytes(value)
        case .xml(let value):
            self.buffer.writeInteger(TDSDataType.xml.rawValue)
            self.buffer.writeInteger(0 as UInt8)
            self.writePLPBytes(value)
        case .json(let value):
            self.buffer.writeInteger(TDSDataType.json.rawValue)
            self.writePLPBytes(value)
        case .table(let value):
            self.tableValuedParameter(value)
        }
    }

    private mutating func typedNullParameterValue(_ type: TDSSQLType) {
        switch type {
        case .bit:
            self.buffer.writeInteger(TDSDataType.bitN.rawValue)
            self.buffer.writeInteger(1 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .tinyInt:
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(1 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .smallInt:
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(2 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .int:
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(4 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .bigInt:
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .real:
            self.buffer.writeInteger(TDSDataType.floatN.rawValue)
            self.buffer.writeInteger(4 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .float:
            self.buffer.writeInteger(TDSDataType.floatN.rawValue)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .decimal(let precision, let scale):
            self.buffer.writeInteger(TDSDataType.decimalN.rawValue)
            self.buffer.writeInteger(17 as UInt8)
            self.buffer.writeInteger(TDSSQLType.clampedPrecision(precision))
            self.buffer.writeInteger(TDSSQLType.clampedScale(scale, precision: precision))
            self.buffer.writeInteger(0 as UInt8)
        case .money:
            self.buffer.writeInteger(TDSDataType.moneyN.rawValue)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .date:
            self.buffer.writeInteger(TDSDataType.dateN.rawValue)
            self.buffer.writeInteger(0 as UInt8)
        case .time(let scale):
            self.buffer.writeInteger(TDSDataType.timeN.rawValue)
            self.buffer.writeInteger(TDSSQLType.clampedTemporalScale(scale))
            self.buffer.writeInteger(0 as UInt8)
        case .datetime:
            self.buffer.writeInteger(TDSDataType.datetimeN.rawValue)
            self.buffer.writeInteger(8 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .datetime2(let scale):
            self.buffer.writeInteger(TDSDataType.datetime2N.rawValue)
            self.buffer.writeInteger(TDSSQLType.clampedTemporalScale(scale))
            self.buffer.writeInteger(0 as UInt8)
        case .datetimeOffset(let scale):
            self.buffer.writeInteger(TDSDataType.datetimeOffsetN.rawValue)
            self.buffer.writeInteger(TDSSQLType.clampedTemporalScale(scale))
            self.buffer.writeInteger(0 as UInt8)
        case .uniqueIdentifier:
            self.buffer.writeInteger(TDSDataType.guid.rawValue)
            self.buffer.writeInteger(16 as UInt8)
            self.buffer.writeInteger(0 as UInt8)
        case .char(let maxBytes):
            let maxBytes = TDSSQLType.normalizedFixedSingleByteMaxBytes(maxBytes)
            self.buffer.writeInteger(TDSDataType.bigChar.rawValue)
            self.buffer.writeInteger(maxBytes, endianness: .little)
            self.buffer.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
            self.buffer.writeInteger(UInt16.max, endianness: .little)
        case .varchar(let maxBytes):
            let maxBytes = TDSSQLType.normalizedVarCharMaxBytes(maxBytes)
            self.buffer.writeInteger(TDSDataType.bigVarChar.rawValue)
            self.buffer.writeInteger(maxBytes, endianness: .little)
            self.buffer.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
            if maxBytes == UInt16.max {
                self.buffer.writeInteger(UInt64.max, endianness: .little)
            } else {
                self.buffer.writeInteger(UInt16.max, endianness: .little)
            }
        case .nchar(let maxBytes):
            let maxBytes = TDSSQLType.normalizedFixedNCharMaxBytes(maxBytes)
            self.buffer.writeInteger(TDSDataType.nChar.rawValue)
            self.buffer.writeInteger(maxBytes, endianness: .little)
            self.buffer.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
            self.buffer.writeInteger(UInt16.max, endianness: .little)
        case .nvarchar(let maxBytes):
            let maxBytes = TDSSQLType.normalizedNVarCharMaxBytes(maxBytes)
            self.buffer.writeInteger(TDSDataType.nVarChar.rawValue)
            self.buffer.writeInteger(maxBytes, endianness: .little)
            self.buffer.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
            if maxBytes == UInt16.max {
                self.buffer.writeInteger(UInt64.max, endianness: .little)
            } else {
                self.buffer.writeInteger(UInt16.max, endianness: .little)
            }
        case .binary(let maxBytes):
            let maxBytes = TDSSQLType.normalizedFixedSingleByteMaxBytes(maxBytes)
            self.buffer.writeInteger(TDSDataType.bigBinary.rawValue)
            self.buffer.writeInteger(maxBytes, endianness: .little)
            self.buffer.writeInteger(UInt16.max, endianness: .little)
        case .varbinary(let maxBytes):
            let maxBytes = TDSSQLType.normalizedVarBinaryMaxBytes(maxBytes)
            self.buffer.writeInteger(TDSDataType.bigVarBin.rawValue)
            self.buffer.writeInteger(maxBytes, endianness: .little)
            if maxBytes == UInt16.max {
                self.buffer.writeInteger(UInt64.max, endianness: .little)
            } else {
                self.buffer.writeInteger(UInt16.max, endianness: .little)
            }
        case .xml:
            self.buffer.writeInteger(TDSDataType.xml.rawValue)
            self.buffer.writeInteger(0 as UInt8)
            self.buffer.writeInteger(UInt64.max, endianness: .little)
        case .json:
            self.buffer.writeInteger(TDSDataType.json.rawValue)
            self.buffer.writeInteger(UInt64.max, endianness: .little)
        }
    }

    private mutating func tableValuedParameter(_ value: TDSTableValuedParameter) {
        self.buffer.writeInteger(0xF3 as UInt8)
        self.bVarchar(value.databaseName)
        self.bVarchar(value.schemaName)
        self.bVarchar(value.typeName)
        self.buffer.writeInteger(UInt16(value.columns.count), endianness: .little)

        for column in value.columns {
            self.buffer.writeInteger(column.userType, endianness: .little)
            self.buffer.writeInteger(column.flags, endianness: .little)
            self.tableValuedParameterColumnTypeInfo(column.dataType)
            self.buffer.writeInteger(0 as UInt8)
        }

        self.buffer.writeInteger(0x00 as UInt8)
        for row in value.rows {
            self.buffer.writeInteger(0x01 as UInt8)
            for (index, column) in value.columns.enumerated() {
                let columnValue = index < row.count ? row[index] : .null
                self.tableValuedParameterColumnValue(columnValue, type: column.dataType)
            }
        }
        self.buffer.writeInteger(0x00 as UInt8)
    }

    private mutating func tableValuedParameterColumnTypeInfo(
        _ type: TDSTableValuedParameter.Column.DataType
    ) {
        switch type {
        case .int(let maxBytes):
            self.buffer.writeInteger(TDSDataType.intN.rawValue)
            self.buffer.writeInteger(maxBytes)
        case .bit:
            self.buffer.writeInteger(TDSDataType.bitN.rawValue)
            self.buffer.writeInteger(1 as UInt8)
        case .nVarChar(let maxBytes, let collation):
            self.buffer.writeInteger(TDSDataType.nVarChar.rawValue)
            self.buffer.writeInteger(maxBytes, endianness: .little)
            self.buffer.writeBytes(collation)
        case .varBinary(let maxBytes):
            self.buffer.writeInteger(TDSDataType.bigVarBin.rawValue)
            self.buffer.writeInteger(maxBytes, endianness: .little)
        }
    }

    private mutating func tableValuedParameterColumnValue(
        _ value: TDSData,
        type: TDSTableValuedParameter.Column.DataType
    ) {
        switch type {
        case .int(let maxBytes):
            guard case .int(let int) = value else {
                self.buffer.writeInteger(0 as UInt8)
                return
            }
            self.buffer.writeInteger(maxBytes)
            switch maxBytes {
            case 1:
                self.buffer.writeInteger(UInt8(truncatingIfNeeded: int))
            case 2:
                self.buffer.writeInteger(Int16(truncatingIfNeeded: int), endianness: .little)
            case 4:
                self.buffer.writeInteger(Int32(truncatingIfNeeded: int), endianness: .little)
            default:
                self.buffer.writeInteger(int, endianness: .little)
            }
        case .bit:
            guard case .bool(let bool) = value else {
                self.buffer.writeInteger(0 as UInt8)
                return
            }
            self.buffer.writeInteger(1 as UInt8)
            self.buffer.writeInteger(bool ? UInt8(1) : UInt8(0))
        case .nVarChar(let maxBytes, _):
            guard case .string(let string) = value else {
                self.buffer.writeInteger(UInt16.max, endianness: .little)
                return
            }
            self.writeUSVarCharValue(string, maxBytes: maxBytes)
        case .varBinary(let maxBytes):
            guard case .bytes(let bytes) = value else {
                self.buffer.writeInteger(UInt16.max, endianness: .little)
                return
            }
            self.writeUSVarByteValue(bytes, maxBytes: maxBytes)
        }
    }

    private static func encodeDecimal(
        _ value: String
    ) -> (isNegative: Bool, precision: UInt8, scale: UInt8, magnitude: [UInt8]) {
        var text = value.trimmingCharacters(in: .whitespacesAndNewlines)
        let isNegative = text.first == "-"
        if text.first == "-" || text.first == "+" {
            text.removeFirst()
        }

        let pieces = text.split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false)
        let integerPart = pieces.first.map(String.init) ?? "0"
        let fractionalPart = pieces.count == 2 ? String(pieces[1]) : ""
        let digits = (integerPart + fractionalPart).filter(\.isNumber)
        let normalizedDigits = digits.drop(while: { $0 == "0" })
        let magnitudeDigits = normalizedDigits.isEmpty ? "0" : String(normalizedDigits)
        let scale = UInt8(min(fractionalPart.count, Int(UInt8.max)))
        let precision = UInt8(min(max(magnitudeDigits.count, 1), 38))

        var magnitude = [UInt8](repeating: 0, count: 16)
        var usedBytes = [UInt8](repeating: 0, count: 1)
        for digit in magnitudeDigits {
            guard let digitValue = digit.wholeNumberValue else { continue }
            var carry = digitValue
            for index in usedBytes.indices {
                let value = Int(usedBytes[index]) * 10 + carry
                usedBytes[index] = UInt8(value & 0xFF)
                carry = value >> 8
            }
            while carry > 0 {
                usedBytes.append(UInt8(carry & 0xFF))
                carry >>= 8
            }
        }

        for index in 0..<min(usedBytes.count, magnitude.count) {
            magnitude[index] = usedBytes[index]
        }
        return (isNegative, precision, scale, magnitude)
    }

    private mutating func writeGUID(_ value: TDSGUID) {
        let bytes = Self.guidBytes(from: value.stringValue)
        self.buffer.writeInteger(Self.readHexInteger(bytes[0..<4], as: UInt32.self), endianness: .little)
        self.buffer.writeInteger(Self.readHexInteger(bytes[4..<6], as: UInt16.self), endianness: .little)
        self.buffer.writeInteger(Self.readHexInteger(bytes[6..<8], as: UInt16.self), endianness: .little)
        self.buffer.writeBytes(bytes[8..<16])
    }

    private static func guidBytes(from string: String) -> [UInt8] {
        let hex = string.filter(\.isHexDigit)
        var bytes: [UInt8] = []
        bytes.reserveCapacity(16)
        var index = hex.startIndex
        while index < hex.endIndex {
            let next = hex.index(index, offsetBy: 2)
            bytes.append(UInt8(hex[index..<next], radix: 16) ?? 0)
            index = next
        }
        if bytes.count < 16 {
            bytes.append(contentsOf: repeatElement(0, count: 16 - bytes.count))
        }
        return Array(bytes.prefix(16))
    }

    private static func readHexInteger<T: FixedWidthInteger>(
        _ bytes: ArraySlice<UInt8>,
        as type: T.Type
    ) -> T {
        var value: T = 0
        for byte in bytes {
            value = (value << 8) | T(byte)
        }
        return value
    }

    private static func encodeFixedScaleDecimal(_ value: String, scale: Int) -> Int64 {
        var text = value.trimmingCharacters(in: .whitespacesAndNewlines)
        let isNegative = text.first == "-"
        if text.first == "-" || text.first == "+" {
            text.removeFirst()
        }

        let pieces = text.split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false)
        let integerPart = Int64(pieces.first.map(String.init) ?? "0") ?? 0
        let fractionalText = pieces.count == 2 ? String(pieces[1]) : ""
        let paddedFractionalText = (fractionalText + String(repeating: "0", count: scale)).prefix(scale)
        let fractionalPart = Int64(paddedFractionalText) ?? 0
        let scaled = integerPart * Int64(Self.powerOf10(scale)) + fractionalPart
        return isNegative ? -scaled : scaled
    }

    private mutating func writeDate(_ date: TDSDate) {
        let days = Self.daysSince0001(year: date.year, month: date.month, day: date.day)
        self.writeLittleEndianUnsignedInteger(UInt64(days), byteCount: 3)
    }

    private mutating func writeTime(_ time: TDSTime) {
        let unitsPerSecond = UInt64(Self.powerOf10(Int(time.scale)))
        let seconds = UInt64(time.hour * 3600 + time.minute * 60 + time.second)
        let fractionalUnits = UInt64(time.nanosecond / Self.powerOf10(9 - Int(time.scale)))
        let units = seconds * unitsPerSecond + fractionalUnits
        self.writeLittleEndianUnsignedInteger(units, byteCount: Int(Self.timeStorageLength(scale: time.scale)))
    }

    private static func timeStorageLength(scale: UInt8) -> UInt8 {
        switch scale {
        case 0...2:
            return 3
        case 3...4:
            return 4
        default:
            return 5
        }
    }

    private mutating func writeLittleEndianUnsignedInteger(_ value: UInt64, byteCount: Int) {
        for index in 0..<byteCount {
            self.buffer.writeInteger(UInt8((value >> UInt64(index * 8)) & 0xFF))
        }
    }

    private mutating func writePLPBytes(_ bytes: [UInt8]) {
        self.buffer.writeInteger(UInt64(bytes.count), endianness: .little)
        if !bytes.isEmpty {
            self.buffer.writeInteger(UInt32(bytes.count), endianness: .little)
            self.buffer.writeBytes(bytes)
        }
        self.buffer.writeInteger(0 as UInt32, endianness: .little)
    }

    private static func daysSince0001(year: Int, month: Int, day: Int) -> Int {
        var days = 0
        if year > 1 {
            for y in 1..<year {
                days += Self.isLeapYear(y) ? 366 : 365
            }
        }
        let monthLengths = Self.isLeapYear(year) ?
            [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] :
            [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        days += monthLengths.prefix(month - 1).reduce(0, +)
        days += day - 1
        return days
    }

    private static func daysBeforeYear(_ year: Int) -> Int {
        let previousYear = year - 1
        return previousYear * 365 + previousYear / 4 - previousYear / 100 + previousYear / 400
    }

    private static func isLeapYear(_ year: Int) -> Bool {
        year.isMultiple(of: 4) && (!year.isMultiple(of: 100) || year.isMultiple(of: 400))
    }

    private static func powerOf10(_ exponent: Int) -> Int {
        var value = 1
        for _ in 0..<exponent {
            value *= 10
        }
        return value
    }

    private mutating func endRequest(
        packetType: TDSPacket.MessageType,
        statusFlags: [TDSPacket.StatusFlag] = [.eom]
    ) {
        var statusFlags = statusFlags
        if self.resetConnectionOnNextRequest, Self.supportsResetConnection(packetType: packetType) {
            self.resetConnectionOnNextRequest = false
            if !statusFlags.contains(.resetConnection) {
                statusFlags.append(.resetConnection)
            }
        }
        let length = self.buffer.readableBytes - TDSPacket.headerLength
        let headerLength = min(length, Int(UInt16.max) - TDSPacket.headerLength)
        self.buffer.prepareSend(
            packetType: packetType,
            statusFlags: statusFlags,
            payloadLength: UInt16(headerLength)
        )
    }

    private static func supportsResetConnection(packetType: TDSPacket.MessageType) -> Bool {
        switch packetType {
        case .sqlBatch, .rpc, .transactionManagerRequest, .bulkLoadData:
            return true
        case .prelogin, .preTDS7Login, .tds7Login, .attentionSignal, .sspi,
            .preloginLoginOrTablularResponse, .federatedAuthenticationToken, .sslKickoff:
            return false
        }
    }
}

extension TDSProtocolVersion {
    var wireValue: UInt32 {
        switch self {
        case .v7_4:
            return 0x7400_0004
        case .v8_0:
            return 0x0800_0000
        }
    }

    var supportsFeatureExt: Bool {
        switch self {
        case .v7_4, .v8_0:
            return true
        }
    }
}
