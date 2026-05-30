//===----------------------------------------------------------------------===//
//
// This source file is part of the TDSNIO open source project
//
// Copyright (c) 2026 TDSNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
// See CONTRIBUTORS.md for the list of TDSNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation
import NIOCore

/// A wire message that is created by a TDS server to be consumed by the TDS client.
enum TDSBackendMessage: Sendable {
    case prelogin(PreloginResponse)
    case loginAck(LoginAck)
    case done(Done)
    case doneProc(Done)
    case doneInProc(Done)
    case envChange(EnvChange)
    case error(InfoError)
    case info(InfoError)
    case featureExtAck(FeatureExtAck)
    case colMetadata(ColMetadata)
    case tabName(TabName)
    case colInfo(ColInfo)
    case order(Order)
    case offset(Offset)
    case dataClassification(DataClassification)
    case altMetadata(AltMetadata)
    case altRow(AltRow)
    case sessionState(SessionState)
    case sspi([UInt8])
    case fedAuthInfo(FedAuthInfo)
    case row(Row)
    case returnStatus(Int32)
    case returnValue(ReturnValue)
    case unknownToken(UInt8, ByteBuffer)

    struct PreloginResponse: Sendable, Hashable {
        var version: Version?
        var encryption: TDSFrontendMessageEncoder.PreloginEncryption?
        var mars: Bool?
        var fedAuthRequired: Bool?
        var nonce: [UInt8]?

        struct Version: Sendable, Hashable {
            var major: UInt8
            var minor: UInt8
            var build: UInt16
            var subBuild: UInt16
        }
    }

    struct LoginAck: Sendable, Hashable {
        var interface: UInt8
        var tdsVersion: UInt32
        var programName: String
        var serverVersion: ServerVersion

        struct ServerVersion: Sendable, Hashable {
            var major: UInt8
            var minor: UInt8
            var buildHigh: UInt8
            var buildLow: UInt8
        }
    }

    struct Done: Sendable, Hashable {
        struct Status: OptionSet, Sendable, Hashable {
            let rawValue: UInt16

            static let more = Status(rawValue: 0x0001)
            static let error = Status(rawValue: 0x0002)
            static let inTransaction = Status(rawValue: 0x0004)
            static let count = Status(rawValue: 0x0010)
            static let attention = Status(rawValue: 0x0020)
            static let serverError = Status(rawValue: 0x0100)
        }

        var status: Status
        var currentCommand: UInt16
        var rowCount: UInt64
    }

    struct InfoError: Sendable, Hashable, Error {
        var number: Int32
        var state: UInt8
        var severity: UInt8
        var message: String
        var serverName: String
        var procedureName: String
        var lineNumber: UInt32
    }

    struct FeatureExtAck: Sendable, Hashable {
        var options: [Option]

        struct Option: Sendable, Hashable {
            var featureID: UInt8
            var data: [UInt8]
        }
    }

    struct Offset: Sendable, Hashable {
        var identifier: UInt16
        var offset: UInt16
    }

    struct DataClassification: Sendable, Hashable {
        struct SensitivityLabel: Sendable, Hashable {
            var name: String
            var id: String
        }

        struct InformationType: Sendable, Hashable {
            var name: String
            var id: String
        }

        struct SensitivityProperty: Sendable, Hashable {
            var labelIndex: UInt16
            var informationTypeIndex: UInt16
            var rank: Int32?
        }

        struct Column: Sendable, Hashable {
            var properties: [SensitivityProperty]
        }

        var labels: [SensitivityLabel]
        var informationTypes: [InformationType]
        var columns: [Column]
    }

    struct AltMetadata: Sendable, Hashable {
        struct Column: Sendable, Hashable {
            var op: UInt8
            var operand: UInt16
            var userType: UInt32
            var flags: UInt16
            var typeInfo: ColMetadata.TypeInfo
            var name: String
        }

        var count: UInt16
        var id: UInt16
        var byColumns: [UInt16]
        var columns: [Column]
    }

    struct AltRow: Sendable, Hashable {
        var id: UInt16
        var values: [TDSData]
    }

    struct EnvChange: Sendable {
        enum Value: Sendable {
            case string(new: String, old: String)
            case bytes(new: [UInt8], old: [UInt8])
            case routing(Routing)
            case unknown(ByteBuffer)
        }

        struct Routing: Sendable, Hashable {
            var protocolByte: UInt8
            var port: UInt16
            var server: String
        }

        var type: UInt8
        var value: Value
    }

    struct ColMetadata: Sendable {
        var columnCount: UInt16
        var columns: [Column]

        struct Column: Sendable, Hashable {
            var userType: UInt32
            var flags: UInt16
            var typeInfo: TypeInfo
            var name: String
        }

        struct TypeInfo: Sendable, Hashable {
            var dataType: TDSDataType
            var length: UInt64?
            var collation: [UInt8]
            var precision: UInt8?
            var scale: UInt8?
            var tableName: String?
            var udtInfo: UDTInfo?
            var xmlInfo: XMLInfo?
        }

        struct UDTInfo: Sendable, Hashable {
            var databaseName: String
            var schemaName: String
            var typeName: String
            var assemblyQualifiedName: String
        }

        struct XMLInfo: Sendable, Hashable {
            var databaseName: String
            var owningSchema: String
            var schemaCollection: String
        }
    }

    struct Row: Sendable {
        var values: [TDSData]
    }

    struct TabName: Sendable, Hashable {
        var tableNames: [String]
    }

    struct ColInfo: Sendable, Hashable {
        struct Column: Sendable, Hashable {
            struct Status: OptionSet, Sendable, Hashable {
                let rawValue: UInt8

                static let expression = Status(rawValue: 0x04)
                static let key = Status(rawValue: 0x08)
                static let hidden = Status(rawValue: 0x10)
                static let differentName = Status(rawValue: 0x20)
            }

            var columnNumber: UInt8
            var tableNumber: UInt8
            var status: Status
            var baseColumnName: String?
        }

        var columns: [Column]
    }

    struct Order: Sendable, Hashable {
        var columnNumbers: [UInt16]
    }

    struct SessionState: Sendable, Hashable {
        struct Status: OptionSet, Sendable, Hashable {
            let rawValue: UInt8

            static let recoverable = Status(rawValue: 0x01)
        }

        struct Entry: Sendable, Hashable {
            var stateID: UInt8
            var value: [UInt8]
        }

        var sequenceNumber: UInt32
        var status: Status
        var entries: [Entry]
    }

    struct FedAuthInfo: Sendable, Hashable {
        struct Option: Sendable, Hashable {
            var id: UInt8
            var data: [UInt8]
        }

        var options: [Option]

        var stsURL: String? {
            self.utf16String(for: 0x01)
        }

        var spn: String? {
            self.utf16String(for: 0x02)
        }

        private func utf16String(for id: UInt8) -> String? {
            guard let data = self.options.first(where: { $0.id == id })?.data else {
                return nil
            }
            return String(bytes: data, encoding: .utf16LittleEndian)
        }
    }

    struct ReturnValue: Sendable {
        var ordinal: UInt16
        var name: String
        var status: UInt8
        var userType: UInt32
        var flags: UInt16
        var typeInfo: ColMetadata.TypeInfo
        var value: TDSData
    }
}

extension TDSBackendMessage {
    static func decode(
        from buffer: inout ByteBuffer,
        of packetID: TDSPacket.MessageType,
        context: TDSBackendMessageDecoder.Context
    ) throws -> (TinySequence<TDSBackendMessage>, lastPacket: Bool) {
        let isLastPacket = context.packetStatus.contains(.eom)

        switch packetID {
        case .preloginLoginOrTablularResponse, .prelogin:
            if Self.looksLikePreloginResponse(buffer) {
                return ([.prelogin(try Self.decodePreloginResponse(from: &buffer))], isLastPacket)
            }

            var messages = TinySequence<TDSBackendMessage>()
            try Self.decodeTokenStream(from: &buffer, into: &messages, context: context)
            return (messages, isLastPacket)
        default:
            throw TDSPartialDecodingError.unknownMessageIDReceived(messageID: packetID.rawValue)
        }
    }

    private static func decodeTokenStream(
        from buffer: inout ByteBuffer,
        into messages: inout TinySequence<TDSBackendMessage>,
        context: TDSBackendMessageDecoder.Context
    ) throws {
        while let token = buffer.readInteger(as: UInt8.self) {
            switch token {
            case 0xAD:
                messages.append(.loginAck(try Self.decodeLoginAck(from: &buffer)))
            case 0xFD:
                messages.append(.done(try Self.decodeDone(from: &buffer)))
            case 0xFE:
                messages.append(.doneProc(try Self.decodeDone(from: &buffer)))
            case 0xFF:
                messages.append(.doneInProc(try Self.decodeDone(from: &buffer)))
            case 0xAA:
                messages.append(.error(try Self.decodeInfoError(from: &buffer)))
            case 0xAB:
                messages.append(.info(try Self.decodeInfoError(from: &buffer)))
            case 0xE3:
                messages.append(.envChange(try Self.decodeEnvChange(from: &buffer)))
            case 0xAE:
                let featureExtAck = try Self.decodeFeatureExtAck(from: &buffer)
                if let dataClassification = featureExtAck.options.first(where: { $0.featureID == 0x09 }),
                    let version = dataClassification.data.first
                {
                    context.dataClassificationVersion = version
                }
                messages.append(.featureExtAck(featureExtAck))
            case 0x81:
                let metadata = try Self.decodeColMetadata(from: &buffer)
                context.columns = metadata.columns
                messages.append(.colMetadata(metadata))
            case 0x88:
                let metadata = try Self.decodeAltMetadata(from: &buffer)
                context.altColumns[metadata.id] = metadata.columns
                messages.append(.altMetadata(metadata))
            case 0xA4:
                messages.append(.tabName(try Self.decodeTabName(from: &buffer)))
            case 0xA5:
                messages.append(.colInfo(try Self.decodeColInfo(from: &buffer)))
            case 0xA9:
                messages.append(.order(try Self.decodeOrder(from: &buffer)))
            case 0x78:
                messages.append(.offset(try Self.decodeOffset(from: &buffer)))
            case 0xA3:
                messages.append(
                    .dataClassification(
                        try Self.decodeDataClassification(
                            from: &buffer,
                            version: context.dataClassificationVersion
                        )))
            case 0xD3:
                messages.append(.altRow(try Self.decodeAltRow(from: &buffer, columnsByID: context.altColumns)))
            case 0xD1:
                messages.append(.row(try Self.decodeRow(from: &buffer, columns: context.columns)))
            case 0xD2:
                messages.append(.row(try Self.decodeNBCRow(from: &buffer, columns: context.columns)))
            case 0x79:
                messages.append(.returnStatus(try Self.decodeReturnStatus(from: &buffer)))
            case 0xAC:
                messages.append(.returnValue(try Self.decodeReturnValue(from: &buffer)))
            case 0xED:
                messages.append(.sspi(try Self.decodeSSPI(from: &buffer)))
            case 0xE4:
                messages.append(.sessionState(try Self.decodeSessionState(from: &buffer)))
            case 0xEE:
                messages.append(.fedAuthInfo(try Self.decodeFedAuthInfo(from: &buffer)))
            default:
                let data = buffer.readSlice(length: buffer.readableBytes) ?? ByteBuffer()
                messages.append(.unknownToken(token, data))
            }
        }
    }

    private static func looksLikePreloginResponse(_ buffer: ByteBuffer) -> Bool {
        guard
            let firstToken = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self),
            firstToken == 0x00
        else {
            return false
        }

        var cursor = buffer.readerIndex
        while cursor < buffer.writerIndex {
            guard let token = buffer.getInteger(at: cursor, as: UInt8.self) else { return false }
            if token == 0xFF { return true }
            guard cursor + 5 <= buffer.writerIndex else { return false }
            cursor += 5
        }
        return false
    }

    private static func decodePreloginResponse(
        from buffer: inout ByteBuffer
    ) throws -> PreloginResponse {
        var options: [(token: UInt8, offset: UInt16, length: UInt16)] = []
        var cursor = buffer.readerIndex

        while true {
            guard let token = buffer.getInteger(at: cursor, as: UInt8.self) else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
            }
            cursor += 1
            if token == 0xFF { break }

            guard
                let offset = buffer.getInteger(at: cursor, endianness: .big, as: UInt16.self),
                let length = buffer.getInteger(at: cursor + 2, endianness: .big, as: UInt16.self)
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
            }
            cursor += 4
            options.append((token, offset, length))
        }

        var response = PreloginResponse()
        for option in options {
            guard
                var data = buffer.getSlice(
                    at: buffer.readerIndex + Int(option.offset),
                    length: Int(option.length)
                )
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
            }

            switch option.token {
            case 0x00:
                guard
                    let major = data.readInteger(as: UInt8.self),
                    let minor = data.readInteger(as: UInt8.self),
                    let build = data.readInteger(endianness: .big, as: UInt16.self),
                    let subBuild = data.readInteger(endianness: .big, as: UInt16.self)
                else {
                    throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
                }
                response.version = .init(
                    major: major,
                    minor: minor,
                    build: build,
                    subBuild: subBuild
                )
            case 0x01:
                guard
                    let raw = data.readInteger(as: UInt8.self),
                    let encryption = TDSFrontendMessageEncoder.PreloginEncryption(rawValue: raw)
                else {
                    throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
                }
                response.encryption = encryption
            case 0x04:
                response.mars = data.readInteger(as: UInt8.self).map { $0 != 0 }
            case 0x06:
                response.fedAuthRequired = data.readInteger(as: UInt8.self).map { $0 != 0 }
            case 0x07:
                response.nonce = data.readBytes(length: data.readableBytes)
            default:
                break
            }
        }

        buffer.moveReaderIndex(to: buffer.writerIndex)
        return response
    }

    private static func decodeLoginAck(from buffer: inout ByteBuffer) throws -> LoginAck {
        guard
            let length = buffer.readInteger(endianness: .little, as: UInt16.self),
            var tokenData = buffer.readSlice(length: Int(length)),
            let interface = tokenData.readInteger(as: UInt8.self),
            let tdsVersion = tokenData.readInteger(endianness: .big, as: UInt32.self),
            let programNameLength = tokenData.readInteger(as: UInt8.self),
            let programName = tokenData.readUTF16String(characterCount: Int(programNameLength)),
            let major = tokenData.readInteger(as: UInt8.self),
            let minor = tokenData.readInteger(as: UInt8.self),
            let buildHigh = tokenData.readInteger(as: UInt8.self),
            let buildLow = tokenData.readInteger(as: UInt8.self)
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
        }

        return .init(
            interface: interface,
            tdsVersion: tdsVersion,
            programName: programName,
            serverVersion: .init(major: major, minor: minor, buildHigh: buildHigh, buildLow: buildLow)
        )
    }

    private static func decodeDone(from buffer: inout ByteBuffer) throws -> Done {
        guard
            let status = buffer.readInteger(endianness: .little, as: UInt16.self),
            let currentCommand = buffer.readInteger(endianness: .little, as: UInt16.self),
            let rowCount = buffer.readInteger(endianness: .little, as: UInt64.self)
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
        }

        return .init(
            status: .init(rawValue: status),
            currentCommand: currentCommand,
            rowCount: rowCount
        )
    }

    private static func decodeInfoError(from buffer: inout ByteBuffer) throws -> InfoError {
        guard
            let length = buffer.readInteger(endianness: .little, as: UInt16.self),
            var tokenData = buffer.readSlice(length: Int(length)),
            let number = tokenData.readInteger(endianness: .little, as: Int32.self),
            let state = tokenData.readInteger(as: UInt8.self),
            let severity = tokenData.readInteger(as: UInt8.self),
            let message = tokenData.readUSVarchar(),
            let serverName = tokenData.readBVarchar(),
            let procedureName = tokenData.readBVarchar(),
            let lineNumber = tokenData.readInteger(endianness: .little, as: UInt32.self)
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
        }

        return .init(
            number: number,
            state: state,
            severity: severity,
            message: message,
            serverName: serverName,
            procedureName: procedureName,
            lineNumber: lineNumber
        )
    }

    private static func decodeLengthPrefixedToken(from buffer: inout ByteBuffer) throws -> ByteBuffer {
        guard
            let length = buffer.readInteger(endianness: .little, as: UInt16.self),
            let data = buffer.readSlice(length: Int(length))
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
        }
        return data
    }

    private static func decodeFeatureExtAck(from buffer: inout ByteBuffer) throws -> FeatureExtAck {
        var options: [FeatureExtAck.Option] = []

        while true {
            let featureID = try buffer.readRequiredInteger(as: UInt8.self)
            if featureID == 0xFF {
                return .init(options: options)
            }

            let length = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt32.self))
            options.append(
                .init(
                    featureID: featureID,
                    data: try buffer.readRequiredBytes(length: length)
                ))
        }
    }

    private static func decodeTabName(from buffer: inout ByteBuffer) throws -> TabName {
        var data = try Self.decodeLengthPrefixedToken(from: &buffer)
        var tableNames: [String] = []
        while data.readableBytes > 0 {
            tableNames.append(try Self.decodeTableName(from: &data))
        }
        return .init(tableNames: tableNames)
    }

    private static func decodeColInfo(from buffer: inout ByteBuffer) throws -> ColInfo {
        var data = try Self.decodeLengthPrefixedToken(from: &buffer)
        var columns: [ColInfo.Column] = []

        while data.readableBytes > 0 {
            guard
                let columnNumber = data.readInteger(as: UInt8.self),
                let tableNumber = data.readInteger(as: UInt8.self),
                let rawStatus = data.readInteger(as: UInt8.self)
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: ColInfo.self)
            }

            let status = ColInfo.Column.Status(rawValue: rawStatus)
            let baseColumnName: String?
            if status.contains(.differentName) {
                guard let name = data.readBVarchar() else {
                    throw TDSPartialDecodingError.fieldNotDecodable(type: String.self)
                }
                baseColumnName = name
            } else {
                baseColumnName = nil
            }

            columns.append(
                .init(
                    columnNumber: columnNumber,
                    tableNumber: tableNumber,
                    status: status,
                    baseColumnName: baseColumnName
                ))
        }

        return .init(columns: columns)
    }

    private static func decodeOrder(from buffer: inout ByteBuffer) throws -> Order {
        var data = try Self.decodeLengthPrefixedToken(from: &buffer)
        var columnNumbers: [UInt16] = []
        while data.readableBytes > 0 {
            columnNumbers.append(try data.readRequiredInteger(endianness: .little, as: UInt16.self))
        }
        return .init(columnNumbers: columnNumbers)
    }

    private static func decodeOffset(from buffer: inout ByteBuffer) throws -> Offset {
        .init(
            identifier: try buffer.readRequiredInteger(endianness: .little, as: UInt16.self),
            offset: try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
        )
    }

    private static func decodeDataClassification(
        from buffer: inout ByteBuffer,
        version: UInt8
    ) throws -> DataClassification {
        let labelCount = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
        var labels: [DataClassification.SensitivityLabel] = []
        labels.reserveCapacity(labelCount)
        for _ in 0..<labelCount {
            guard let name = buffer.readBVarchar(), let id = buffer.readBVarchar() else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: DataClassification.self)
            }
            labels.append(.init(name: name, id: id))
        }

        let informationTypeCount = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
        var informationTypes: [DataClassification.InformationType] = []
        informationTypes.reserveCapacity(informationTypeCount)
        for _ in 0..<informationTypeCount {
            guard let name = buffer.readBVarchar(), let id = buffer.readBVarchar() else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: DataClassification.self)
            }
            informationTypes.append(.init(name: name, id: id))
        }

        let columnCount = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
        var columns: [DataClassification.Column] = []
        columns.reserveCapacity(columnCount)
        for _ in 0..<columnCount {
            let propertyCount = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
            var properties: [DataClassification.SensitivityProperty] = []
            properties.reserveCapacity(propertyCount)
            for _ in 0..<propertyCount {
                let labelIndex = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
                let informationTypeIndex = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
                let rank =
                    version >= 2
                    ? try buffer.readRequiredInteger(endianness: .little, as: Int32.self)
                    : nil
                properties.append(
                    .init(
                        labelIndex: labelIndex,
                        informationTypeIndex: informationTypeIndex,
                        rank: rank
                    ))
            }
            columns.append(.init(properties: properties))
        }

        return .init(labels: labels, informationTypes: informationTypes, columns: columns)
    }

    private static func decodeAltMetadata(from buffer: inout ByteBuffer) throws -> AltMetadata {
        let count = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
        let id = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
        let byColumnCount = Int(try buffer.readRequiredInteger(as: UInt8.self))

        var byColumns: [UInt16] = []
        byColumns.reserveCapacity(byColumnCount)
        for _ in 0..<byColumnCount {
            byColumns.append(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
        }

        var columns: [AltMetadata.Column] = []
        columns.reserveCapacity(Int(count))
        for _ in 0..<count {
            let op = try buffer.readRequiredInteger(as: UInt8.self)
            let operand = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
            let userType = try buffer.readRequiredInteger(endianness: .little, as: UInt32.self)
            let flags = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
            let typeInfo = try Self.decodeTypeInfo(from: &buffer)
            guard let name = buffer.readBVarchar() else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: AltMetadata.self)
            }
            columns.append(
                .init(
                    op: op,
                    operand: operand,
                    userType: userType,
                    flags: flags,
                    typeInfo: typeInfo,
                    name: name
                ))
        }

        return .init(count: count, id: id, byColumns: byColumns, columns: columns)
    }

    private static func decodeSSPI(from buffer: inout ByteBuffer) throws -> [UInt8] {
        var data = try Self.decodeLengthPrefixedToken(from: &buffer)
        return data.readBytes(length: data.readableBytes) ?? []
    }

    private static func decodeSessionState(from buffer: inout ByteBuffer) throws -> SessionState {
        var data = try Self.decodeLongLengthPrefixedToken(from: &buffer)
        let sequenceNumber = try data.readRequiredInteger(endianness: .little, as: UInt32.self)
        let status = SessionState.Status(rawValue: try data.readRequiredInteger(as: UInt8.self))
        var entries: [SessionState.Entry] = []

        while data.readableBytes > 0 {
            let stateID = try data.readRequiredInteger(as: UInt8.self)
            let shortLength = try data.readRequiredInteger(as: UInt8.self)
            let length: Int
            if shortLength == UInt8.max {
                length = Int(try data.readRequiredInteger(endianness: .little, as: UInt32.self))
            } else {
                length = Int(shortLength)
            }
            entries.append(
                .init(
                    stateID: stateID,
                    value: try data.readRequiredBytes(length: length)
                ))
        }

        return .init(sequenceNumber: sequenceNumber, status: status, entries: entries)
    }

    private static func decodeFedAuthInfo(from buffer: inout ByteBuffer) throws -> FedAuthInfo {
        var data = try Self.decodeLongLengthPrefixedToken(from: &buffer)
        let count = Int(try data.readRequiredInteger(endianness: .little, as: UInt32.self))
        var descriptors: [(id: UInt8, length: UInt32, offset: UInt32)] = []
        descriptors.reserveCapacity(count)

        for _ in 0..<count {
            descriptors.append(
                (
                    id: try data.readRequiredInteger(as: UInt8.self),
                    length: try data.readRequiredInteger(endianness: .little, as: UInt32.self),
                    offset: try data.readRequiredInteger(endianness: .little, as: UInt32.self)
                ))
        }

        let tokenStart = data.readerIndex - (4 + count * 9)
        var options: [FedAuthInfo.Option] = []
        options.reserveCapacity(count)

        for descriptor in descriptors {
            let start = tokenStart + Int(descriptor.offset)
            guard
                let slice = data.getSlice(
                    at: start,
                    length: Int(descriptor.length)
                )
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: FedAuthInfo.self)
            }
            options.append(
                .init(
                    id: descriptor.id,
                    data: Array(slice.readableBytesView)
                ))
        }

        return .init(options: options)
    }

    private static func decodeLongLengthPrefixedToken(from buffer: inout ByteBuffer) throws -> ByteBuffer {
        guard
            let length = buffer.readInteger(endianness: .little, as: UInt32.self),
            let data = buffer.readSlice(length: Int(length))
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
        }
        return data
    }

    private static func decodeEnvChange(from buffer: inout ByteBuffer) throws -> EnvChange {
        guard
            let length = buffer.readInteger(endianness: .little, as: UInt16.self),
            var data = buffer.readSlice(length: Int(length)),
            let type = data.readInteger(as: UInt8.self)
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
        }

        let value: EnvChange.Value
        switch type {
        case 1, 2, 3, 4, 5, 6, 13, 19:
            guard
                let new = data.readBVarchar(),
                let old = data.readBVarchar()
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: EnvChange.self)
            }
            value = .string(new: new, old: old)
        case 7, 8, 9, 10, 11, 12, 17, 18:
            value = .bytes(
                new: try data.readBVarbyte(),
                old: try data.readBVarbyte()
            )
        case 15:
            value = .bytes(
                new: try data.readLVarbyte(),
                old: data.readBytes(length: data.readableBytes) ?? []
            )
        case 20:
            guard
                let routingDataLength = data.readInteger(endianness: .little, as: UInt16.self),
                Int(routingDataLength) <= data.readableBytes,
                let protocolByte = data.readInteger(as: UInt8.self),
                let port = data.readInteger(endianness: .little, as: UInt16.self),
                let server = data.readUSVarchar()
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: EnvChange.self)
            }
            _ = data.readBytes(length: data.readableBytes)
            value = .routing(.init(protocolByte: protocolByte, port: port, server: server))
        default:
            value = .unknown(data)
        }

        return .init(type: type, value: value)
    }

    private static func decodeColMetadata(from buffer: inout ByteBuffer) throws -> ColMetadata {
        guard let count = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
        }
        if count == UInt16.max {
            return .init(columnCount: count, columns: [])
        }

        var columns: [ColMetadata.Column] = []
        columns.reserveCapacity(Int(count))

        for _ in 0..<count {
            guard
                let userType = buffer.readInteger(endianness: .little, as: UInt32.self),
                let flags = buffer.readInteger(endianness: .little, as: UInt16.self)
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
            }

            let typeInfo = try Self.decodeTypeInfo(from: &buffer)
            guard
                let nameLength = buffer.readInteger(as: UInt8.self),
                let name = buffer.readUTF16String(characterCount: Int(nameLength))
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
            }

            columns.append(
                .init(
                    userType: userType,
                    flags: flags,
                    typeInfo: typeInfo,
                    name: name
                ))
        }

        return .init(columnCount: count, columns: columns)
    }

    private static func decodeTypeInfo(from buffer: inout ByteBuffer) throws -> ColMetadata.TypeInfo {
        guard
            let typeByte = buffer.readInteger(as: UInt8.self),
            let dataType = TDSDataType(rawValue: typeByte)
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ByteBuffer.self)
        }

        var length: UInt64?
        var collation: [UInt8] = []
        var precision: UInt8?
        var scale: UInt8?
        var tableName: String?
        var udtInfo: ColMetadata.UDTInfo?
        var xmlInfo: ColMetadata.XMLInfo?

        switch dataType {
        case .intN, .bitN, .floatN, .moneyN, .datetimeN:
            length = try buffer.readRequiredInteger(as: UInt8.self).asUInt64
        case .sqlVariant:
            length = try buffer.readRequiredInteger(endianness: .little, as: UInt32.self).asUInt64
        case .decimalN, .numericN, .legacyDecimal, .legacyNumeric:
            length = try buffer.readRequiredInteger(as: UInt8.self).asUInt64
            precision = try buffer.readRequiredInteger(as: UInt8.self)
            scale = try buffer.readRequiredInteger(as: UInt8.self)
        case .dateN:
            length = 3
        case .timeN:
            scale = try buffer.readRequiredInteger(as: UInt8.self)
            length = Self.timeStorageLength(scale: scale!)
        case .datetime2N:
            scale = try buffer.readRequiredInteger(as: UInt8.self)
            length = Self.timeStorageLength(scale: scale!) + 3
        case .datetimeOffsetN:
            scale = try buffer.readRequiredInteger(as: UInt8.self)
            length = Self.timeStorageLength(scale: scale!) + 5
        case .legacyVarBin, .legacyBinary, .legacyVarChar, .legacyChar:
            length = try buffer.readRequiredInteger(as: UInt8.self).asUInt64
        case .bigVarBin, .bigBinary:
            length = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self).asUInt64
        case .bigVarChar, .bigChar, .nVarChar, .nChar:
            length = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self).asUInt64
            collation = try buffer.readRequiredBytes(length: 5)
        case .image, .text, .nText:
            length = try buffer.readRequiredInteger(endianness: .little, as: UInt32.self).asUInt64
            if dataType == .text || dataType == .nText {
                collation = try buffer.readRequiredBytes(length: 5)
            }
            tableName = try Self.decodeTableName(from: &buffer)
        case .xml:
            xmlInfo = try Self.decodeXMLInfo(from: &buffer)
        case .json:
            break
        case .udt:
            let info = try Self.decodeUDTInfo(from: &buffer)
            length = info.maxByteSize.asUInt64
            udtInfo = info.udtInfo
        case .null, .int1, .bit, .int2, .int4, .int8, .float4, .float8, .money, .money4, .datetime,
            .datetime4, .guid:
            break
        }

        return .init(
            dataType: dataType,
            length: length,
            collation: collation,
            precision: precision,
            scale: scale,
            tableName: tableName,
            udtInfo: udtInfo,
            xmlInfo: xmlInfo
        )
    }

    private static func decodeTableName(from buffer: inout ByteBuffer) throws -> String {
        let partCount = Int(try buffer.readRequiredInteger(as: UInt8.self))
        var parts: [String] = []
        parts.reserveCapacity(partCount)
        for _ in 0..<partCount {
            guard let part = buffer.readUSVarchar() else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            parts.append(part)
        }
        return parts.joined(separator: ".")
    }

    private static func decodeXMLInfo(from buffer: inout ByteBuffer) throws -> ColMetadata.XMLInfo? {
        let schemaPresent = try buffer.readRequiredInteger(as: UInt8.self)
        guard schemaPresent != 0 else {
            return nil
        }

        guard
            let databaseName = buffer.readBVarchar(),
            let owningSchema = buffer.readBVarchar(),
            let schemaCollection = buffer.readUSVarchar()
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: String.self)
        }
        return .init(
            databaseName: databaseName,
            owningSchema: owningSchema,
            schemaCollection: schemaCollection
        )
    }

    private static func decodeUDTInfo(
        from buffer: inout ByteBuffer
    ) throws -> (maxByteSize: UInt16, udtInfo: ColMetadata.UDTInfo) {
        guard
            let maxByteSize = buffer.readInteger(endianness: .little, as: UInt16.self),
            let databaseName = buffer.readBVarchar(),
            let schemaName = buffer.readBVarchar(),
            let typeName = buffer.readBVarchar(),
            let assemblyQualifiedName = buffer.readUSVarchar()
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ColMetadata.UDTInfo.self)
        }

        return (
            maxByteSize,
            .init(
                databaseName: databaseName,
                schemaName: schemaName,
                typeName: typeName,
                assemblyQualifiedName: assemblyQualifiedName
            )
        )
    }

    private static func decodeRow(
        from buffer: inout ByteBuffer,
        columns: [ColMetadata.Column]
    ) throws -> Row {
        var values: [TDSData] = []
        values.reserveCapacity(columns.count)
        for column in columns {
            values.append(try Self.decodeValue(from: &buffer, typeInfo: column.typeInfo))
        }
        return .init(values: values)
    }

    private static func decodeNBCRow(
        from buffer: inout ByteBuffer,
        columns: [ColMetadata.Column]
    ) throws -> Row {
        let nullBitmapLength = (columns.count + 7) / 8
        let nullBitmap = try buffer.readRequiredBytes(length: nullBitmapLength)
        var values: [TDSData] = []
        values.reserveCapacity(columns.count)

        for index in columns.indices {
            let bitmapByte = nullBitmap[index / 8]
            let isNull = (bitmapByte & (1 << UInt8(index % 8))) != 0
            if isNull {
                values.append(.null)
            } else {
                values.append(try Self.decodeValue(from: &buffer, typeInfo: columns[index].typeInfo))
            }
        }
        return .init(values: values)
    }

    private static func decodeAltRow(
        from buffer: inout ByteBuffer,
        columnsByID: [UInt16: [AltMetadata.Column]]
    ) throws -> AltRow {
        let id = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
        guard let columns = columnsByID[id] else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: AltRow.self)
        }

        var values: [TDSData] = []
        values.reserveCapacity(columns.count)
        for column in columns {
            values.append(try Self.decodeValue(from: &buffer, typeInfo: column.typeInfo))
        }
        return .init(id: id, values: values)
    }

    private static func decodeReturnStatus(from buffer: inout ByteBuffer) throws -> Int32 {
        try buffer.readRequiredInteger(endianness: .little, as: Int32.self)
    }

    private static func decodeReturnValue(from buffer: inout ByteBuffer) throws -> ReturnValue {
        guard
            let ordinal = buffer.readInteger(endianness: .little, as: UInt16.self),
            let name = buffer.readBVarchar(),
            let status = buffer.readInteger(as: UInt8.self),
            let userType = buffer.readInteger(endianness: .little, as: UInt32.self),
            let flags = buffer.readInteger(endianness: .little, as: UInt16.self)
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: ReturnValue.self)
        }

        let typeInfo = try Self.decodeTypeInfo(from: &buffer)
        let value = try Self.decodeValue(from: &buffer, typeInfo: typeInfo)
        return .init(
            ordinal: ordinal,
            name: name,
            status: status,
            userType: userType,
            flags: flags,
            typeInfo: typeInfo,
            value: value
        )
    }

    private static func decodeValue(
        from buffer: inout ByteBuffer,
        typeInfo: ColMetadata.TypeInfo
    ) throws -> TDSData {
        switch typeInfo.dataType {
        case .null:
            return .null
        case .int1:
            return .tinyInt(try buffer.readRequiredInteger(as: UInt8.self))
        case .int2:
            return .smallInt(try buffer.readRequiredInteger(endianness: .little, as: Int16.self))
        case .int4:
            return .int32(try buffer.readRequiredInteger(endianness: .little, as: Int32.self))
        case .int8:
            return .int(try buffer.readRequiredInteger(endianness: .little, as: Int64.self))
        case .intN:
            let length = Int(try buffer.readRequiredInteger(as: UInt8.self))
            return try Self.decodeVariableInt(from: &buffer, length: length)
        case .bit:
            return .bool(try buffer.readRequiredInteger(as: UInt8.self) != 0)
        case .bitN:
            let length = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if length == 0 { return .null }
            return .bool(try buffer.readRequiredInteger(as: UInt8.self) != 0)
        case .float4:
            let raw = try buffer.readRequiredInteger(endianness: .little, as: UInt32.self)
            return .float(Float(bitPattern: raw))
        case .float8:
            let raw = try buffer.readRequiredInteger(endianness: .little, as: UInt64.self)
            return .double(Double(bitPattern: raw))
        case .floatN:
            let length = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if length == 0 { return .null }
            if length == 4 {
                let raw = try buffer.readRequiredInteger(endianness: .little, as: UInt32.self)
                return .float(Float(bitPattern: raw))
            }
            if length == 8 {
                let raw = try buffer.readRequiredInteger(endianness: .little, as: UInt64.self)
                return .double(Double(bitPattern: raw))
            }
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
        case .legacyVarChar, .legacyChar:
            let length = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if length == Int(UInt8.max) { return .null }
            let bytes = try buffer.readRequiredBytes(length: length)
            return .string(String(decoding: bytes, as: UTF8.self))
        case .text:
            guard let bytes = try Self.decodeLegacyLOBBytes(from: &buffer) else {
                return .null
            }
            return .string(String(decoding: bytes, as: UTF8.self))
        case .nText:
            guard let bytes = try Self.decodeLegacyLOBBytes(from: &buffer) else {
                return .null
            }
            guard let string = String(bytes: bytes, encoding: .utf16LittleEndian) else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            return .string(string)
        case .bigVarChar:
            if typeInfo.length == UInt64(UInt16.max) {
                guard let bytes = try Self.decodePLPBytes(from: &buffer) else {
                    return .null
                }
                return .string(String(decoding: bytes, as: UTF8.self))
            }
            fallthrough
        case .bigChar:
            let length = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
            if length == Int(UInt16.max) { return .null }
            let bytes = try buffer.readRequiredBytes(length: length)
            return .string(String(decoding: bytes, as: UTF8.self))
        case .nVarChar:
            if typeInfo.length == UInt64(UInt16.max) {
                guard let bytes = try Self.decodePLPBytes(from: &buffer) else {
                    return .null
                }
                guard let string = String(bytes: bytes, encoding: .utf16LittleEndian) else {
                    throw TDSPartialDecodingError.fieldNotDecodable(type: String.self)
                }
                return .string(string)
            }
            fallthrough
        case .nChar:
            let length = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
            if length == Int(UInt16.max) { return .null }
            guard let string = String(bytes: try buffer.readRequiredBytes(length: length), encoding: .utf16LittleEndian)
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            return .string(string)
        case .legacyVarBin, .legacyBinary:
            let length = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if length == Int(UInt8.max) { return .null }
            return .bytes(try buffer.readRequiredBytes(length: length))
        case .image:
            guard let bytes = try Self.decodeLegacyLOBBytes(from: &buffer) else {
                return .null
            }
            return .bytes(bytes)
        case .bigVarBin:
            if typeInfo.length == UInt64(UInt16.max) {
                guard let bytes = try Self.decodePLPBytes(from: &buffer) else {
                    return .null
                }
                return .bytes(bytes)
            }
            fallthrough
        case .bigBinary:
            let length = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
            if length == Int(UInt16.max) { return .null }
            return .bytes(try buffer.readRequiredBytes(length: length))
        case .xml:
            guard let bytes = try Self.decodePLPBytes(from: &buffer) else {
                return .null
            }
            return .xml(bytes)
        case .json:
            guard let bytes = try Self.decodePLPBytes(from: &buffer) else {
                return .null
            }
            return .json(bytes)
        case .sqlVariant:
            return try Self.decodeSQLVariant(from: &buffer)
        case .udt:
            guard let bytes = try Self.decodePLPBytes(from: &buffer) else {
                return .null
            }
            return .bytes(bytes)
        case .guid:
            let length = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if length == 0 { return .null }
            guard length == 16 else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: TDSGUID.self)
            }
            return .guid(.init(try Self.decodeGUID(from: &buffer)))
        case .decimalN, .numericN, .legacyDecimal, .legacyNumeric:
            return try Self.decodeDecimal(from: &buffer, typeInfo: typeInfo)
        case .dateN:
            return try Self.decodeDateN(from: &buffer)
        case .timeN:
            return try Self.decodeTimeN(from: &buffer, scale: typeInfo.scale ?? 0)
        case .datetime2N:
            return try Self.decodeDateTime2N(from: &buffer, scale: typeInfo.scale ?? 0)
        case .datetimeOffsetN:
            return try Self.decodeDateTimeOffsetN(from: &buffer, scale: typeInfo.scale ?? 0)
        case .money:
            return try Self.decodeMoney(from: &buffer, byteCount: 8)
        case .money4:
            return try Self.decodeMoney(from: &buffer, byteCount: 4)
        case .moneyN:
            let length = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if length == 0 { return .null }
            return try Self.decodeMoney(from: &buffer, byteCount: length)
        case .datetime:
            return try Self.decodeLegacyDateTime(from: &buffer, byteCount: 8)
        case .datetime4:
            return try Self.decodeLegacyDateTime(from: &buffer, byteCount: 4)
        case .datetimeN:
            let length = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if length == 0 { return .null }
            return try Self.decodeLegacyDateTime(from: &buffer, byteCount: length)
        }
    }

    private static func decodePLPBytes(from buffer: inout ByteBuffer) throws -> [UInt8]? {
        let plpNull = UInt64.max
        guard let totalLength = buffer.readInteger(endianness: .little, as: UInt64.self) else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: [UInt8].self)
        }
        if totalLength == plpNull {
            return nil
        }

        var bytes: [UInt8] = []
        if totalLength != UInt64.max - 1 {
            bytes.reserveCapacity(Int(totalLength))
        }

        while true {
            let chunkLength = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt32.self))
            if chunkLength == 0 {
                return bytes
            }
            bytes.append(contentsOf: try buffer.readRequiredBytes(length: chunkLength))
        }
    }

    private static func decodeLegacyLOBBytes(from buffer: inout ByteBuffer) throws -> [UInt8]? {
        let textPointerLength = Int(try buffer.readRequiredInteger(as: UInt8.self))
        if textPointerLength == 0 {
            return nil
        }
        _ = try buffer.readRequiredBytes(length: textPointerLength)
        _ = try buffer.readRequiredBytes(length: 8)
        let length = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt32.self))
        return try buffer.readRequiredBytes(length: length)
    }

    private static func decodeSQLVariant(from buffer: inout ByteBuffer) throws -> TDSData {
        let length = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt32.self))
        if length == 0 {
            return .null
        }

        guard var value = buffer.readSlice(length: length) else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
        }
        guard
            let baseTypeByte = value.readInteger(as: UInt8.self),
            let baseType = TDSDataType(rawValue: baseTypeByte),
            let propertyLength = value.readInteger(as: UInt8.self)
        else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
        }

        let properties = try value.readRequiredBytes(length: Int(propertyLength))
        switch baseType {
        case .null:
            return .null
        case .int1:
            return .tinyInt(try value.readRequiredInteger(as: UInt8.self))
        case .bit:
            return .bool(try value.readRequiredInteger(as: UInt8.self) != 0)
        case .int2:
            return .smallInt(try value.readRequiredInteger(endianness: .little, as: Int16.self))
        case .int4:
            return .int32(try value.readRequiredInteger(endianness: .little, as: Int32.self))
        case .int8:
            return .int(try value.readRequiredInteger(endianness: .little, as: Int64.self))
        case .float4:
            let raw = try value.readRequiredInteger(endianness: .little, as: UInt32.self)
            return .float(Float(bitPattern: raw))
        case .float8:
            let raw = try value.readRequiredInteger(endianness: .little, as: UInt64.self)
            return .double(Double(bitPattern: raw))
        case .guid:
            return .guid(.init(try Self.decodeGUID(from: &value)))
        case .money:
            return try Self.decodeMoney(from: &value, byteCount: 8)
        case .money4:
            return try Self.decodeMoney(from: &value, byteCount: 4)
        case .datetime:
            return try Self.decodeLegacyDateTime(from: &value, byteCount: 8)
        case .datetime4:
            return try Self.decodeLegacyDateTime(from: &value, byteCount: 4)
        case .dateN:
            return try Self.decodeDate(from: &value)
        case .timeN:
            guard properties.count == 1 else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
            }
            return try .time(Self.decodeTime(from: &value, scale: properties[0]))
        case .datetime2N:
            guard properties.count == 1 else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
            }
            let time = try Self.decodeTime(from: &value, scale: properties[0])
            let date = try Self.decodeDateValue(from: &value)
            return .datetime2(.init(date: date, time: time))
        case .datetimeOffsetN:
            guard properties.count == 1 else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
            }
            let time = try Self.decodeTime(from: &value, scale: properties[0])
            let date = try Self.decodeDateValue(from: &value)
            let offset = try value.readRequiredInteger(endianness: .little, as: Int16.self)
            let dateTime = Self.localDateTime(fromUTCDate: date, time: time, offsetMinutes: Int(offset))
            return .datetimeOffset(
                .init(
                    dateTime: dateTime,
                    offsetMinutes: Int(offset)
                ))
        case .decimalN, .numericN:
            guard properties.count == 2 else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
            }
            let sign = try value.readRequiredInteger(as: UInt8.self)
            let magnitude = try value.readRequiredBytes(length: value.readableBytes)
            var decimal = Self.decimalString(
                fromLittleEndianMagnitude: magnitude,
                scale: Int(properties[1])
            )
            if sign == 0 && decimal != "0" {
                decimal.insert("-", at: decimal.startIndex)
            }
            return .decimal(decimal)
        case .bigVarChar, .bigChar:
            return .string(String(decoding: try value.readRequiredBytes(length: value.readableBytes), as: UTF8.self))
        case .nVarChar, .nChar:
            guard
                let string = String(
                    bytes: try value.readRequiredBytes(length: value.readableBytes),
                    encoding: .utf16LittleEndian
                )
            else {
                throw TDSPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            return .string(string)
        case .bigVarBin, .bigBinary:
            return .bytes(try value.readRequiredBytes(length: value.readableBytes))
        default:
            return .bytes(try value.readRequiredBytes(length: value.readableBytes))
        }
    }

    private static func decodeGUID(from buffer: inout ByteBuffer) throws -> String {
        let data1 = try buffer.readRequiredInteger(endianness: .little, as: UInt32.self)
        let data2 = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
        let data3 = try buffer.readRequiredInteger(endianness: .little, as: UInt16.self)
        let data4 = try buffer.readRequiredBytes(length: 8)
        return [
            Self.hex(data1, width: 8),
            Self.hex(data2, width: 4),
            Self.hex(data3, width: 4),
            data4.prefix(2).map { Self.hex($0, width: 2) }.joined(),
            data4.suffix(6).map { Self.hex($0, width: 2) }.joined(),
        ].joined(separator: "-")
    }

    private static func hex<T: FixedWidthInteger>(_ value: T, width: Int) -> String {
        let text = String(value, radix: 16)
        return String(repeating: "0", count: max(0, width - text.count)) + text
    }

    private static func decodeDecimal(
        from buffer: inout ByteBuffer,
        typeInfo: ColMetadata.TypeInfo
    ) throws -> TDSData {
        let valueLength = Int(try buffer.readRequiredInteger(as: UInt8.self))
        if valueLength == 0 {
            return .null
        }

        let sign = try buffer.readRequiredInteger(as: UInt8.self)
        let magnitude = try buffer.readRequiredBytes(length: valueLength - 1)
        let scale = Int(typeInfo.scale ?? 0)
        var decimal = Self.decimalString(fromLittleEndianMagnitude: magnitude, scale: scale)
        if sign == 0 && decimal != "0" {
            decimal.insert("-", at: decimal.startIndex)
        }
        return .decimal(decimal)
    }

    private static func decimalString(
        fromLittleEndianMagnitude magnitude: [UInt8],
        scale: Int
    ) -> String {
        var digits = [0]
        for byte in magnitude.reversed() {
            var carry = Int(byte)
            for index in digits.indices {
                let value = digits[index] * 256 + carry
                digits[index] = value % 10
                carry = value / 10
            }
            while carry > 0 {
                digits.append(carry % 10)
                carry /= 10
            }
        }

        while digits.count > 1 && digits.last == 0 {
            digits.removeLast()
        }

        if scale == 0 {
            return digits.reversed().map(String.init).joined()
        }

        while digits.count <= scale {
            digits.append(0)
        }
        let reversedDigits = digits.reversed().map(String.init)
        let integerDigitCount = reversedDigits.count - scale
        let integerPart = reversedDigits.prefix(integerDigitCount).joined()
        let fractionalPart = reversedDigits.suffix(scale).joined()
        return "\(integerPart).\(fractionalPart)"
    }

    private static func decodeDate(from buffer: inout ByteBuffer) throws -> TDSData {
        .date(try Self.decodeDateValue(from: &buffer))
    }

    private static func decodeDateN(from buffer: inout ByteBuffer) throws -> TDSData {
        let valueLength = Int(try buffer.readRequiredInteger(as: UInt8.self))
        if valueLength == 0 {
            return .null
        }
        guard valueLength == 3 else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSDate.self)
        }
        return try Self.decodeDate(from: &buffer)
    }

    private static func decodeTimeN(from buffer: inout ByteBuffer, scale: UInt8) throws -> TDSData {
        let valueLength = Int(try buffer.readRequiredInteger(as: UInt8.self))
        if valueLength == 0 {
            return .null
        }
        guard valueLength == Self.timeStorageLength(scale: scale) else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSTime.self)
        }
        return try .time(Self.decodeTime(from: &buffer, scale: scale))
    }

    private static func decodeDateTime2N(from buffer: inout ByteBuffer, scale: UInt8) throws -> TDSData {
        let valueLength = Int(try buffer.readRequiredInteger(as: UInt8.self))
        if valueLength == 0 {
            return .null
        }
        guard valueLength == Self.timeStorageLength(scale: scale) + 3 else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSDateTime.self)
        }
        let time = try Self.decodeTime(from: &buffer, scale: scale)
        let date = try Self.decodeDateValue(from: &buffer)
        return .datetime2(.init(date: date, time: time))
    }

    private static func decodeDateTimeOffsetN(from buffer: inout ByteBuffer, scale: UInt8) throws -> TDSData {
        let valueLength = Int(try buffer.readRequiredInteger(as: UInt8.self))
        if valueLength == 0 {
            return .null
        }
        guard valueLength == Self.timeStorageLength(scale: scale) + 5 else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSDateTimeOffset.self)
        }
        let time = try Self.decodeTime(from: &buffer, scale: scale)
        let date = try Self.decodeDateValue(from: &buffer)
        let offset = try buffer.readRequiredInteger(endianness: .little, as: Int16.self)
        let dateTime = Self.localDateTime(fromUTCDate: date, time: time, offsetMinutes: Int(offset))
        return .datetimeOffset(
            .init(
                dateTime: dateTime,
                offsetMinutes: Int(offset)
            ))
    }

    private static func localDateTime(
        fromUTCDate date: TDSDate,
        time: TDSTime,
        offsetMinutes: Int
    ) -> TDSDateTime {
        guard let utcDate = TDSDateTime(date: date, time: time).dateValue() else {
            return .init(date: date, time: time)
        }
        return TDSDateTime(
            utcDate.addingTimeInterval(TimeInterval(offsetMinutes * 60)),
            scale: time.scale
        )
    }

    private static func decodeMoney(from buffer: inout ByteBuffer, byteCount: Int) throws -> TDSData {
        let scaledValue: Int64
        switch byteCount {
        case 4:
            scaledValue = Int64(try buffer.readRequiredInteger(endianness: .little, as: Int32.self))
        case 8:
            let high = try buffer.readRequiredInteger(endianness: .little, as: UInt32.self)
            let low = try buffer.readRequiredInteger(endianness: .little, as: UInt32.self)
            let bits = (UInt64(high) << 32) | UInt64(low)
            scaledValue = Int64(bitPattern: bits)
        default:
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
        }
        return .money(Self.fixedScaleDecimalString(scaledValue, scale: 4))
    }

    private static func decodeLegacyDateTime(
        from buffer: inout ByteBuffer,
        byteCount: Int
    ) throws -> TDSData {
        switch byteCount {
        case 4:
            let days = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
            let minutes = Int(try buffer.readRequiredInteger(endianness: .little, as: UInt16.self))
            let date = Self.gregorianDate(daysSince0001: Self.daysBeforeYear(1900) + days)
            let time = TDSTime(
                hour: minutes / 60,
                minute: minutes % 60,
                second: 0,
                nanosecond: 0,
                scale: 0
            )
            return .datetime(.init(date: date, time: time))
        case 8:
            let days = Int(try buffer.readRequiredInteger(endianness: .little, as: Int32.self))
            let ticks = UInt64(try buffer.readRequiredInteger(endianness: .little, as: UInt32.self))
            let totalNanoseconds = ticks * 1_000_000_000 / 300
            let date = Self.gregorianDate(daysSince0001: Self.daysBeforeYear(1900) + days)
            let time = TDSTime(
                hour: Int(totalNanoseconds / 3_600_000_000_000),
                minute: Int(totalNanoseconds / 60_000_000_000) % 60,
                second: Int(totalNanoseconds / 1_000_000_000) % 60,
                nanosecond: Int(totalNanoseconds % 1_000_000_000),
                scale: 3
            )
            return .datetime(.init(date: date, time: time))
        default:
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
        }
    }

    private static func decodeDateValue(from buffer: inout ByteBuffer) throws -> TDSDate {
        let days = Int(try Self.readLittleEndianUnsignedInteger(from: &buffer, byteCount: 3))
        return Self.gregorianDate(daysSince0001: days)
    }

    private static func decodeTime(from buffer: inout ByteBuffer, scale: UInt8) throws -> TDSTime {
        let byteCount = Int(Self.timeStorageLength(scale: scale))
        let units = try Self.readLittleEndianUnsignedInteger(from: &buffer, byteCount: byteCount)
        let nanosPerUnit = Self.powerOf10(9 - Int(scale))
        let totalNanoseconds = units * UInt64(nanosPerUnit)
        let hour = Int(totalNanoseconds / 3_600_000_000_000)
        let minute = Int(totalNanoseconds / 60_000_000_000) % 60
        let second = Int(totalNanoseconds / 1_000_000_000) % 60
        let nanosecond = Int(totalNanoseconds % 1_000_000_000)
        return .init(
            hour: hour,
            minute: minute,
            second: second,
            nanosecond: nanosecond,
            scale: scale
        )
    }

    private static func readLittleEndianUnsignedInteger(
        from buffer: inout ByteBuffer,
        byteCount: Int
    ) throws -> UInt64 {
        let bytes = try buffer.readRequiredBytes(length: byteCount)
        var value: UInt64 = 0
        for (index, byte) in bytes.enumerated() {
            value |= UInt64(byte) << UInt64(index * 8)
        }
        return value
    }

    private static func gregorianDate(daysSince0001 days: Int) -> TDSDate {
        var day = days
        let years400 = day / 146_097
        day %= 146_097
        let years100 = min(day / 36_524, 3)
        day -= years100 * 36_524
        let years4 = day / 1_461
        day %= 1_461
        let years1 = min(day / 365, 3)
        day -= years1 * 365

        let year = years400 * 400 + years100 * 100 + years4 * 4 + years1 + 1
        let monthLengths =
            Self.isLeapYear(year)
            ? [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] : [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        var month = 1
        while day >= monthLengths[month - 1] {
            day -= monthLengths[month - 1]
            month += 1
        }

        return .init(year: year, month: month, day: day + 1)
    }

    private static func isLeapYear(_ year: Int) -> Bool {
        year.isMultiple(of: 4) && (!year.isMultiple(of: 100) || year.isMultiple(of: 400))
    }

    private static func daysBeforeYear(_ year: Int) -> Int {
        let previousYear = year - 1
        return previousYear * 365 + previousYear / 4 - previousYear / 100 + previousYear / 400
    }

    private static func fixedScaleDecimalString(_ scaledValue: Int64, scale: Int) -> String {
        let isNegative = scaledValue < 0
        let magnitude = UInt64(scaledValue.magnitude)
        let divisor = UInt64(Self.powerOf10(scale))
        let integerPart = magnitude / divisor
        let fractionalPart = magnitude % divisor
        let fractionalText = String(fractionalPart)
        let paddedFractionalText = String(repeating: "0", count: max(0, scale - fractionalText.count)) + fractionalText
        var value = "\(integerPart).\(paddedFractionalText)"
        if isNegative && value != "0.0000" {
            value.insert("-", at: value.startIndex)
        }
        return value
    }

    private static func powerOf10(_ exponent: Int) -> Int {
        var value = 1
        for _ in 0..<exponent {
            value *= 10
        }
        return value
    }

    private static func decodeVariableInt(
        from buffer: inout ByteBuffer,
        length: Int
    ) throws -> TDSData {
        switch length {
        case 0:
            return .null
        case 1:
            return .tinyInt(try buffer.readRequiredInteger(as: UInt8.self))
        case 2:
            return .smallInt(try buffer.readRequiredInteger(endianness: .little, as: Int16.self))
        case 4:
            return .int32(try buffer.readRequiredInteger(endianness: .little, as: Int32.self))
        case 8:
            return .int(try buffer.readRequiredInteger(endianness: .little, as: Int64.self))
        default:
            throw TDSPartialDecodingError.fieldNotDecodable(type: TDSData.self)
        }
    }

    private static func decodeOpaqueValue(
        from buffer: inout ByteBuffer,
        typeInfo: ColMetadata.TypeInfo
    ) throws -> TDSData {
        let length: Int
        switch typeInfo.dataType {
        case .money4, .datetime4:
            length = 4
        case .money, .datetime:
            length = 8
        case .dateN:
            let valueLength = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if valueLength == 0 { return .null }
            length = valueLength
        case .timeN, .datetime2N, .datetimeOffsetN:
            let valueLength = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if valueLength == 0 { return .null }
            length = valueLength
        case .moneyN, .datetimeN, .decimalN, .numericN:
            let valueLength = Int(try buffer.readRequiredInteger(as: UInt8.self))
            if valueLength == 0 { return .null }
            length = valueLength
        default:
            length = Int(typeInfo.length ?? 0)
        }
        return .bytes(try buffer.readRequiredBytes(length: length))
    }

    private static func timeStorageLength(scale: UInt8) -> UInt64 {
        switch scale {
        case 0...2:
            return 3
        case 3...4:
            return 4
        default:
            return 5
        }
    }
}

extension TDSBackendMessage: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .prelogin(let response): ".prelogin(\(response))"
        case .loginAck(let ack): ".loginAck(\(ack))"
        case .done(let done): ".done(\(done))"
        case .doneProc(let done): ".doneProc(\(done))"
        case .doneInProc(let done): ".doneInProc(\(done))"
        case .envChange(let envChange): ".envChange(\(envChange))"
        case .error(let error): ".error(\(error))"
        case .info(let info): ".info(\(info))"
        case .featureExtAck(let data): ".featureExtAck(\(data))"
        case .colMetadata(let metadata): ".colMetadata(\(metadata))"
        case .tabName(let tabName): ".tabName(\(tabName))"
        case .colInfo(let colInfo): ".colInfo(\(colInfo))"
        case .order(let order): ".order(\(order))"
        case .offset(let offset): ".offset(\(offset))"
        case .dataClassification(let dataClassification): ".dataClassification(\(dataClassification))"
        case .altMetadata(let altMetadata): ".altMetadata(\(altMetadata))"
        case .altRow(let altRow): ".altRow(\(altRow))"
        case .sessionState(let sessionState): ".sessionState(\(sessionState))"
        case .sspi(let sspi): ".sspi(\(sspi))"
        case .fedAuthInfo(let fedAuthInfo): ".fedAuthInfo(\(fedAuthInfo))"
        case .row(let row): ".row(\(row))"
        case .returnStatus(let status): ".returnStatus(\(status))"
        case .returnValue(let value): ".returnValue(\(value))"
        case .unknownToken(let token, let data): ".unknownToken(\(token), \(data))"
        }
    }
}

extension FixedWidthInteger {
    fileprivate var asUInt64: UInt64 { UInt64(self) }
}

extension ByteBuffer {
    fileprivate mutating func readUTF16String(characterCount: Int) -> String? {
        guard let bytes = self.readBytes(length: characterCount * 2) else {
            return nil
        }
        return String(bytes: bytes, encoding: .utf16LittleEndian)
    }

    fileprivate mutating func readBVarchar() -> String? {
        guard let characterCount = self.readInteger(as: UInt8.self) else {
            return nil
        }
        return self.readUTF16String(characterCount: Int(characterCount))
    }

    fileprivate mutating func readUSVarchar() -> String? {
        guard let characterCount = self.readInteger(endianness: .little, as: UInt16.self) else {
            return nil
        }
        return self.readUTF16String(characterCount: Int(characterCount))
    }

    fileprivate mutating func readBVarbyte() throws -> [UInt8] {
        guard let length = self.readInteger(as: UInt8.self) else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: [UInt8].self)
        }
        return try self.readRequiredBytes(length: Int(length))
    }

    fileprivate mutating func readLVarbyte() throws -> [UInt8] {
        guard let length = self.readInteger(endianness: .little, as: UInt32.self) else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: [UInt8].self)
        }
        return try self.readRequiredBytes(length: Int(length))
    }

    fileprivate mutating func readRequiredInteger<T: FixedWidthInteger>(
        endianness: Endianness = .big,
        as type: T.Type = T.self
    ) throws -> T {
        guard let value = self.readInteger(endianness: endianness, as: type) else {
            throw TDSPartialDecodingError.fieldNotDecodable(type: type)
        }
        return value
    }

    fileprivate mutating func readRequiredBytes(length: Int) throws -> [UInt8] {
        guard let bytes = self.readBytes(length: length) else {
            throw TDSPartialDecodingError.expectedAtLeastNRemainingBytes(length, actual: self.readableBytes)
        }
        return bytes
    }
}
