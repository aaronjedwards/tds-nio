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

import NIOCore

/// Defining the capabilities (negotiated at connect time) that both
/// the database server and the client are capable of.
struct Capabilities: Sendable, Hashable {
    enum FeatureID: UInt8, Sendable, Hashable {
        case dataClassification = 0x09
        case utf8Support = 0x0A
        case jsonSupport = 0x0D
    }

    var requestedProtocolVersion: TDSProtocolVersion
    var negotiatedProtocolVersion: TDSProtocolVersion?
    var acknowledgedFeatureExtensions: [UInt8: [UInt8]]
    var dataClassificationVersion: UInt8?
    var supportsUTF8 = false
    var supportsJSON = false

    init(requestedProtocolVersion: TDSProtocolVersion = .v7_4) {
        self.requestedProtocolVersion = requestedProtocolVersion
        self.acknowledgedFeatureExtensions = [:]
    }

    mutating func adjustForLoginAck(_ loginAck: TDSBackendMessage.LoginAck) {
        self.negotiatedProtocolVersion = loginAck.negotiatedProtocolVersion
    }

    mutating func adjustForFeatureExtAck(_ featureExtAck: TDSBackendMessage.FeatureExtAck) {
        for option in featureExtAck.options {
            self.acknowledgedFeatureExtensions[option.featureID] = option.data
            switch FeatureID(rawValue: option.featureID) {
            case .dataClassification:
                self.dataClassificationVersion = option.data.first ?? 1
            case .utf8Support:
                self.supportsUTF8 = option.data.first.map { $0 != 0 } ?? true
            case .jsonSupport:
                self.supportsJSON = option.data.first.map { $0 != 0 } ?? true
            case .none:
                break
            }
        }
    }

    func wasAcknowledged(_ feature: FeatureID) -> Bool {
        self.acknowledgedFeatureExtensions[feature.rawValue] != nil
    }
}
