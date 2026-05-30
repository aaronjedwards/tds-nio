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

/// A server-to-client authentication challenge raised during the login exchange.
///
/// Consumers that implement SSPI/SPNEGO or federated authentication can listen for these user
/// events, generate the next authentication token, and send it back with ``TDSAuthenticationToken``.
public enum TDSAuthenticationChallenge: Sendable, Hashable {
    /// A SPNEGO/SSPI token returned by the server.
    case sspi([UInt8])

    /// Federated authentication metadata needed to acquire a token from an identity provider.
    case federatedInfo(FederatedInfo)

    public struct FederatedInfo: Sendable, Hashable {
        public struct Option: Sendable, Hashable {
            public var id: UInt8
            public var data: [UInt8]

            public init(id: UInt8, data: [UInt8]) {
                self.id = id
                self.data = data
            }
        }

        public var options: [Option]

        public var stsURL: String? {
            self.utf16String(for: 0x01)
        }

        public var spn: String? {
            self.utf16String(for: 0x02)
        }

        public init(options: [Option]) {
            self.options = options
        }

        init(_ fedAuthInfo: TDSBackendMessage.FedAuthInfo) {
            self.options = fedAuthInfo.options.map {
                Option(id: $0.id, data: $0.data)
            }
        }

        private func utf16String(for id: UInt8) -> String? {
            guard let data = self.options.first(where: { $0.id == id })?.data else {
                return nil
            }
            return String(bytes: data, encoding: .utf16LittleEndian)
        }
    }
}
