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

/// A client-to-server authentication continuation message.
///
/// SQL password authentication completes in the LOGIN7 packet. SSPI and federated authentication can
/// require extra client packets during the login exchange, after the server has returned an SSPI or
/// FEDAUTHINFO token.
public enum TDSAuthenticationToken: Sendable, Hashable {
    /// A SPNEGO/SSPI continuation token.
    case sspi([UInt8])

    /// A federated authentication token, optionally followed by the 32-byte server nonce from PRELOGIN.
    case federated(token: [UInt8], nonce: [UInt8]? = nil)
}
