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

import TDSNIO

// snippet.configuration
let configuration = TDSConnection.Configuration(
    host: "localhost",
    username: "sa",
    password: "yourStrong(!)Password",
    database: "master",
    tls: .disable
)
// snippet.end

// snippet.connect
let connection = try await TDSConnection.connect(configuration: configuration, id: 1)
// snippet.end

// snippet.use
_ = try await connection.execute("SELECT 'Hello, World!'")
// snippet.end

// snippet.close
let closeConnection: () async throws -> Void = connection.close
try await closeConnection()
// snippet.end
