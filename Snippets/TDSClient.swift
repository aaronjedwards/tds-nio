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

// snippet.makeClient
let client = TDSClient(configuration: configuration)
// snippet.end

// snippet.run
await withTaskGroup(of: Void.self) { taskGroup in
    taskGroup.addTask {
        await client.run()
    }

    // You can use the client while the `client.run()` method is not cancelled.

    // To shutdown the client, cancel its run method by cancelling the taskGroup.
    taskGroup.cancelAll()
}
// snippet.end
