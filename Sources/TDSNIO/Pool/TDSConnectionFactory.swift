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

import Logging
import NIOCore

final class TDSConnectionFactory: Sendable {
    let configuration: TDSConnection.Configuration
    let eventLoopGroup: any EventLoopGroup
    let logger: Logger

    init(
        configuration: TDSConnection.Configuration,
        eventLoopGroup: any EventLoopGroup,
        logger: Logger
    ) {
        self.configuration = configuration
        self.eventLoopGroup = eventLoopGroup
        self.logger = logger
    }

    func makeConnection(_ connectionID: TDSConnection.ID) async throws -> TDSConnection {
        var connectionLogger = self.logger
        connectionLogger[tdsMetadataKey: .connectionID] = "\(connectionID)"

        return try await TDSConnection.connect(
            on: self.eventLoopGroup.any(),
            configuration: self.configuration,
            id: connectionID,
            logger: connectionLogger
        )
    }
}
