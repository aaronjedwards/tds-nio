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
