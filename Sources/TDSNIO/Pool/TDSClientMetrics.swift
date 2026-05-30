import Logging
import _ConnectionPoolModule

final class TDSClientMetrics: ConnectionPoolObservabilityDelegate {
    typealias ConnectionID = TDSConnection.ID

    let logger: Logger

    init(logger: Logger) {
        self.logger = logger
    }

    func startedConnecting(id: TDSConnection.ID) {
        self.logger.debug(
            "Creating new connection",
            metadata: [.connectionID: "\(id)"]
        )
    }

    func connectFailed(id: TDSConnection.ID, error: Error) {
        self.logger.debug(
            "Connection creation failed",
            metadata: [
                .connectionID: "\(id)",
                .error: "\(String(reflecting: error))",
            ]
        )
    }

    func connectSucceeded(id: TDSConnection.ID) {
        self.logger.debug(
            "Connection established",
            metadata: [.connectionID: "\(id)"]
        )
    }

    func connectionLeased(id: ConnectionID) {
        self.logger.debug(
            "Connection leased",
            metadata: [.connectionID: "\(id)"]
        )
    }

    func connectionReleased(id: ConnectionID) {
        self.logger.debug(
            "Connection released",
            metadata: [.connectionID: "\(id)"]
        )
    }

    func keepAliveTriggered(id: ConnectionID) {
        self.logger.debug(
            "Run keep-alive ping",
            metadata: [.connectionID: "\(id)"]
        )
    }

    func keepAliveSucceeded(id: ConnectionID) {}

    func keepAliveFailed(id: TDSConnection.ID, error: Error) {}

    func connectionClosing(id: ConnectionID) {
        self.logger.debug(
            "Close connection",
            metadata: [.connectionID: "\(id)"]
        )
    }

    func connectionClosed(id: ConnectionID, error: Error?) {
        self.logger.debug(
            "Connection closed",
            metadata: [.connectionID: "\(id)"]
        )
    }

    func requestQueueDepthChanged(_ newDepth: Int) {}

    func connectSucceeded(id: TDSConnection.ID, streamCapacity: UInt16) {}

    func connectionUtilizationChanged(
        id: TDSConnection.ID,
        streamsUsed: UInt16,
        streamCapacity: UInt16
    ) {}
}
