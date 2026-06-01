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

enum TDSSQLEvent {
    /// The event that is used to inform upstream handlers that ``TDSChannelHandler`` has
    /// established a connection successfully.
    case startupDone(
        version: TDSProtocolVersion,
        sessionID: Int,
        serialNumber: Int
    )
    /// The event that is used to inform upstream handlers that ``TDSChannelHandler`` is
    /// currently idle.
    case readyForQuery
    case routing(TDSBackendMessage.EnvChange.Routing)
    case packetSizeChanged(Int)
    case resetConnection
    case resetConnectionOnNextRequest
    /// The event that is used to inform state about an ongoing TLS renegotiation.
    case renegotiateTLS
}

final class TDSEventsHandler: ChannelInboundHandler {
    typealias InboundIn = Never

    typealias StartupContext = (
        version: TDSProtocolVersion, sessionID: Int, serialNumber: Int,
        routing: TDSBackendMessage.EnvChange.Routing?
    )

    let logger: Logger
    var startupDoneFuture: EventLoopFuture<StartupContext>! {
        self.startupDonePromise!.futureResult
    }

    private enum State {
        case initialized
        case connected
        case readyForStartup
        case authenticated
        case failed
    }

    private var startupDonePromise: EventLoopPromise<StartupContext>!
    private var state: State = .initialized
    private var routing: TDSBackendMessage.EnvChange.Routing?

    init(logger: Logger) {
        self.logger = logger
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case TDSSQLEvent.routing(let routing):
            self.routing = routing
        case TDSSQLEvent.startupDone(
            let version, let sessionID, let serialNumber
        ):
            guard case .connected = self.state else {
                preconditionFailure()
            }
            self.state = .readyForStartup
            self.startupDonePromise.succeed((version, sessionID, serialNumber, self.routing))
        case TDSSQLEvent.readyForQuery:
            switch self.state {
            case .initialized, .connected:
                preconditionFailure(
                    "Expected to get a `readyForStartup` before we get a `readyForQuery` event"
                )
            case .readyForStartup:
                // for the first time, we are ready to query, this means
                // startup/auth was successful
                self.state = .authenticated
            case .authenticated:
                break
            case .failed:
                break
            }
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.startupDonePromise = context.eventLoop.makePromise()

        if context.channel.isActive, case .initialized = self.state {
            self.state = .connected
        }
    }

    func channelActive(context: ChannelHandlerContext) {
        if case .initialized = self.state {
            self.state = .connected
        }
        context.fireChannelActive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        switch self.state {
        case .initialized:
            preconditionFailure("Unexpected message for state")
        case .connected:
            self.startupDonePromise.fail(error)
            self.state = .failed
        case .readyForStartup:
            self.startupDonePromise.fail(error)
            self.state = .failed
        case .authenticated:
            break
        case .failed:
            break
        }

        context.fireErrorCaught(error)
    }

    func channelInactive(context: ChannelHandlerContext) {
        switch self.state {
        case .initialized, .connected, .readyForStartup:
            self.startupDonePromise.fail(ChannelError.ioOnClosedChannel)
            self.state = .failed
        case .authenticated, .failed:
            break
        }
        context.fireChannelInactive()
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        struct HandlerRemovedConnectionError: Error {}

        if case .initialized = self.state {
            self.startupDonePromise.fail(HandlerRemovedConnectionError())
        }
    }

}
