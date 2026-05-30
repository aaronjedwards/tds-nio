import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOSSL
import NIOTestUtils
import XCTest

@testable import TDSNIO

extension TDSTests {
    func testStateMachineNegotiatesTLSBeforeLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOn,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard
            case .startTLS(let removeAfterLogin) = state.preloginReceived(
                response,
                clientEncryption: .encryptOn
            )
        else {
            return XCTFail("Expected TLS negotiation to start")
        }
        XCTAssertFalse(removeAfterLogin)

        guard case .sendLoginRequest = state.tlsEstablished() else {
            return XCTFail("Expected LOGIN7 after TLS handshake")
        }
    }

    func testStateMachineMarksClientOnlyTLSForRemovalAfterLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard
            case .startTLS(let removeAfterLogin) = state.preloginReceived(
                response,
                clientEncryption: .encryptOn
            )
        else {
            return XCTFail("Expected login-only TLS negotiation to start")
        }
        XCTAssertTrue(removeAfterLogin)
        guard case .sendLoginRequest = state.tlsEstablished() else {
            return XCTFail("Expected LOGIN7 after TLS handshake")
        }

        let ack = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: 0x7400_0004,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )
        guard case .wait = state.loginAckReceived(ack) else {
            return XCTFail("Expected LOGINACK to be stored until DONE")
        }

        let done = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)
        guard case .authenticated(_, let removeTLS) = state.doneReceived(done) else {
            return XCTFail("Expected authentication completion")
        }
        XCTAssertTrue(removeTLS)
    }

    func testStateMachineForwardsInactiveAfterClientClose() throws {
        var state = ConnectionStateMachine(.loggedIn)

        guard case .closeConnectionAndCleanup(let cleanup) = state.close(nil) else {
            return XCTFail("Expected client close to start connection cleanup")
        }
        guard case .close = cleanup.action else {
            return XCTFail("Expected cleanup to close the channel")
        }
        XCTAssertTrue(cleanup.tasks.isEmpty)
        XCTAssertNil(cleanup.rowStreamError)

        guard case .fireChannelInactive = state.closed() else {
            return XCTFail("Expected channelInactive to be forwarded after close completes")
        }
    }

    func testStateMachineQueuesTasksDuringStartup() throws {
        let channel = EmbeddedChannel()
        defer {
            XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
        }
        var state = ConnectionStateMachine(.sentPrelogin)
        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)

        guard
            case .wait = state.enqueue(
                task: .sqlBatch("SELECT 1", promise),
                promise: nil
            )
        else {
            return XCTFail("Expected startup task to be queued")
        }

        guard case .closeConnectionAndCleanup(let cleanup) = state.close(nil) else {
            return XCTFail("Expected queued startup task to be failed during close")
        }
        XCTAssertEqual(cleanup.tasks.count, 1)
        for task in cleanup.tasks {
            task.fail(cleanup.error)
        }
    }
}
