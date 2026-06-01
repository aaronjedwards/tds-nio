import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOSSL
import NIOTestUtils
import Testing

@testable import TDSNIO

extension TDSTests {
    @Test func stateMachineNegotiatesTLSBeforeLogin() throws {
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
            Issue.record("Expected TLS negotiation to start"); return
        }
        expectFalse(removeAfterLogin)

        guard case .sendLoginRequest = state.tlsEstablished() else {
            Issue.record("Expected LOGIN7 after TLS handshake"); return
        }
    }

    @Test func stateMachineMarksClientOnlyTLSForRemovalAfterLogin() throws {
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
            Issue.record("Expected login-only TLS negotiation to start"); return
        }
        expectTrue(removeAfterLogin)
        guard case .sendLoginRequest = state.tlsEstablished() else {
            Issue.record("Expected LOGIN7 after TLS handshake"); return
        }

        let ack = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: 0x7400_0004,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )
        guard case .wait = state.loginAckReceived(ack) else {
            Issue.record("Expected LOGINACK to be stored until DONE"); return
        }

        let done = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)
        guard case .authenticated(_, let removeTLS) = state.doneReceived(done) else {
            Issue.record("Expected authentication completion"); return
        }
        expectTrue(removeTLS)
    }

    @Test func stateMachineCompletesStartupAfterInitialSQLDone() throws {
        var state = ConnectionStateMachine(.loggedIn)
        let ack = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: 0x7400_0004,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )

        guard case .sendSQLBatch("set ansi_nulls on") = state.startInitialSQL(
            "set ansi_nulls on",
            loginAck: ack,
            removeTLS: false
        ) else {
            Issue.record("Expected initial SQL batch to be sent"); return
        }

        let done = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)
        guard case .startupComplete(let completedAck, let removeTLS) = state.doneReceived(done)
        else {
            Issue.record("Expected startup completion after initial SQL DONE"); return
        }
        expectEqual(completedAck.tdsVersion, ack.tdsVersion)
        expectFalse(removeTLS)
    }

    @Test func stateMachineFailsStartupWhenInitialSQLErrorDoneArrives() throws {
        var state = ConnectionStateMachine(.loggedIn)
        let ack = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: 0x7400_0004,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )
        _ = state.startInitialSQL("set language invalid", loginAck: ack, removeTLS: false)
        let error = TDSBackendMessage.InfoError(
            number: 50000,
            state: 1,
            severity: 16,
            message: "Initial SQL failed",
            serverName: "",
            procedureName: "",
            lineNumber: 1
        )

        guard case .wait = state.backendErrorReceived(error) else {
            Issue.record("Expected initial SQL error token to be recorded until DONE"); return
        }

        let done = TDSBackendMessage.Done(status: .error, currentCommand: 0, rowCount: 0)
        guard case .closeConnectionAndCleanup(let cleanup) = state.doneReceived(done) else {
            Issue.record("Expected startup to fail once initial SQL error DONE arrives"); return
        }
        expectEqual(cleanup.error.serverInfo?.message, "Initial SQL failed")
    }

    @Test func stateMachineIgnoresInitialSQLReturnStatusAndReturnValue() throws {
        var state = ConnectionStateMachine(.loggedIn)
        let ack = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: 0x7400_0004,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )
        _ = state.startInitialSQL("exec dbo.startup", loginAck: ack, removeTLS: false)

        guard case .wait = state.returnStatusReceived(7) else {
            Issue.record("Expected initial SQL RETURNSTATUS to be ignored"); return
        }
        guard
            case .wait = state.returnValueReceived(
                .init(
                    ordinal: 1,
                    name: "answer",
                    status: 1,
                    userType: 0,
                    flags: 0,
                    typeInfo: .init(
                        dataType: .intN,
                        length: 4,
                        collation: [],
                        precision: nil,
                        scale: nil,
                        tableName: nil,
                        udtInfo: nil,
                        xmlInfo: nil
                    ),
                    value: .int32(42)
                ))
        else {
            Issue.record("Expected initial SQL RETURNVALUE to be ignored"); return
        }

        let done = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)
        guard case .startupComplete = state.doneReceived(done) else {
            Issue.record("Expected startup to complete after ignored return tokens"); return
        }
    }

    @Test func stateMachineRejectsUnsupportedLoginAckTDSVersion() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let ack = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: 0x7100_0001,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )

        guard case .wait = state.loginAckReceived(ack) else {
            Issue.record("Expected unsupported TDS version to be recorded until login response completes"); return
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            Issue.record("Expected unsupported TDS version to fail authentication on message completion"); return
        }
        expectEqual(cleanup.error.code, .connectionError)
    }

    @Test func stateMachineRejectsUnsupportedLoginAckInterface() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let ack = TDSBackendMessage.LoginAck(
            interface: 2,
            tdsVersion: 0x7400_0004,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )

        guard case .wait = state.loginAckReceived(ack) else {
            Issue.record("Expected unsupported interface to be recorded until login response completes"); return
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            Issue.record("Expected unsupported interface to fail authentication on message completion"); return
        }
        expectEqual(cleanup.error.code, .connectionError)
    }

    @Test func stateMachineRejectsLoginDoneWithoutLoginAck() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let done = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)
        guard case .closeConnectionAndCleanup(let cleanup) = state.doneReceived(done) else {
            Issue.record("Expected missing LOGINACK to fail authentication"); return
        }
        expectEqual(cleanup.error.code, .connectionError)
    }

    @Test func stateMachineRejectsUnexpectedFedAuthFeatureAckDuringLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let featureExtAck = TDSBackendMessage.FeatureExtAck(
            options: [.init(featureID: 0x02, data: [])]
        )
        guard case .wait = state.featureExtAckReceived(featureExtAck) else {
            Issue.record("Expected unexpected FedAuth acknowledgement to be recorded until login response completes"); return
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            Issue.record("Expected unexpected FedAuth acknowledgement to fail authentication on message completion"); return
        }
        expectEqual(cleanup.error.code, .connectionError)
    }

    @Test func stateMachineRejectsUnexpectedFeatureAckDuringLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let featureExtAck = TDSBackendMessage.FeatureExtAck(
            options: [.init(featureID: 0x55, data: [])]
        )
        guard case .wait = state.featureExtAckReceived(featureExtAck) else {
            Issue.record("Expected unrequested feature acknowledgement to be recorded until login response completes"); return
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            Issue.record("Expected unrequested feature acknowledgement to fail authentication on message completion"); return
        }
        expectEqual(cleanup.error.code, .connectionError)
    }

    @Test func stateMachineAcceptsAdvertisedFeatureAckDuringLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let featureExtAck = TDSBackendMessage.FeatureExtAck(
            options: [
                .init(featureID: Capabilities.FeatureID.utf8Support.rawValue, data: [0x01]),
                .init(featureID: 0x55, data: [0xAA]),
            ]
        )
        guard case .wait = state.featureExtAckReceived(featureExtAck) else {
            Issue.record("Expected UTF8 acknowledgement and ignored extra feature acknowledgements during authentication"); return
        }
    }

    @Test func stateMachineAcceptsFedAuthFeatureAckWhenRequestedDuringLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: true,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "user@example.test",
            password: "Secret123!",
            tls: .disable,
            authentication: .federatedAuthentication()
        )
        let featureExtAck = TDSBackendMessage.FeatureExtAck(
            options: [.init(featureID: 0x02, data: [])]
        )
        guard
            case .wait = state.featureExtAckReceived(
                featureExtAck,
                requestedFeatureIDs: configuration.requestedFeatureExtAckFeatureIDs
            )
        else {
            Issue.record("Expected requested FedAuth acknowledgement to be accepted during authentication"); return
        }
    }

    @Test func stateMachineRejectsMissingFedAuthFeatureAckWhenRequestedDuringLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: true,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "user@example.test",
            password: "Secret123!",
            tls: .disable,
            authentication: .federatedAuthentication()
        )
        let featureExtAck = TDSBackendMessage.FeatureExtAck(
            options: [.init(featureID: Capabilities.FeatureID.utf8Support.rawValue, data: [0x01])]
        )
        guard
            case .wait = state.featureExtAckReceived(
                featureExtAck,
                requestedFeatureIDs: configuration.requestedFeatureExtAckFeatureIDs
            )
        else {
            Issue.record("Expected missing FedAuth acknowledgement to be recorded until login response completes"); return
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            Issue.record("Expected missing FedAuth acknowledgement to fail authentication on message completion"); return
        }
        expectEqual(cleanup.error.code, .connectionError)
    }

    @Test func stateMachineRejectsFedAuthFeatureAckWithDataDuringLogin() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: true,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let configuration = TDSConnection.Configuration(
            host: "sql.example.test",
            username: "user@example.test",
            password: "Secret123!",
            tls: .disable,
            authentication: .federatedAuthentication()
        )
        let featureExtAck = TDSBackendMessage.FeatureExtAck(
            options: [.init(featureID: 0x02, data: [0x01])]
        )
        guard
            case .wait = state.featureExtAckReceived(
                featureExtAck,
                requestedFeatureIDs: configuration.requestedFeatureExtAckFeatureIDs
            )
        else {
            Issue.record("Expected invalid FedAuth acknowledgement to be recorded until login response completes"); return
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            Issue.record("Expected invalid FedAuth acknowledgement to fail authentication on message completion"); return
        }
        expectEqual(cleanup.error.code, .connectionError)
    }

    @Test func stateMachineWaitsForFedAuthTokenWhenFedAuthInfoArrivesWithLoginError() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: true,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        let fedAuthInfo = TDSBackendMessage.FedAuthInfo(options: [
            .init(id: 0x01, data: Array("https://sts.example.test".utf16).flatMap {
                [$0.littleEndian & 0x00FF, $0.littleEndian >> 8].map(UInt8.init)
            }),
            .init(id: 0x02, data: Array("MSSQLSvc/sql.example.test:1433".utf16).flatMap {
                [$0.littleEndian & 0x00FF, $0.littleEndian >> 8].map(UInt8.init)
            }),
        ])
        guard case .fireAuthenticationChallenge = state.fedAuthInfoReceived(fedAuthInfo) else {
            Issue.record("Expected FEDAUTHINFO to request a token"); return
        }

        let loginError = TDSBackendMessage.InfoError(
            number: 18456,
            state: 1,
            severity: 16,
            message: "Login failed while requesting token.",
            serverName: "",
            procedureName: "",
            lineNumber: 1
        )
        guard case .wait = state.backendErrorReceived(loginError) else {
            Issue.record("Expected login error to be recorded during FedAuth info response"); return
        }
        guard case .wait = state.backendMessageComplete() else {
            Issue.record("Expected FEDAUTHINFO to take precedence over login error until token response"); return
        }
        guard case .wait = state.authenticationTokenSent() else {
            Issue.record("Expected sent FedAuth token to resume normal login response handling"); return
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            Issue.record("Expected recorded login error to fail after FedAuth token response completes"); return
        }
        expectEqual(cleanup.error.code, .server)
    }

    @Test func stateMachineWaitsForSSPIResponseThenFailsPostTokenLoginError() throws {
        var state = ConnectionStateMachine(.sentPrelogin)
        let response = TDSBackendMessage.PreloginResponse(
            version: nil,
            encryption: .encryptOff,
            mars: nil,
            fedAuthRequired: nil,
            nonce: nil
        )

        guard
            case .sendLoginRequest = state.preloginReceived(
                response,
                clientEncryption: .encryptOff
            )
        else {
            Issue.record("Expected LOGIN7 without TLS"); return
        }

        guard case .fireAuthenticationChallenge = state.sspiReceived([0xAA, 0xBB]) else {
            Issue.record("Expected SSPI token to request a continuation token"); return
        }
        let loginError = TDSBackendMessage.InfoError(
            number: 18456,
            state: 1,
            severity: 16,
            message: "Login failed while requesting SSPI token.",
            serverName: "",
            procedureName: "",
            lineNumber: 1
        )
        guard case .wait = state.backendErrorReceived(loginError) else {
            Issue.record("Expected login error to be recorded during SSPI challenge response"); return
        }
        guard case .wait = state.backendMessageComplete() else {
            Issue.record("Expected SSPI challenge to take precedence over login error until token response"); return
        }
        guard case .wait = state.authenticationTokenSent() else {
            Issue.record("Expected sent SSPI token to resume normal login response handling"); return
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            Issue.record("Expected recorded login error to fail after SSPI token response completes"); return
        }
        expectEqual(cleanup.error.code, .server)
    }

    @Test func stateMachineForwardsInactiveAfterClientClose() throws {
        var state = ConnectionStateMachine(.loggedIn)

        guard case .closeConnectionAndCleanup(let cleanup) = state.close(nil) else {
            Issue.record("Expected client close to start connection cleanup"); return
        }
        guard case .close = cleanup.action else {
            Issue.record("Expected cleanup to close the channel"); return
        }
        expectTrue(cleanup.tasks.isEmpty)
        expectNil(cleanup.rowStreamError)

        guard case .fireChannelInactive = state.closed() else {
            Issue.record("Expected channelInactive to be forwarded after close completes"); return
        }
    }

    @Test func stateMachineQueuesTasksDuringStartup() throws {
        let channel = EmbeddedChannel()
        defer {
            expectNoThrow(try channel.finish(acceptAlreadyClosed: true))
        }
        var state = ConnectionStateMachine(.sentPrelogin)
        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)

        guard
            case .wait = state.enqueue(
                task: .sqlBatch("SELECT 1", promise),
                promise: nil
            )
        else {
            Issue.record("Expected startup task to be queued"); return
        }

        guard case .closeConnectionAndCleanup(let cleanup) = state.close(nil) else {
            Issue.record("Expected queued startup task to be failed during close"); return
        }
        expectEqual(cleanup.tasks.count, 1)
        for task in cleanup.tasks {
            task.fail(cleanup.error)
        }
    }

    @Test func stateMachineWaitsForAttentionDoneBeforeLeavingSentAttention() throws {
        let channel = EmbeddedChannel()
        defer {
            expectNoThrow(try channel.finish(acceptAlreadyClosed: true))
        }
        var state = ConnectionStateMachine(.sentAttention)
        let normalDone = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)

        guard case .wait = state.doneReceived(normalDone, tokenKind: .done) else {
            Issue.record("Expected non-attention DONE to be drained"); return
        }

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        guard
            case .wait = state.enqueue(
                task: .sqlBatch("SELECT 1", promise),
                promise: nil
            )
        else {
            Issue.record("Expected state to keep draining until the attention acknowledgement"); return
        }
        expectThrowsError(try promise.futureResult.wait())

        let attentionDone = TDSBackendMessage.Done(
            status: .attention,
            currentCommand: 0,
            rowCount: 0
        )
        guard case .wait = state.doneReceived(attentionDone, tokenKind: .done) else {
            Issue.record("Expected attention acknowledgement to complete cancellation"); return
        }

        let nextPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        guard
            case .sendSQLBatch("SELECT 2") = state.enqueue(
                task: .sqlBatch("SELECT 2", nextPromise),
                promise: nil
            )
        else {
            Issue.record("Expected connection to accept requests after attention acknowledgement"); return
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.close(nil) else {
            Issue.record("Expected close to clean up the started query"); return
        }
        for task in cleanup.tasks {
            task.fail(cleanup.error)
        }
    }

    @Test func stateMachineRejectsResultMetadataWithoutActiveRequestDuringLogin() throws {
        var state = ConnectionStateMachine(.sentLoginWithCompleteAuth)

        guard case .closeConnectionAndCleanup(let cleanup) = state.colMetadataReceived(Self.intMetadata())
        else {
            Issue.record("Expected COLMETADATA without an active request to close the connection"); return
        }
        expectEqual(cleanup.error.code, .connectionError)
        expectTrue(String(describing: cleanup.error.underlying).contains("COLMETADATA"))
    }

    @Test func stateMachineRejectsRowsWithoutActiveRequestAfterLogin() throws {
        var state = ConnectionStateMachine(.loggedIn)

        guard case .closeConnectionAndCleanup(let cleanup) = state.rowReceived(.init(values: [.int32(1)]))
        else {
            Issue.record("Expected ROW without an active request to close the connection"); return
        }
        expectEqual(cleanup.error.code, .connectionError)
        expectTrue(String(describing: cleanup.error.underlying).contains("ROW"))
    }

    private static func intMetadata() -> TDSBackendMessage.ColMetadata {
        .init(
            columnCount: 1,
            columns: [
                .init(
                    userType: 0,
                    flags: 0,
                    typeInfo: .init(
                        dataType: .intN,
                        length: 4,
                        collation: [],
                        precision: nil,
                        scale: nil,
                        tableName: nil,
                        udtInfo: nil,
                        xmlInfo: nil
                    ),
                    name: "id"
                )
            ]
        )
    }
}
