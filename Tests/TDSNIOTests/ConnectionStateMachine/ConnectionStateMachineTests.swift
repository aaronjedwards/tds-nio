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

    func testStateMachineCompletesStartupAfterInitialSQLDone() throws {
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
            return XCTFail("Expected initial SQL batch to be sent")
        }

        let done = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)
        guard case .startupComplete(let completedAck, let removeTLS) = state.doneReceived(done)
        else {
            return XCTFail("Expected startup completion after initial SQL DONE")
        }
        XCTAssertEqual(completedAck.tdsVersion, ack.tdsVersion)
        XCTAssertFalse(removeTLS)
    }

    func testStateMachineFailsStartupWhenInitialSQLErrorDoneArrives() throws {
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
            return XCTFail("Expected initial SQL error token to be recorded until DONE")
        }

        let done = TDSBackendMessage.Done(status: .error, currentCommand: 0, rowCount: 0)
        guard case .closeConnectionAndCleanup(let cleanup) = state.doneReceived(done) else {
            return XCTFail("Expected startup to fail once initial SQL error DONE arrives")
        }
        XCTAssertEqual(cleanup.error.serverInfo?.message, "Initial SQL failed")
    }

    func testStateMachineIgnoresInitialSQLReturnStatusAndReturnValue() throws {
        var state = ConnectionStateMachine(.loggedIn)
        let ack = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: 0x7400_0004,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )
        _ = state.startInitialSQL("exec dbo.startup", loginAck: ack, removeTLS: false)

        guard case .wait = state.returnStatusReceived(7) else {
            return XCTFail("Expected initial SQL RETURNSTATUS to be ignored")
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
            return XCTFail("Expected initial SQL RETURNVALUE to be ignored")
        }

        let done = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)
        guard case .startupComplete = state.doneReceived(done) else {
            return XCTFail("Expected startup to complete after ignored return tokens")
        }
    }

    func testStateMachineRejectsUnsupportedLoginAckTDSVersion() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
        }

        let ack = TDSBackendMessage.LoginAck(
            interface: 1,
            tdsVersion: 0x7100_0001,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )

        guard case .wait = state.loginAckReceived(ack) else {
            return XCTFail("Expected unsupported TDS version to be recorded until login response completes")
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            return XCTFail("Expected unsupported TDS version to fail authentication on message completion")
        }
        XCTAssertEqual(cleanup.error.code, .connectionError)
    }

    func testStateMachineRejectsUnsupportedLoginAckInterface() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
        }

        let ack = TDSBackendMessage.LoginAck(
            interface: 2,
            tdsVersion: 0x7400_0004,
            programName: "SQL",
            serverVersion: .init(major: 16, minor: 0, buildHigh: 0x10, buildLow: 0x6A)
        )

        guard case .wait = state.loginAckReceived(ack) else {
            return XCTFail("Expected unsupported interface to be recorded until login response completes")
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            return XCTFail("Expected unsupported interface to fail authentication on message completion")
        }
        XCTAssertEqual(cleanup.error.code, .connectionError)
    }

    func testStateMachineRejectsLoginDoneWithoutLoginAck() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
        }

        let done = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)
        guard case .closeConnectionAndCleanup(let cleanup) = state.doneReceived(done) else {
            return XCTFail("Expected missing LOGINACK to fail authentication")
        }
        XCTAssertEqual(cleanup.error.code, .connectionError)
    }

    func testStateMachineRejectsUnexpectedFedAuthFeatureAckDuringLogin() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
        }

        let featureExtAck = TDSBackendMessage.FeatureExtAck(
            options: [.init(featureID: 0x02, data: [])]
        )
        guard case .wait = state.featureExtAckReceived(featureExtAck) else {
            return XCTFail("Expected unexpected FedAuth acknowledgement to be recorded until login response completes")
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            return XCTFail("Expected unexpected FedAuth acknowledgement to fail authentication on message completion")
        }
        XCTAssertEqual(cleanup.error.code, .connectionError)
    }

    func testStateMachineRejectsUnexpectedFeatureAckDuringLogin() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
        }

        let featureExtAck = TDSBackendMessage.FeatureExtAck(
            options: [.init(featureID: 0x55, data: [])]
        )
        guard case .wait = state.featureExtAckReceived(featureExtAck) else {
            return XCTFail("Expected unrequested feature acknowledgement to be recorded until login response completes")
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            return XCTFail("Expected unrequested feature acknowledgement to fail authentication on message completion")
        }
        XCTAssertEqual(cleanup.error.code, .connectionError)
    }

    func testStateMachineAcceptsAdvertisedFeatureAckDuringLogin() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
        }

        let featureExtAck = TDSBackendMessage.FeatureExtAck(
            options: [
                .init(featureID: Capabilities.FeatureID.utf8Support.rawValue, data: [0x01]),
                .init(featureID: 0x55, data: [0xAA]),
            ]
        )
        guard case .wait = state.featureExtAckReceived(featureExtAck) else {
            return XCTFail("Expected UTF8 acknowledgement and ignored extra feature acknowledgements during authentication")
        }
    }

    func testStateMachineAcceptsFedAuthFeatureAckWhenRequestedDuringLogin() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
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
            return XCTFail("Expected requested FedAuth acknowledgement to be accepted during authentication")
        }
    }

    func testStateMachineRejectsMissingFedAuthFeatureAckWhenRequestedDuringLogin() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
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
            return XCTFail("Expected missing FedAuth acknowledgement to be recorded until login response completes")
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            return XCTFail("Expected missing FedAuth acknowledgement to fail authentication on message completion")
        }
        XCTAssertEqual(cleanup.error.code, .connectionError)
    }

    func testStateMachineRejectsFedAuthFeatureAckWithDataDuringLogin() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
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
            return XCTFail("Expected invalid FedAuth acknowledgement to be recorded until login response completes")
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            return XCTFail("Expected invalid FedAuth acknowledgement to fail authentication on message completion")
        }
        XCTAssertEqual(cleanup.error.code, .connectionError)
    }

    func testStateMachineWaitsForFedAuthTokenWhenFedAuthInfoArrivesWithLoginError() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
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
            return XCTFail("Expected FEDAUTHINFO to request a token")
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
            return XCTFail("Expected login error to be recorded during FedAuth info response")
        }
        guard case .wait = state.backendMessageComplete() else {
            return XCTFail("Expected FEDAUTHINFO to take precedence over login error until token response")
        }
        guard case .wait = state.authenticationTokenSent() else {
            return XCTFail("Expected sent FedAuth token to resume normal login response handling")
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            return XCTFail("Expected recorded login error to fail after FedAuth token response completes")
        }
        XCTAssertEqual(cleanup.error.code, .server)
    }

    func testStateMachineWaitsForSSPIResponseThenFailsPostTokenLoginError() throws {
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
            return XCTFail("Expected LOGIN7 without TLS")
        }

        guard case .fireAuthenticationChallenge = state.sspiReceived([0xAA, 0xBB]) else {
            return XCTFail("Expected SSPI token to request a continuation token")
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
            return XCTFail("Expected login error to be recorded during SSPI challenge response")
        }
        guard case .wait = state.backendMessageComplete() else {
            return XCTFail("Expected SSPI challenge to take precedence over login error until token response")
        }
        guard case .wait = state.authenticationTokenSent() else {
            return XCTFail("Expected sent SSPI token to resume normal login response handling")
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.backendMessageComplete() else {
            return XCTFail("Expected recorded login error to fail after SSPI token response completes")
        }
        XCTAssertEqual(cleanup.error.code, .server)
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

    func testStateMachineWaitsForAttentionDoneBeforeLeavingSentAttention() throws {
        let channel = EmbeddedChannel()
        defer {
            XCTAssertNoThrow(try channel.finish(acceptAlreadyClosed: true))
        }
        var state = ConnectionStateMachine(.sentAttention)
        let normalDone = TDSBackendMessage.Done(status: [], currentCommand: 0, rowCount: 0)

        guard case .wait = state.doneReceived(normalDone, tokenKind: .done) else {
            return XCTFail("Expected non-attention DONE to be drained")
        }

        let promise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        guard
            case .wait = state.enqueue(
                task: .sqlBatch("SELECT 1", promise),
                promise: nil
            )
        else {
            return XCTFail("Expected state to keep draining until the attention acknowledgement")
        }
        XCTAssertThrowsError(try promise.futureResult.wait())

        let attentionDone = TDSBackendMessage.Done(
            status: .attention,
            currentCommand: 0,
            rowCount: 0
        )
        guard case .wait = state.doneReceived(attentionDone, tokenKind: .done) else {
            return XCTFail("Expected attention acknowledgement to complete cancellation")
        }

        let nextPromise = channel.eventLoop.makePromise(of: TDSQueryResult.self)
        guard
            case .sendSQLBatch("SELECT 2") = state.enqueue(
                task: .sqlBatch("SELECT 2", nextPromise),
                promise: nil
            )
        else {
            return XCTFail("Expected connection to accept requests after attention acknowledgement")
        }
        guard case .closeConnectionAndCleanup(let cleanup) = state.close(nil) else {
            return XCTFail("Expected close to clean up the started query")
        }
        for task in cleanup.tasks {
            task.fail(cleanup.error)
        }
    }

    func testStateMachineRejectsResultMetadataWithoutActiveRequestDuringLogin() throws {
        var state = ConnectionStateMachine(.sentLoginWithCompleteAuth)

        guard case .closeConnectionAndCleanup(let cleanup) = state.colMetadataReceived(Self.intMetadata())
        else {
            return XCTFail("Expected COLMETADATA without an active request to close the connection")
        }
        XCTAssertEqual(cleanup.error.code, .connectionError)
        XCTAssertTrue(String(describing: cleanup.error.underlying).contains("COLMETADATA"))
    }

    func testStateMachineRejectsRowsWithoutActiveRequestAfterLogin() throws {
        var state = ConnectionStateMachine(.loggedIn)

        guard case .closeConnectionAndCleanup(let cleanup) = state.rowReceived(.init(values: [.int32(1)]))
        else {
            return XCTFail("Expected ROW without an active request to close the connection")
        }
        XCTAssertEqual(cleanup.error.code, .connectionError)
        XCTAssertTrue(String(describing: cleanup.error.underlying).contains("ROW"))
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
