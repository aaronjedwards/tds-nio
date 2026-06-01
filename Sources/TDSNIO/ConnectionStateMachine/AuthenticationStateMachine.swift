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

struct AuthenticationStateMachine {
    enum Action {
        case wait
        case sendPreloginRequest
        case startTLS(removeAfterLogin: Bool)
        case sendLoginRequest
        case fireAuthenticationChallenge(TDSAuthenticationChallenge)
        case authenticated(TDSBackendMessage.LoginAck, removeTLS: Bool)
        case failAuthentication(TDSSQLError)
    }

    private enum State {
        case initialized
        case preloginSent
        case tlsNegotiationRequired(removeAfterLogin: Bool)
        case login7Sent(removeTLSAfterLogin: Bool, loginAck: TDSBackendMessage.LoginAck?)
        case sspiChallengeContinuation(removeTLSAfterLogin: Bool, loginAck: TDSBackendMessage.LoginAck?)
        case fedAuthTokenContinuation(removeTLSAfterLogin: Bool, loginAck: TDSBackendMessage.LoginAck?)
        case authenticated
        case failed
    }

    private var state: State = .initialized
    private var loginError: TDSSQLError?

    mutating func connected() -> Action {
        guard case .initialized = self.state else {
            return .wait
        }
        self.state = .preloginSent
        return .sendPreloginRequest
    }

    mutating func preloginReceived(
        _ response: TDSBackendMessage.PreloginResponse,
        clientEncryption: TDSFrontendMessageEncoder.PreloginEncryption
    ) -> Action {
        guard case .preloginSent = self.state else {
            return .wait
        }

        if Self.requiresTLS(
            serverEncryption: response.encryption,
            clientEncryption: clientEncryption
        ) {
            let removeAfterLogin = Self.isLoginOnlyTLS(
                serverEncryption: response.encryption,
                clientEncryption: clientEncryption
            )
            self.state = .tlsNegotiationRequired(removeAfterLogin: removeAfterLogin)
            return .startTLS(removeAfterLogin: removeAfterLogin)
        }

        self.state = .login7Sent(removeTLSAfterLogin: false, loginAck: nil)
        return .sendLoginRequest
    }

    mutating func tlsEstablished() -> Action {
        guard case .tlsNegotiationRequired(let removeAfterLogin) = self.state else {
            return .wait
        }
        self.state = .login7Sent(removeTLSAfterLogin: removeAfterLogin, loginAck: nil)
        return .sendLoginRequest
    }

    mutating func loginAckReceived(_ loginAck: TDSBackendMessage.LoginAck) -> Action {
        guard loginAck.negotiatedProtocolVersion != nil else {
            self.loginError = .connectionError(underlying: UnsupportedLoginAckTDSVersion(loginAck.tdsVersion))
            return .wait
        }
        guard loginAck.hasSupportedInterface else {
            self.loginError = .connectionError(underlying: UnsupportedLoginAckInterface(loginAck.interface))
            return .wait
        }

        switch self.state {
        case .login7Sent(let removeTLSAfterLogin, _):
            self.state = .login7Sent(
                removeTLSAfterLogin: removeTLSAfterLogin,
                loginAck: loginAck
            )
        case .sspiChallengeContinuation(let removeTLSAfterLogin, _):
            self.state = .sspiChallengeContinuation(
                removeTLSAfterLogin: removeTLSAfterLogin,
                loginAck: loginAck
            )
        case .fedAuthTokenContinuation(let removeTLSAfterLogin, _):
            self.state = .fedAuthTokenContinuation(
                removeTLSAfterLogin: removeTLSAfterLogin,
                loginAck: loginAck
            )
        case .initialized, .preloginSent, .tlsNegotiationRequired, .authenticated, .failed:
            break
        }
        return .wait
    }

    mutating func sspiReceived(_ bytes: [UInt8]) -> Action {
        switch self.state {
        case .login7Sent(let removeTLSAfterLogin, let loginAck),
            .sspiChallengeContinuation(let removeTLSAfterLogin, let loginAck):
            self.state = .sspiChallengeContinuation(
                removeTLSAfterLogin: removeTLSAfterLogin,
                loginAck: loginAck
            )
            return .fireAuthenticationChallenge(.sspi(bytes))
        default:
            return .wait
        }
    }

    mutating func fedAuthInfoReceived(
        _ fedAuthInfo: TDSBackendMessage.FedAuthInfo
    ) -> Action {
        switch self.state {
        case .login7Sent(let removeTLSAfterLogin, let loginAck),
            .sspiChallengeContinuation(let removeTLSAfterLogin, let loginAck),
            .fedAuthTokenContinuation(let removeTLSAfterLogin, let loginAck):
            self.state = .fedAuthTokenContinuation(
                removeTLSAfterLogin: removeTLSAfterLogin,
                loginAck: loginAck
            )
            return .fireAuthenticationChallenge(.federatedInfo(.init(fedAuthInfo)))
        default:
            return .wait
        }
    }

    mutating func authenticationTokenSent() -> Action {
        switch self.state {
        case .sspiChallengeContinuation(let removeTLSAfterLogin, let loginAck),
            .fedAuthTokenContinuation(let removeTLSAfterLogin, let loginAck):
            self.state = .login7Sent(
                removeTLSAfterLogin: removeTLSAfterLogin,
                loginAck: loginAck
            )
        case .initialized, .preloginSent, .tlsNegotiationRequired, .login7Sent, .authenticated, .failed:
            break
        }
        return .wait
    }

    mutating func featureExtAckReceived(
        _ featureExtAck: TDSBackendMessage.FeatureExtAck,
        requestedFeatureIDs: Set<UInt8> = Self.defaultFeatureExtAckFeatureIDs
    ) -> Action {
        guard Self.isAuthenticating(self.state) else {
            return .wait
        }
        let requiresFedAuthAck = requestedFeatureIDs.contains(Self.fedAuthFeatureID)
        var receivedFedAuthAck = false
        var receivedRequestedFeatureAck = false
        for option in featureExtAck.options {
            guard requestedFeatureIDs.contains(option.featureID) else {
                if option.featureID == Self.fedAuthFeatureID {
                    self.loginError = .connectionError(underlying: UnexpectedFedAuthAck())
                    return .wait
                }
                continue
            }
            receivedRequestedFeatureAck = true
            if option.featureID == Self.fedAuthFeatureID {
                receivedFedAuthAck = true
                guard option.data.isEmpty else {
                    self.loginError = .connectionError(underlying: UnexpectedFedAuthAckData())
                    return .wait
                }
            }
        }
        if requiresFedAuthAck && !receivedFedAuthAck {
            self.loginError = .connectionError(underlying: MissingFedAuthAck())
            return .wait
        }
        if !receivedRequestedFeatureAck {
            self.loginError = .connectionError(underlying: UnknownFeatureExtAck())
            return .wait
        }
        return .wait
    }

    mutating func backendErrorReceived(_ error: TDSBackendMessage.InfoError) -> Action {
        guard Self.isAuthenticating(self.state) else {
            return self.failAuthentication(.server(error))
        }
        self.loginError = .server(error)
        return .wait
    }

    mutating func doneReceived() -> Action {
        self.messageComplete()
    }

    mutating func messageComplete() -> Action {
        switch self.state {
        case .login7Sent(_, nil):
            if let loginError {
                return self.failAuthentication(loginError)
            }
            return self.failAuthentication(
                .connectionError(underlying: MissingLoginAck())
            )
        case .login7Sent(let removeTLSAfterLogin, .some(let loginAck)):
            self.state = .authenticated
            return .authenticated(loginAck, removeTLS: removeTLSAfterLogin)
        case .sspiChallengeContinuation(let removeTLSAfterLogin, let loginAck),
            .fedAuthTokenContinuation(let removeTLSAfterLogin, let loginAck):
            guard let loginAck else {
                return .wait
            }
            self.state = .authenticated
            return .authenticated(loginAck, removeTLS: removeTLSAfterLogin)
        case .initialized, .preloginSent, .tlsNegotiationRequired, .authenticated, .failed:
            return .wait
        }
    }

    mutating func failAuthentication(_ error: TDSSQLError) -> Action {
        self.state = .failed
        return .failAuthentication(error)
    }

    private static func requiresTLS(
        serverEncryption: TDSFrontendMessageEncoder.PreloginEncryption?,
        clientEncryption: TDSFrontendMessageEncoder.PreloginEncryption
    ) -> Bool {
        switch serverEncryption {
        case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
            return true
        case .encryptOff, .encryptNotSup, .encryptClientCertOff, nil:
            switch clientEncryption {
            case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
                return true
            case .encryptOff, .encryptNotSup, .encryptClientCertOff:
                return false
            }
        }
    }

    private static func isLoginOnlyTLS(
        serverEncryption: TDSFrontendMessageEncoder.PreloginEncryption?,
        clientEncryption: TDSFrontendMessageEncoder.PreloginEncryption
    ) -> Bool {
        switch serverEncryption {
        case .encryptOff, .encryptNotSup, .encryptClientCertOff, nil:
            switch clientEncryption {
            case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
                return true
            case .encryptOff, .encryptNotSup, .encryptClientCertOff:
                return false
            }
        case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
            return false
        }
    }

    private static func isAuthenticating(_ state: State) -> Bool {
        switch state {
        case .login7Sent, .sspiChallengeContinuation, .fedAuthTokenContinuation:
            return true
        case .initialized, .preloginSent, .tlsNegotiationRequired, .authenticated, .failed:
            return false
        }
    }

    private static let fedAuthFeatureID: UInt8 = 0x02
    private static let defaultFeatureExtAckFeatureIDs: Set<UInt8> = [
        Capabilities.FeatureID.utf8Support.rawValue
    ]
}

private struct UnsupportedLoginAckTDSVersion: Error, CustomStringConvertible {
    var rawValue: UInt32

    init(_ rawValue: UInt32) {
        self.rawValue = rawValue
    }

    var description: String {
        "Server responded with unknown TDS version: \(self.rawValue)."
    }
}

private struct UnsupportedLoginAckInterface: Error, CustomStringConvertible {
    var rawValue: UInt8

    init(_ rawValue: UInt8) {
        self.rawValue = rawValue
    }

    var description: String {
        "Server responded with unsupported interface: \(self.rawValue)."
    }
}

private struct MissingLoginAck: Error, CustomStringConvertible {
    var description: String {
        "Login completed without LOGINACK."
    }
}

private struct UnexpectedFedAuthAck: Error, CustomStringConvertible {
    var description: String {
        "Server acknowledged federated authentication that was not requested."
    }
}

private struct MissingFedAuthAck: Error, CustomStringConvertible {
    var description: String {
        "Server did not acknowledge requested federated authentication."
    }
}

private struct UnexpectedFedAuthAckData: Error, CustomStringConvertible {
    var description: String {
        "Server included unexpected data in the federated authentication acknowledgement."
    }
}

private struct UnknownFeatureExtAck: Error, CustomStringConvertible {
    var description: String {
        "Server acknowledged an unknown feature extension."
    }
}
