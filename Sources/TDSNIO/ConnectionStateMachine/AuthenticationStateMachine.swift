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

    mutating func doneReceived() -> Action {
        switch self.state {
        case .login7Sent(let removeTLSAfterLogin, let loginAck),
            .sspiChallengeContinuation(let removeTLSAfterLogin, let loginAck),
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
}
