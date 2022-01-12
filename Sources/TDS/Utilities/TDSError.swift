import Foundation

public enum TDSError: Error, LocalizedError, CustomStringConvertible {
    case protocolError(String)
    case connectionClosed
    case invalidCredentials
    case errorToken(String)
    
    /// See `LocalizedError`.
    public var errorDescription: String? {
        return self.description
    }
    
    /// See `CustomStringConvertible`.
    public var description: String {
        let description: String
        switch self {
        case .protocolError(let message):
            description = "protocol error: \(message)"
        case .connectionClosed:
            description = "connection closed"
        case .invalidCredentials:
            description = "Invalid login credentials"
        case .errorToken(let message):
            description = "Error token msg: \(message)"
        }
        return "TDS error: \(description)"
    }
}
