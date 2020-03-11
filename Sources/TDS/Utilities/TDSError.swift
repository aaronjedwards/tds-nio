import Foundation

public enum TDSError: Error, LocalizedError, CustomStringConvertible {
    case protocolError(String)
    case connectionClosed
    case invalidCredentials
    case invalidConnectionOptionValueLength(fieldName: String, limit: UInt16)
    
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
        case .invalidConnectionOptionValueLength(let fieldName, let limit):
            description = "The value's length for field '\(fieldName)' exceeds it's limit of '\(limit)'"
        }
        return "TDS error: \(description)"
    }
}
