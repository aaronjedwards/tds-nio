import NIOCore

/// An error that is thrown from the TDSClient.
///
/// - Warning: These errors should not be forwarded to the end user, as they may leak sensitive information.
public struct TDSSQLError: Sendable, Error {

    public struct Code: Sendable, Hashable, CustomStringConvertible {
        enum Base: Sendable, Hashable {
            case clientClosedConnection
            case connectionError
            case server
            case requestCancelled
        }

        internal var base: Base

        private init(_ base: Base) {
            self.base = base
        }

        public static let clientClosedConnection = Self(.clientClosedConnection)
        public static let connectionError = Self(.connectionError)
        public static let server = Self(.server)
        public static let requestCancelled = Self(.requestCancelled)

        public var description: String {
            switch self.base {
            case .clientClosedConnection:
                return "clientClosedConnection"
            case .connectionError:
                return "connectionError"
            case .server:
                return "server"
            case .requestCancelled:
                return "requestCancelled"
            }
        }
    }

    private var backing: Backing

    private mutating func copyBackingStorageIfNecessary() {
        if !isKnownUniquelyReferenced(&self.backing) {
            self.backing = self.backing.copy()
        }
    }

    /// The ``TDSSQLError/Code`` code.
    public internal(set) var code: Code {
        get { self.backing.code }
        set {
            self.copyBackingStorageIfNecessary()
            self.backing.code = newValue
        }
    }

    /// The info that was received from the server.
    public internal(set) var serverInfo: ServerInfo? {
        get { self.backing.serverInfo }
        set {
            self.copyBackingStorageIfNecessary()
            self.backing.serverInfo = newValue
        }
    }

    /// The underlying error.
    public internal(set) var underlying: Error? {
        get { self.backing.underlying }
        set {
            self.copyBackingStorageIfNecessary()
            self.backing.underlying = newValue
        }
    }

    /// The file in which the TDS operation was triggered that failed.
    public internal(set) var file: String? {
        get { self.backing.file }
        set {
            self.copyBackingStorageIfNecessary()
            self.backing.file = newValue
        }
    }

    /// The line in which the TDS operation was triggered that failed.
    public internal(set) var line: Int? {
        get { self.backing.line }
        set {
            self.copyBackingStorageIfNecessary()
            self.backing.line = newValue
        }
    }

    /// The query that failed.
    public internal(set) var query: TDSQuery? {
        get { self.backing.query }
        set {
            self.copyBackingStorageIfNecessary()
            self.backing.query = newValue
        }
    }

    /// The backend message... we should keep this internal but we can use it to print more advanced
    /// debug reasons.
    var backendMessage: TDSBackendMessage? {
        get { self.backing.backendMessage }
        set {
            self.copyBackingStorageIfNecessary()
            self.backing.backendMessage = newValue
        }
    }

    init(
        code: Code, query: TDSQuery,
        file: String? = nil, line: Int? = nil
    ) {
        self.backing = .init(code: code)
        self.query = query
        self.file = file
        self.line = line
    }

    init(code: Code) {
        self.backing = .init(code: code)
    }

    private final class Backing: @unchecked Sendable {
        fileprivate var code: Code
        fileprivate var serverInfo: ServerInfo?
        fileprivate var underlying: Error?
        fileprivate var file: String?
        fileprivate var line: Int?
        fileprivate var query: TDSQuery?
        fileprivate var backendMessage: TDSBackendMessage?

        init(code: Code) {
            self.code = code
        }

        func copy() -> Self {
            let new = Self.init(code: self.code)
            new.serverInfo = self.serverInfo
            new.underlying = self.underlying
            new.file = self.file
            new.line = self.line
            new.query = self.query
            new.backendMessage = self.backendMessage
            return new
        }
    }

    public struct ServerInfo: Sendable {
        let underlying: TDSBackendMessage.InfoError

        public var number: Int32 {
            self.underlying.number
        }

        public var state: UInt8 {
            self.underlying.state
        }

        public var severity: UInt8 {
            self.underlying.severity
        }

        public var message: String {
            self.underlying.message
        }

        public var serverName: String {
            self.underlying.serverName
        }

        public var procedureName: String {
            self.underlying.procedureName
        }

        public var lineNumber: UInt32 {
            self.underlying.lineNumber
        }

        init(_ underlying: TDSBackendMessage.InfoError) {
            self.underlying = underlying
        }
    }

    static func clientClosedConnection(underlying: Error?) -> TDSSQLError {
        var error = TDSSQLError(code: .clientClosedConnection)
        error.underlying = underlying
        return error
    }

    static func connectionError(underlying: Error?) -> TDSSQLError {
        var error = TDSSQLError(code: .connectionError)
        error.underlying = underlying
        return error
    }

    static func server(_ message: String) -> TDSSQLError {
        var error = TDSSQLError(code: .server)
        error.underlying = ServerError(message: message)
        return error
    }

    static func server(_ error: TDSBackendMessage.InfoError) -> TDSSQLError {
        var new = TDSSQLError(code: .server)
        new.serverInfo = .init(error)
        new.underlying = ServerError(message: error.message)
        return new
    }

    static func requestCancelled() -> TDSSQLError {
        TDSSQLError(code: .requestCancelled)
    }

    private struct ServerError: Error, CustomStringConvertible {
        var message: String
        var description: String { self.message }
    }
}

extension TDSSQLError: CustomStringConvertible {
    public var description: String {
        var result = #"TDSSQLError(code: \#(self.code)"#

        if let serverInfo {
            result.append(", serverInfo: ")
            result.append("InfoError(")
            result.append("number: \(String(reflecting: serverInfo.number))")
            result.append(", state: \(String(reflecting: serverInfo.state))")
            result.append(", severity: \(String(reflecting: serverInfo.severity))")
            result.append(", message: \(String(reflecting: serverInfo.message))")
            result.append(", serverName: \(String(reflecting: serverInfo.serverName))")
            result.append(", procedureName: \(String(reflecting: serverInfo.procedureName))")
            result.append(", lineNumber: \(String(reflecting: serverInfo.lineNumber))")
            result.append(")")
        }

        if let underlying {
            result.append(", underlying: \(String(reflecting: underlying))")
        }

        result.append(")")
        return result
    }
}
