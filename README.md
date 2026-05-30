# SwiftTDS (TDS Implementation)

Non-blocking, event-driven Swift implementation of the [Tabular Data Stream (TDS) Protocol](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/1ef08b76-1594-40cf-8ce0-d2407133dd3d) built on top of [SwiftNIO](https://github.com/apple/swift-nio).

This package provides a client implementation of the TDS protocol, with functionality for connecting to, authorizing, and querying instances of Microsoft's SQL Server. 

**It is currenlty under active development and is an incomplete implementation of the protocol.**

## Getting started

### Adding the dependency

Add `TDSNIO` as a dependency to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/aaronjedwards/tds-nio.git", branch: "main"),
]
```

Add `TDSNIO` to the target you want to use it in:

```swift
targets: [
    .target(name: "MyTarget", dependencies: [
        .product(name: "TDSNIO", package: "tds-nio"),
    ])
]
```

### Creating a connection

Create a `TDSConnection.Configuration` with the SQL Server host, port, credentials, and optional initial database:

```swift
import TDSNIO

let configuration = TDSConnection.Configuration(
    host: "127.0.0.1",
    port: 1433,
    username: "sa",
    password: "your-strong-password",
    database: "app_database"
)
```

You can also build the same configuration from a SQL Server-style connection string:

```swift
let configuration = try TDSConnection.Configuration(
    connectionString: "Server=127.0.0.1,1433;User Id=sa;Password=your-strong-password;Database=app_database"
)
```

To enable TLS, pass `TDSConnection.Configuration.TLS.prefer(_:)` or `require(_:)` with a `NIOSSLContext`:

```swift
import NIOSSL
import TDSNIO

let sslContext = try NIOSSLContext(configuration: .makeClientConfiguration())

let configuration = TDSConnection.Configuration(
    host: "sql.example.com",
    username: "sa",
    password: "your-strong-password",
    database: "app_database",
    tls: .require(sslContext)
)
```

A `Logger` can be supplied to record connection background events:

```swift
import Logging
import TDSNIO

let logger = Logger(label: "tds-nio")

let connection = try await TDSConnection.connect(
    configuration: configuration,
    id: 1,
    logger: logger
)

try await connection.close()
```

### Sending requests

Once a connection has been established, send SQL with `query(_:)` when you want rows back:

```swift
let rows = try await connection.query("""
    SELECT id, username, created_at
    FROM users
    ORDER BY id
    """)

for try await row in rows {
    let id = try row.decode(column: "id", as: Int.self)
    let username = try row.decode(column: "username", as: String.self)
    let createdAt = try row.decode(column: "created_at", as: Date.self)

    print(id, username, createdAt)
}
```

Rows can also be decoded positionally:

```swift
let rows = try await connection.query("SELECT id, username FROM users")

for try await (id, username) in rows.decode((Int, String).self) {
    print(id, username)
}
```

Use `execute(_:)` for SQL batches where you want the full `TDSQueryResult`, including rows affected and additional result metadata:

```swift
let result = try await connection.execute("""
    UPDATE users
    SET is_active = 1
    WHERE last_login_at IS NOT NULL
    """)

print(result.rowsAffected ?? 0)
```

### Querying with parameters

`TDSQuery` supports Swift string interpolation for bind parameters. Interpolated values are sent as parameters through `sp_executesql`, rather than being pasted into the SQL string:

```swift
let id = 42
let username = "fancyuser"

try await connection.execute("""
    INSERT INTO users (id, username)
    VALUES (\(id), \(username))
    """)
```

Optional values can be interpolated as well:

```swift
let lastLoginAt: Date? = nil

try await connection.execute("""
    UPDATE users
    SET last_login_at = \(lastLoginAt)
    WHERE id = \(id)
    """)
```

The following common Swift and Foundation types can be bound into queries and decoded from rows:

- `Bool`
- `UInt8`, `Int16`, `Int32`, `Int`, `Int64`
- `Float`, `Double`, `Decimal`
- `String`
- `[UInt8]`, `ByteBuffer`, `Data`
- `UUID`, `TDSGUID`
- `Date`, `TDSDate`, `TDSTime`, `TDSDateTime`, `TDSDateTimeOffset`

### Connection pooling

For applications that need to reuse connections, create a `TDSClient` from a connection configuration and run it in a long-lived task:

```swift
import Logging
import TDSNIO

var options = TDSClient.Options()
options.maximumConnections = 10

let client = TDSClient(
    configuration: configuration,
    options: options,
    backgroundLogger: Logger(label: "tds-nio.pool")
)

Task {
    await client.run()
}

try await client.withConnection { connection in
    let rows = try await connection.query("SELECT id, username FROM users")

    for try await (id, username) in rows.decode((Int, String).self) {
        print(id, username)
    }
}
```

`withConnection(_:)` leases a connection for the closure's lifetime and returns it to the pool afterward. The connection is marked for reset before its next request so pooled sessions do not accidentally share session state between callers.
