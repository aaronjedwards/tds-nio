# TDSNIO

[![Supported Swift Versions](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Faaronjedwards%2Ftds-nio%2Fbadge%3Ftype%3Dswift-versions)][SPI]
[![Supported Platforms](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Faaronjedwards%2Ftds-nio%2Fbadge%3Ftype%3Dplatforms)][SPI]
[![Documentation](http://img.shields.io/badge/read_the-docs-2196f3.svg)][Documentation]
[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-brightgreen)][Apache License]
[![CI](https://github.com/aaronjedwards/tds-nio/actions/workflows/ci.yml/badge.svg)][CI]

Non-blocking, event-driven Swift client for Microsoft SQL Server built on
[SwiftNIO](https://github.com/apple/swift-nio).

TDSNIO is a client implementation of the Tabular Data Stream (TDS) protocol,
with APIs for connecting to, authenticating with, querying, and pooling
connections to SQL Server.

> TDSNIO is currently pre-release software. The first planned tag is
> `0.1.0-alpha.1`; until a stable release exists, public APIs may change between
> minor or pre-release versions.

## Features

- A `TDSConnection` for connecting to SQL Server, running SQL batches, and reading results.
- Async/await APIs for queries, row streams, and connection pooling.
- Swift string interpolation for parameterized SQL through `sp_executesql`.
- Automatic conversions between common Swift/Foundation types and SQL Server values.
- Integrated logging with [swift-log](https://github.com/apple/swift-log).
- Support for TLS negotiation using [swift-nio-ssl](https://github.com/apple/swift-nio-ssl).
- Support for `Network.framework` transports when available on Apple platforms.
- A `TDSClient` connection pool backed by PostgresNIO's `_ConnectionPoolModule`.

## Supported SQL Server Versions

TDSNIO targets TDS 7.4 and TDS 8.0 capable SQL Server deployments.

| Version | Tested |
| --- | --- |
| SQL Server 2019 | Not yet part of CI |
| SQL Server 2022 | Yes, in GitHub Actions |
| Azure SQL Database | Not yet part of CI |

The current CI suite runs integration tests against
`mcr.microsoft.com/mssql/server:2022-latest`.

## Language and Platform Support

TDSNIO currently requires Swift 6.1 or newer. CI tests Swift 6.1, Swift 6.2,
Swift 6.3, and a nightly toolchain, with nightly allowed to fail.

The package declares support for macOS 13, iOS 16, tvOS 16, watchOS 9, and
visionOS 1 or newer. The primary tested platforms today are Linux and macOS.

The repository includes `.swift-version` set to Swift 6.2.0 so tooling and
Swift Package Index documentation builds use the same baseline as CI.

## API Docs

Once Swift Package Index has indexed a tagged release, generated DocC
documentation will be available at [TDSNIO API docs][Documentation].

## Getting Started

### Adding the Dependency

Before the first tag, depend on `main`:

```swift
dependencies: [
    .package(url: "https://github.com/aaronjedwards/tds-nio.git", branch: "main"),
]
```

After `0.1.0-alpha.1` is tagged, prefer an exact pre-release dependency:

```swift
dependencies: [
    .package(url: "https://github.com/aaronjedwards/tds-nio.git", exact: "0.1.0-alpha.1"),
]
```

Add `TDSNIO` to the target that uses it:

```swift
targets: [
    .target(name: "MyTarget", dependencies: [
        .product(name: "TDSNIO", package: "tds-nio"),
    ])
]
```

### Creating a Connection

Create a `TDSConnection.Configuration` with the SQL Server host, credentials,
and optional initial database:

```swift
import Logging
import TDSNIO

let configuration = TDSConnection.Configuration(
    host: "127.0.0.1",
    port: 1433,
    username: "sa",
    password: "your-strong-password",
    database: "app_database"
)

let logger = Logger(label: "tds-nio")

let connection = try await TDSConnection.connect(
    configuration: configuration,
    id: 1,
    logger: logger
)

try await connection.close()
```

You can also build the same configuration from a SQL Server-style connection
string:

```swift
let configuration = try TDSConnection.Configuration(
    connectionString: "Server=127.0.0.1,1433;User Id=sa;Password=your-strong-password;Database=app_database"
)
```

All connections can use `TDSConnection.Configuration.TLS`. Use `.prefer(_:)`
or `.require(_:)` with a `NIOSSLContext` when TLS should be negotiated:

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

### Running SQL Statements

Use `query(_:)` when you want rows from the first result set:

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

Use `execute(_:)` when you need the full `TDSQueryResult`, including rows
affected, output parameters, return status, or additional result-set metadata:

```swift
let result = try await connection.execute("""
    UPDATE users
    SET is_active = 1
    WHERE last_login_at IS NOT NULL
    """)

print(result.rowsAffected ?? 0)
```

### Statements With Parameters

`TDSQuery` supports Swift string interpolation for bind parameters. Interpolated
values are sent as parameters through `sp_executesql`, rather than being pasted
into the SQL string:

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

The following common Swift and Foundation types can be bound into queries and
decoded from rows:

- `Bool`
- `UInt8`, `Int16`, `Int32`, `Int`, `Int64`
- `Float`, `Double`, `Decimal`
- `String`
- `[UInt8]`, `ByteBuffer`, `Data`
- `UUID`, `TDSGUID`
- `Date`, `TDSDate`, `TDSTime`, `TDSDateTime`, `TDSDateTimeOffset`
- `TDSJSON`, `TDSJSONValue`
- `TDSTableValuedParameter`

### Connection Pooling

For applications that need to reuse connections, create a `TDSClient` from a
connection configuration and run it in a long-lived task:

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

`withConnection(_:)` leases a connection for the closure's lifetime and returns
it to the pool afterward. The connection is marked for reset before its next
request so pooled sessions do not accidentally share session state between
callers.

## Changelog

Pre-release changes will be documented on the [GitHub releases page][Releases]
once tags exist.

## License

[Apache 2.0][Apache License]

Copyright (c) 2026 TDSNIO project authors.

This project contains code and design work influenced by other open source
projects. See [NOTICE.txt](NOTICE.txt) for attribution details.

**Microsoft SQL Server** is a trademark of Microsoft Corporation. Any use of
the trademark is for identification only and does not imply affiliation with or
endorsement by Microsoft.

**Swift** is a trademark of Apple Inc. Any use of the trademark is for
identification only and does not imply affiliation with or endorsement by Apple.

[SPI]: https://swiftpackageindex.com/aaronjedwards/tds-nio
[Documentation]: https://swiftpackageindex.com/aaronjedwards/tds-nio/documentation/tdsnio
[Apache License]: LICENSE
[Releases]: https://github.com/aaronjedwards/tds-nio/releases
[CI]: https://github.com/aaronjedwards/tds-nio/actions/workflows/ci.yml
