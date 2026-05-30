# Connecting to SQL Server

Create a connection configuration, connect, run work, and close the connection
when you are done.

## Overview

The simplest configuration uses a host, port, SQL Server username, password, and
optional database name.

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

let connection = try await TDSConnection.connect(
    configuration: configuration,
    id: 1,
    logger: Logger(label: "tds-nio")
)

try await connection.close()
```

You can also initialize the same configuration from a SQL Server-style
connection string:

```swift
let configuration = try TDSConnection.Configuration(
    connectionString: "Server=127.0.0.1,1433;User Id=sa;Password=your-strong-password;Database=app_database"
)
```

Use ``TDSConnection/Configuration/TLS`` to control TLS negotiation. Pass
``TDSConnection/Configuration/TLS/prefer(_:)`` when TLS should be used if the
server supports it, or ``TDSConnection/Configuration/TLS/require(_:)`` when the
connection must be encrypted.

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

### Related

- ``TDSConnection``
- ``TDSConnection/Configuration``
- ``TDSConnection/Configuration/TLS``

