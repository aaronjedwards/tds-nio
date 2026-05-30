# Querying and Pooling

Run SQL with `TDSConnection`, decode rows, and use `TDSClient` when an
application needs a reusable connection pool.

## Overview

Use ``TDSConnection/query(_:file:line:)`` when you want rows from the first
result set:

```swift
let rows = try await connection.query("""
    SELECT id, username
    FROM users
    ORDER BY id
    """)

for try await row in rows {
    let id = try row.decode(column: "id", as: Int.self)
    let username = try row.decode(column: "username", as: String.self)

    print(id, username)
}
```

Rows can also be decoded positionally:

```swift
let rows = try await connection.query("SELECT id, username FROM users")

for try await (id, username) in rows.decode((Int, String).self) {
    print(id, username)
}
```

Use ``TDSConnection/execute(_:file:line:)`` when you need the full
``TDSQueryResult``:

```swift
let result = try await connection.execute("""
    UPDATE users
    SET is_active = 1
    WHERE last_login_at IS NOT NULL
    """)

print(result.rowsAffected ?? 0)
```

`TDSQuery` supports string interpolation for bind parameters. Interpolated
values are sent to SQL Server as parameters through `sp_executesql`.

```swift
let id = 42
let username = "fancyuser"

try await connection.execute("""
    INSERT INTO users (id, username)
    VALUES (\(id), \(username))
    """)
```

For applications that need to reuse connections, create a ``TDSClient`` and run
it in a long-lived task:

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

### Related

- ``TDSConnection/query(_:file:line:)``
- ``TDSConnection/execute(_:file:line:)``
- ``TDSQuery``
- ``TDSRowSequence``
- ``TDSClient``

