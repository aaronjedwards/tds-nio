# ``TDSNIO``

Non-blocking, event-driven Swift client for Microsoft SQL Server built on SwiftNIO.

## Overview

TDSNIO implements a client for the Tabular Data Stream (TDS) protocol. It can
connect to SQL Server, authenticate with username and password credentials, run
SQL batches, bind common Swift and Foundation values as query parameters, decode
rows, and pool connections for server applications.

TDSNIO is currently pre-release software and public APIs may change substantially before a stable release.

### Features

- A ``TDSConnection`` for connecting to SQL Server and running requests.
- Async/await APIs for queries, row streams, and connection pooling.
- Swift string interpolation for parameterized SQL through `sp_executesql`.
- Automatic conversions between common Swift/Foundation types and SQL Server values.
- Integrated logging with swift-log.
- TLS support through swift-nio-ssl.
- A ``TDSClient`` connection pool backed by PostgresNIO's `_ConnectionPoolModule`.

### Supported SQL Server Versions

TDSNIO targets TDS 7.4 and TDS 8.0 capable SQL Server deployments. The current
CI suite runs integration tests against SQL Server 2019, 2022, and 2025.

## Topics

### Guides

- <doc:connecting-to-sql-server>
- <doc:querying-and-pooling>

### Connections

- ``TDSConnection``
- ``TDSConnection/Configuration``
- ``TDSConnection/Configuration/TLS``
- ``TDSConnection/Configuration/Options``
- ``TDSConnectionStringError``
- ``TDSProtocolVersion``
- ``TDSClient``
- ``TDSClient/Options``

### Querying

- ``TDSQuery``
- ``TDSBindings``
- ``TDSRPC``
- ``TDSQueryResult``
- ``TDSResultSet``
- ``TDSRowSequence``
- ``TDSRowStream``
- ``TDSRow``
- ``TDSRandomAccessRow``
- ``TDSCell``
- ``TDSColumn``
- ``TDSOutputParameter``
- ``TDSOffset``
- ``TDSAlternateResultSet``

### Encoding and Decoding

- ``TDSBindable``
- ``TDSDecodable``
- ``TDSCodable``
- ``TDSRowDecodable``
- ``TDSDecodingError``
- ``TDSData``
- ``TDSDataType``
- ``TDSSQLType``
- ``TDSGUID``
- ``TDSDate``
- ``TDSTime``
- ``TDSDateTime``
- ``TDSDateTimeOffset``
- ``TDSJSON``
- ``TDSJSONValue``
- ``TDSTableValuedParameter``

### Messages and Events

- ``TDSSQLError``
- ``TDSInfoMessage``
- ``TDSEnvChangeMessage``
- ``TDSSessionStateMessage``
- ``TDSAuthenticationChallenge``
- ``TDSAuthenticationToken``
