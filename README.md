# DynamoDB Connector

Adds DynamoDB connectivity as an installable connector extension.

This connector is listed in the public Irodori extension marketplace.

## Connector

- Extension ID: `irodori.dynamodb`
- Engine ID: `dynamodb`
- Wire: `keyValue`
- Default port: `443`
- Native ABI: `irodori.connector.native.v1`
- Driver linked: `true`

The native driver uses the AWS SDK for DynamoDB and supports connection checks, PartiQL statements, and table metadata.

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust code keeps native ABI exports in `src/lib.rs`, shared buffer/JSON helpers in `src/abi.rs`, and DynamoDB behavior in `src/driver.rs`.

## Connection Metadata

- Endpoint modes: `cloudResource`, `customEndpoint`, `connectionString`
- Transport modes: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS supported: `true`
- Custom driver options: `true`

| Auth method | Label | Secret purposes |
|---|---|---|
| `none` | No authentication | none |
| `connectionString` | Connection string / DSN | none |
| `awsSigV4` | AWS SigV4 | `token` |
| `awsProfile` | AWS shared config profile | none |
| `awsSso` | AWS IAM Identity Center / SSO | `token` |
| `webIdentity` | AWS web identity | `token` |
| `sessionToken` | AWS session token | `token` |
| `customDriverOptions` | Custom driver options | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## ABI Calls

The driver handles these JSON requests today:

| Method | Response |
|---|---|
| `health` / `ping` | Connector health, engine id, ABI version, and driver link status. |
| `describe` / `capabilities` | Embedded manifest and connector config. |
| `manifest` | Raw `irodori.extension.json`. |
| `config` | Raw `connector.config.json`. |
| `connect` | Opens a DynamoDB client and validates it with `ListTables`. |
| `query` | Runs DynamoDB PartiQL through `ExecuteStatement`. |
| `metadata` | Loads table metadata through `ListTables` and `DescribeTable`. |
| `close` | Removes the cached native connection. |

## Development


Generated extension repositories share `../target` across sibling repositories so Rust dependencies are compiled once per checkout. DuckDB and MotherDuck are driver-linked by default; set `IRODORI_CONNECTOR_LINK_DUCKDB=0` only when you need metadata-only DuckDB-compatible scaffolds.


```sh
make check
make build
```

Release packages place platform-specific native artifacts under `dist/native`.
