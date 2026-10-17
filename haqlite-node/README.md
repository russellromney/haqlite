# haqlite for Node.js

Native Node.js bindings for base haqlite, with generated TypeScript types and CommonJS and ESM entry points. Use local SQLite for development, or join an embedded SingleWriter HA cluster with S3 WAL storage.

Like the Rust library, this package is experimental. It does not include `haqlite-turbolite` or the remote-only `HaQLiteClient`.

## Build from source

Use Node.js 22 or later, npm, and a Rust toolchain compatible with the workspace dependencies. The package currently builds a native binary for your host; it does not download prebuilt binaries.

The root workspace currently patches `hadb`, `walrust`, and `turbolite` to sibling checkouts. Make those dependencies available before building. Compatible revisions used for validation were `hadb` at `c5773c1`, `walrust` at `e556bd5`, and `turbolite` at `79b38c5`.

The root `.cargo/config.toml` also refers to a local `sccache` installation. Set `RUSTC_WRAPPER=` if you do not use that installation.

From the repository root:

```sh
cd haqlite-node
npm ci
npm run build:release
npm test
npm run test:types
```

Cargo overrides can be passed through the build command:

```sh
RUSTC_WRAPPER= npm run build -- -- --config /path/to/cargo-overrides.toml
```

`npm run build` produces a debug build. Both build commands generate `index.js`, `index.d.ts`, and a platform-specific `.node` binary. Do not commit the binary. Commit regenerated JavaScript and declarations when the Rust interface changes.

To use the built package in another local application:

```sh
npm install /path/to/haqlite/haqlite-node
```

Build for another supported target on a suitable toolchain with `npm run build:release -- --target <rust-target-triple>`. Cross-compilation toolchains and prebuilt distribution are not configured here.

## Open a local database

```js
import { HaQLite } from 'haqlite'
// CommonJS: const { HaQLite } = require('haqlite')

const db = await HaQLite.local(
  './app.db',
  'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)',
)

try {
  const changes = await db.execute(
    'INSERT INTO users (name) VALUES (?)',
    ['Alice'],
  )
  const rows = await db.query('SELECT id, name FROM users ORDER BY id')
  console.log(changes, rows) // 1, [[1, 'Alice']]
} finally {
  await db.close()
}
```

The schema is optional. Use idempotent schema statements such as `CREATE TABLE IF NOT EXISTS`. Databases must be file-backed; `:memory:` is not supported by the underlying open path.

## Join an HA cluster

```js
const db = await HaQLite.open('./data/app.db', {
  bucket: 'my-haqlite-bucket',
  schema: 'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)',
  prefix: 'myapp/',
  instanceId: 'node-1',
  address: 'http://node-1.internal:18080',
  forwardingPort: 18080,
  secret: process.env.HAQLITE_SECRET,
  syncIntervalMs: 1000,
  followerPullIntervalMs: 1000,
  // endpoint: 'http://localhost:9000', // custom S3-compatible endpoint
  // leaseEndpoint: 'https://lease-service.example.com',
  // leaseToken: process.env.LEASE_TOKEN,
})

try {
  await db.execute('INSERT INTO users (name) VALUES (?)', ['Alice'])
  console.log(await db.status())
} finally {
  await db.close()
}
```

Use the same bucket, prefix, database filename stem, and schema across nodes. Give each node its own local database path and instance ID. Set an address that the other nodes can reach. Configure the same forwarding secret on all nodes; the built-in server listens on all interfaces, so restrict network access too.

S3 credentials and region use the AWS provider chain, including `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`. Custom S3 endpoints use path-style access. By default, leases use the same S3 bucket. Set `leaseEndpoint` for HTTP leases instead. `leaseToken` requires `leaseEndpoint`. The bindings do not read `HAQLITE_LEASE_URL` implicitly.

The lease backend must support atomic conditional writes. Tigris is not suitable for S3 leases; use an HTTP lease service with Tigris WAL storage.

`forwardingPort: 0` disables the built-in forwarding listener. Such a node cannot receive forwarded writes if it becomes the leader. This setting is for deployments that provide their own leader routing.

## Methods and SQL values

All database operations return promises. SQL runs on a dedicated database worker, not on Node's event loop. Calls on one handle execute in invocation order. Different handles have independent queues.

| Method | Result |
| --- | --- |
| `HaQLite.local(path, schema?)` | local database handle |
| `HaQLite.open(path, options)` | HA database handle |
| `execute(sql, params?)` | number of affected rows; follower writes forward to the leader |
| `query(sql, params?)` | array of rows, each an array of column values; reads are local |
| `status()` | role, `isCaughtUp`, bigint `replayPosition`, and `dbPath` |
| `handoff()` | whether leadership was released; false in local mode |
| `close()` | drains queued work, flushes replication, releases the lease, and closes SQLite |

Supported positional parameters:

| JavaScript value | SQLite value |
| --- | --- |
| `null` | NULL |
| `boolean` | INTEGER 0 or 1 |
| safe integer `number` | INTEGER |
| finite fractional `number` | REAL |
| signed 64-bit `bigint` | INTEGER |
| `string` | TEXT |
| `Buffer` | BLOB, copied when the method is called |

SQL INTEGER results return as numbers within JavaScript's safe integer range, otherwise as bigints. Booleans return as 0 or 1. BLOB results are Buffers. Queries with no matches return `[]`.

Unsafe integer numbers, out-of-range bigints, and non-finite numbers are rejected rather than silently truncated. Named parameters and prepared-statement handles are not exposed. Invalid JavaScript argument types can throw synchronously; SQL and operation failures reject the returned promise.

Always await `close()` before exiting. Repeated close calls are safe. Other operations reject after closing. Garbage collection only attempts best-effort cleanup; it cannot report replication or shutdown failures.

## HA limitations

Reads on followers can lag behind writes. A successful write acknowledges the underlying library's write path, not synchronous durability in S3. `syncIntervalMs` controls background WAL shipping.

The current base engine can also stall incremental follower replay after restoring a snapshot. Its checksum-chain verifier expects the next sequence after the reported replay position, but the restored follower position starts at 0. In an S3-backed validation run, the next changeset had sequence 4, so the verifier refused it and follower reads stayed stale. These bindings do not change that engine behavior. The Node HA integration test covers cluster setup and authenticated write forwarding, not successful incremental follower catch-up.

Track the follower replay bug in [haqlite issue #12](https://github.com/russellromney/haqlite/issues/12).

Only the base library's implemented SingleWriter topology is exposed. SharedWriter, explicit client roles, tiered storage, and custom JavaScript storage callbacks are not included.

## Run the S3 integration test

Local tests need no external services. To exercise cluster setup and forwarding, point the test at a disposable S3-compatible service with an existing bucket and conditional-write support:

```sh
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export HAQLITE_TEST_S3_ENDPOINT=http://127.0.0.1:5000
export HAQLITE_TEST_S3_BUCKET=haqlite-test
aws --endpoint-url "$HAQLITE_TEST_S3_ENDPOINT" s3 mb "s3://$HAQLITE_TEST_S3_BUCKET"
npm test
```

For a local test service, run Moto in another terminal:

```sh
uvx --from 'moto[server]==5.1.15' moto_server --host 127.0.0.1 --port 5000
```

Use only test credentials and a disposable bucket. The integration test writes replicated data under a unique prefix. It skips when `HAQLITE_TEST_S3_ENDPOINT` is unset.
