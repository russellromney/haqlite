const assert = require('node:assert/strict')
const { mkdtemp, mkdir, rm } = require('node:fs/promises')
const { tmpdir } = require('node:os')
const { join } = require('node:path')
const { setImmediate: immediate } = require('node:timers/promises')
const { test } = require('node:test')
const { HaQLite } = require('haqlite')

async function directory(t) {
  const path = await mkdtemp(join(tmpdir(), 'haqlite-node-'))
  const databases = []
  t.after(async () => {
    // Close before removing files, including on platforms that disallow
    // unlinking open databases. Close followers before their leader.
    for (const db of databases.reverse()) await db.close()
    await rm(path, { recursive: true, force: true })
  })
  return { path, track: (db) => { databases.push(db); return db } }
}

async function local(t, schema = '') {
  const temporary = await directory(t)
  const path = join(temporary.path, 'test.db')
  const db = temporary.track(await HaQLite.local(path, schema))
  return { db, path, track: temporary.track }
}

test('CommonJS and ESM expose the same native class and round-trip SQL values', async (t) => {
  const esm = await import('haqlite')
  assert.equal(esm.HaQLite, HaQLite)
  const { db } = await local(t)
  const values = [
    null, true, false, 0, -42, 3.5, 'héllo\0世界',
    Buffer.from([0, 128, 255]), Buffer.alloc(0),
    Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER,
    9007199254740992n, -9007199254740992n,
    9223372036854775807n, -9223372036854775808n, 0n,
  ]
  const expected = [
    null, 1, 0, 0, -42, 3.5, 'héllo\0世界',
    Buffer.from([0, 128, 255]), Buffer.alloc(0),
    9007199254740991, -9007199254740991,
    9007199254740992n, -9007199254740992n,
    9223372036854775807n, -9223372036854775808n, 0,
  ]
  assert.deepEqual(
    await db.query('SELECT ' + values.map(() => '?').join(', '), values),
    [expected],
  )
  assert.deepEqual(await db.query('SELECT 1 WHERE 0'), [])
})

test('rejects invalid SQL parameters without truncation or poisoning the database', async (t) => {
  const { db } = await local(t)
  for (const [value, message] of [
    [NaN, /finite/], [Infinity, /finite/], [-Infinity, /finite/],
    [9007199254740992, /Unsafe integer/],
    [9223372036854775808n, /signed 64-bit/],
    [-9223372036854775809n, /signed 64-bit/],
    [1n << 100n, /signed 64-bit/],
    [undefined, /none of these types/], [{}, /none of these types/],
  ]) {
    await assert.rejects(async () => db.query('SELECT ?', [value]), message)
  }
  assert.deepEqual(await db.query('SELECT 1'), [[1]])
})

test('queues operations in call order, snapshots buffers, and drains before close', async (t) => {
  const { db } = await local(t, 'CREATE TABLE entries (value BLOB)')
  const buffer = Buffer.from([1, 2, 3])
  const insert = db.execute('INSERT INTO entries VALUES (?)', [buffer])
  buffer.fill(9)
  const inserts = Array.from({ length: 20 }, (_, i) =>
    db.execute('INSERT INTO entries VALUES (?)', [Buffer.from([i])]),
  )
  const rows = db.query('SELECT value FROM entries ORDER BY rowid')
  const closing = db.close()
  const closingAgain = db.close()
  assert.equal(await insert, 1)
  assert.deepEqual(await Promise.all(inserts), Array(20).fill(1))
  const result = await rows
  assert.equal(result.length, 21)
  assert.deepEqual(result[0], [Buffer.from([1, 2, 3])])
  assert.deepEqual(result[20], [Buffer.from([19])])
  await Promise.all([closing, closingAgain])
  await db.close()
  for (const operation of [
    () => db.execute('INSERT INTO entries VALUES (NULL)'),
    () => db.query('SELECT 1'),
    () => db.status(),
    () => db.handoff(),
  ]) {
    await assert.rejects(async () => operation(), /Engine closed/)
  }
})

test('persists data and keeps SQL errors observable across the native interface', async (t) => {
  const { db, path, track } = await local(t, 'CREATE TABLE users (name TEXT UNIQUE)')
  assert.equal(await db.execute('INSERT INTO users VALUES (?)', ['Alice']), 1)
  await assert.rejects(db.execute('INSERT INTO users VALUES (?)', ['Alice']), /UNIQUE/)
  await assert.rejects(db.query('SELECT missing FROM users'), /missing/)
  const status = await db.status()
  assert.equal(status.role, 'Leader')
  assert.equal(status.isCaughtUp, true)
  assert.equal(status.replayPosition, 0n)
  assert.equal(status.dbPath, path)
  assert.equal(await db.handoff(), false)
  await db.close()
  const reopened = track(await HaQLite.local(path))
  assert.deepEqual(await reopened.query('SELECT name FROM users'), [['Alice']])
})

test('long SQLite work leaves the Node event loop free', async (t) => {
  const { db } = await local(t)
  const query = db.query(
    'WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000) SELECT sum(x) FROM n',
  )
  assert.equal(
    await Promise.race([query.then(() => 'query'), immediate().then(() => 'event loop')]),
    'event loop',
  )
  assert.deepEqual(await query, [[500000500000]])
})

test('validates file paths and HA options before contacting storage', async (t) => {
  const path = join((await directory(t)).path, 'test.db')
  for (const invalid of ['', ':memory:']) {
    await assert.rejects(HaQLite.local(invalid), /file-backed/)
  }
  for (const [options, message] of [
    [{ bucket: '' }, /bucket/],
    [{ leaseToken: 'token' }, /leaseToken/],
    [{ forwardingPort: -1 }, /forwardingPort/],
    [{ forwardingPort: 65536 }, /forwardingPort/],
    [{ forwardingPort: 1.5 }, /forwardingPort/],
    [{ syncIntervalMs: 0 }, /syncIntervalMs/],
    [{ readConcurrency: 0 }, /readConcurrency/],
    [{ followerPullIntervalMs: NaN }, /followerPullIntervalMs/],
    [{ leaseTtlSeconds: -1 }, /leaseTtlSeconds/],
    [{ leaseRenewIntervalMs: Infinity }, /leaseRenewIntervalMs/],
    [{ leaseFollowerPollIntervalMs: 0 }, /leaseFollowerPollIntervalMs/],
    [{ forwardTimeoutMs: 0 }, /forwardTimeoutMs/],
  ]) {
    await assert.rejects(HaQLite.open(path, { bucket: 'unused', ...options }), message)
  }
  await assert.rejects(HaQLite.local(path, 'invalid schema'), /syntax error/)
})

test('HA nodes use S3 storage and forward authenticated writes to the leader', {
  skip: !process.env.HAQLITE_TEST_S3_ENDPOINT,
  timeout: 30000,
}, async (t) => {
  const { createServer } = require('node:net')
  const temporary = await directory(t)
  const root = temporary.path
  const dbName = require('node:crypto').randomUUID() + '.db'
  await mkdir(join(root, 'leader'))
  await mkdir(join(root, 'follower'))

  // Reserve a free port rather than colliding with a developer's forwarding server.
  const reservation = createServer()
  await new Promise((resolve) => reservation.listen(0, '127.0.0.1', resolve))
  const port = reservation.address().port
  await new Promise((resolve) => reservation.close(resolve))
  const options = {
    bucket: process.env.HAQLITE_TEST_S3_BUCKET || 'haqlite-test',
    endpoint: process.env.HAQLITE_TEST_S3_ENDPOINT,
    schema: 'CREATE TABLE IF NOT EXISTS entries (value TEXT)',
    prefix: 'node-test-' + Date.now() + '/',
    syncIntervalMs: 50,
    followerPullIntervalMs: 50,
    forwardingPort: port,
    address: 'http://127.0.0.1:' + port,
    secret: 'test-forwarding-secret',
  }
  const leader = temporary.track(await HaQLite.open(join(root, 'leader', dbName), {
    ...options, instanceId: 'node-leader',
  }))
  assert.equal((await leader.status()).role, 'Leader')
  const unauthorized = await fetch(options.address + '/haqlite/execute', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql: 'INSERT INTO entries VALUES (?)', params: [{ Text: 'unauthorized' }] }),
  })
  assert.equal(unauthorized.status, 401)

  const follower = temporary.track(await HaQLite.open(join(root, 'follower', dbName), {
    ...options, instanceId: 'node-follower', forwardingPort: 0,
  }))
  assert.equal((await follower.status()).role, 'Follower')
  assert.equal(await follower.execute('INSERT INTO entries VALUES (?)', ['forwarded']), 1)
  assert.deepEqual(await leader.query('SELECT value FROM entries'), [['forwarded']])

  // Incremental follower replay is owned by the core engine tests. Its known
  // post-restore sequence-verifier limitation is documented in README.md.
  await follower.close()
  assert.equal(await leader.handoff(), true)
})
