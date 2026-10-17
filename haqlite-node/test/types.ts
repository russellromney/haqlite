import { HaQLite, type OpenOptions, type DatabaseStatus } from 'haqlite'

async function example(path: string, options: OpenOptions) {
  const db: HaQLite = await HaQLite.local(path, 'CREATE TABLE example (value)')
  const changes: number = await db.execute('INSERT INTO example VALUES (?)', [42n])
  const rows: Array<Array<null | number | string | bigint | Buffer>> =
    await db.query('SELECT ?, ?, ?, ?, ?', [Buffer.from([1]), null, true, 'text', 3.5])
  const status: DatabaseStatus = await db.status()
  const position: bigint = status.replayPosition
  const handedOff: boolean = await db.handoff()
  await db.close()

  const clustered = await HaQLite.open(path, options)
  await clustered.close()

  // @ts-expect-error Objects are not SQL values.
  await db.execute('SELECT ?', [{}])
  // @ts-expect-error S3 bucket is required.
  await HaQLite.open(path, {})
  return { changes, rows, position, handedOff }
}

void example
