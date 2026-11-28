// Initializing one database from several processes at once: the look at
// the columns of a table made before them, and the adding of them, are
// not split by another process's initialization.

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, test } from 'node:test';

import { PostgresStore, SQLiteStore } from './store.ts';

const dir = mkdtempSync(join(tmpdir(), 'kairo-sdk-store-init-'));
after(() => rmSync(dir, { recursive: true, force: true }));

/** A run table from before the columns added later (parent, workflow, meta, created_order). */
const OLD_SCHEMA = (p: string, blob: string) => `CREATE TABLE ${p}run (id TEXT PRIMARY KEY, plan TEXT NOT NULL, hash TEXT NOT NULL, state ${blob} NOT NULL,
	input TEXT, status TEXT NOT NULL, output TEXT, error TEXT, seq INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`;
const ADDED = ['parent', 'workflow', 'meta', 'created_order'];

async function sqlite(): Promise<any> {
	const name = 'node:sqlite';
	return ((await import(name)) as any).DatabaseSync;
}

async function runColumns(path: string): Promise<string[]> {
	const DatabaseSync = await sqlite();
	const db = new DatabaseSync(path);
	try {
		return (db.prepare(`PRAGMA table_info(kairo_run)`).all() as { name: string }[]).map((c) => c.name);
	} finally {
		db.close();
	}
}

describe('initializing a SQLite store', () => {
	// Another process's initialization comes between this one's look at the
	// columns and its adding them, the moment the two would both add the
	// same column (duplicate column name). The look and the change are one
	// transaction holding the write lock: the other process cannot change
	// the schema in between (told the database is locked, with no wait
	// here), and both initializations succeed.
	test('another process cannot come between the look at the columns and the adding of them', async () => {
		const path = join(dir, 'race.db');
		const DatabaseSync = await sqlite();
		const old = new DatabaseSync(path);
		old.exec(OLD_SCHEMA('kairo_', 'BLOB'));
		old.close();

		const proto = DatabaseSync.prototype;
		const exec = proto.exec;
		let interleaved: unknown = 'not run';
		proto.exec = function (this: any, sql: string) {
			if (interleaved === 'not run' && sql.startsWith('ALTER TABLE kairo_run ADD COLUMN parent')) {
				// Between the look and the change: what another process's
				// initialization does, without waiting for a lock.
				const other = new DatabaseSync(path, { timeout: 0 });
				try {
					exec.call(other, 'ALTER TABLE kairo_run ADD COLUMN parent TEXT');
					interleaved = undefined;
				} catch (e) {
					interleaved = e;
				} finally {
					other.close();
				}
			}
			return exec.call(this, sql);
		};
		const a = new SQLiteStore(path);
		try {
			await a.init();
		} finally {
			proto.exec = exec;
		}
		assert.notEqual(interleaved, 'not run', 'the column was not added');
		assert.ok(interleaved instanceof Error, 'another process changed the schema between the look and the change');
		assert.match(String(interleaved.message), /locked|busy/);
		// The other process, initializing after: nothing left to add.
		const b = new SQLiteStore(path);
		await b.init();
		const cols = await runColumns(path);
		for (const c of ADDED) assert.ok(cols.includes(c), `column ${c}`);
		await a.close();
		await b.close();
	});

	test('a table from before the added columns gets them, and its runs are read', async () => {
		const path = join(dir, 'old.db');
		const DatabaseSync = await sqlite();
		const old = new DatabaseSync(path);
		old.exec(OLD_SCHEMA('kairo_', 'BLOB'));
		old.exec(`INSERT INTO kairo_run (id, plan, hash, state, status, seq, created_at, updated_at) VALUES ('old', 'p', 'h', x'00', 'running', 0, 1, 1)`);
		old.close();
		const s = new SQLiteStore(path);
		await s.init();
		const r = await s.get('old');
		assert.equal(r?.status, 'running');
		assert.equal(r?.parent, null);
		assert.equal(r?.createdOrder, 0);
		const cols = await runColumns(path);
		for (const c of ADDED) assert.ok(cols.includes(c), `column ${c}`);
		await s.close();
	});
});

// PostgreSQL (KAIRO_SDK_PG_DSN, and KAIRO_SDK_PG_MODULE where "pg" is
// installed): two sessions making the same table at once can both find it
// missing, so initialization takes an advisory lock of the prefix's first.
// While another session holds it, initialization waits for it (seen in
// pg_locks), and goes on once it is let go. Several initializations at
// once, of a table from before the added columns, all succeed.
const dsn = process.env.KAIRO_SDK_PG_DSN;
const pgDir = process.env.KAIRO_SDK_PG_MODULE;
describe('initializing a PostgreSQL store', { skip: !dsn || !pgDir }, () => {
	test('initialization takes the schema lock', async () => {
		const pg = createRequire(join(pgDir!, 'x.js'))('pg');
		const pool = new pg.Pool({ connectionString: dsn, max: 8 });
		const prefix = `ktsinit${process.pid}_`;
		const drop = async () => {
			for (const t of ['run', 'counter', 'event', 'timer', 'lease', 'rate']) await pool.query(`DROP TABLE IF EXISTS ${prefix}${t}`);
		};
		const key = `hashtextextended('kairo.schema:' || $1::text, 0)`;
		try {
			await drop();
			await pool.query(OLD_SCHEMA(prefix, 'BYTEA'));
			const holder = await pool.connect();
			await holder.query(`SELECT pg_advisory_lock(${key})`, [prefix]);
			let settled = false;
			const init = new PostgresStore(pool, { prefix }).init().finally(() => (settled = true));
			const deadline = Date.now() + 10_000;
			for (;;) {
				const { rows } = await pool.query(
					`SELECT count(*)::int AS n FROM pg_locks l, (SELECT ${key} AS k) h
					 WHERE l.locktype = 'advisory' AND NOT l.granted AND l.objsubid = 1
					 AND l.classid::bigint = ((h.k >> 32) & 4294967295) AND l.objid::bigint = (h.k & 4294967295)`,
					[prefix],
				);
				if (rows[0].n > 0) break;
				assert.equal(settled, false, 'initialization did not wait for the schema lock');
				assert.ok(Date.now() < deadline, 'initialization never asked for the schema lock');
				await new Promise((r) => setTimeout(r, 5));
			}
			await holder.query(`SELECT pg_advisory_unlock(${key})`, [prefix]);
			holder.release();
			await init;

			await Promise.all(Array.from({ length: 4 }, () => new PostgresStore(pool, { prefix }).init()));
			const { rows } = await pool.query(
				`SELECT count(*)::int AS n FROM information_schema.columns WHERE table_name = $1 AND column_name = ANY($2)`,
				[`${prefix}run`, ADDED],
			);
			assert.equal(rows[0].n, ADDED.length);
		} finally {
			await drop();
			await pool.end();
		}
	});
});
