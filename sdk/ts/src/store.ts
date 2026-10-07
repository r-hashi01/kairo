// Where the embedded runtime keeps runs (ADR 0051): per run its state (the
// core's encoding) and its events (appended only), and the timers armed.
// Applying an event is one transaction: lock the run's row, then append
// the events, replace the state and arm or disarm timers.

import type { CoreEvent } from './core.ts';

export interface RunRow {
	id: string;
	plan: string;
	hash: string;
	state: Uint8Array;
	/** The run's input (JSON). */
	input: string | null;
	status: string;
	output: string | null; // JSON
	error: string | null;
	/** Events recorded so far (the next one gets this number). */
	seq: number;
	createdAt: number;
	updatedAt: number;
}

/** A step dispatched to a process, held while it runs (ADR 0051). */
export interface LeaseRow {
	run: string;
	act: number;
	attempt: number;
	owner: string;
	/** Unix ms: past it, the owner is taken to have stopped. */
	until: number;
}

export interface TimerRow {
	run: string;
	timer: number;
	act: number;
	at: number;
}

/** What a transaction on a run writes; computed from the row as read. */
export interface Changes<T> {
	events: CoreEvent[];
	row?: RunRow;
	setTimers?: TimerRow[];
	deleteTimers?: number[];
	/** Drop every timer and lease of the run (it ended). */
	clearTimers?: boolean;
	setLeases?: LeaseRow[];
	/** Steps (acts) whose leases end: their outcome is in. */
	endLeases?: number[];
	/** The run settled (finished, or stopped for review): tell the listeners at commit. */
	notify?: boolean;
	result: T;
}

export interface Store {
	init(): Promise<void>;
	/** Locks run id's row (absent: undefined), computes the changes, writes them. */
	withRun<T>(id: string, fn: (row: RunRow | undefined) => Changes<T>): Promise<T>;
	get(id: string): Promise<RunRow | undefined>;
	/** Timers due at now, oldest first. */
	dueTimers(now: number, limit: number): Promise<TimerRow[]>;
	/** Leases past their time at now: steps whose process stopped. */
	expiredLeases(now: number, limit: number): Promise<LeaseRow[]>;
	/** Extends owner's leases to until. */
	renewLeases(owner: string, until: number): Promise<void>;
	/** Ends owner's leases now (a process that knows its earlier self stopped). */
	expireLeases(owner: string, now: number): Promise<void>;
	/**
	 * Calls settled with the id of each run that settles in any process
	 * (Changes.notify), until the returned function is called. Absent:
	 * runs settled in other processes are not heard of.
	 */
	listen?(settled: (runId: string) => void): Promise<() => Promise<void>>;
	close(): Promise<void>;
}

const DDL = (p: string, blob: string, big: string) => [
	`CREATE TABLE IF NOT EXISTS ${p}run (id TEXT PRIMARY KEY, plan TEXT NOT NULL, hash TEXT NOT NULL, state ${blob} NOT NULL,
	  input TEXT, status TEXT NOT NULL, output TEXT, error TEXT, seq ${big} NOT NULL, created_at ${big} NOT NULL, updated_at ${big} NOT NULL)`,
	`CREATE TABLE IF NOT EXISTS ${p}event (run TEXT NOT NULL, seq ${big} NOT NULL, body TEXT NOT NULL, PRIMARY KEY (run, seq))`,
	`CREATE TABLE IF NOT EXISTS ${p}timer (run TEXT NOT NULL, timer ${big} NOT NULL, act ${big} NOT NULL, at ${big} NOT NULL, PRIMARY KEY (run, timer))`,
	`CREATE INDEX IF NOT EXISTS ${p}timer_at ON ${p}timer (at)`,
	`CREATE TABLE IF NOT EXISTS ${p}lease (run TEXT NOT NULL, act ${big} NOT NULL, attempt ${big} NOT NULL, owner TEXT NOT NULL,
	  until ${big} NOT NULL, PRIMARY KEY (run, act))`,
	`CREATE INDEX IF NOT EXISTS ${p}lease_until ON ${p}lease (until)`,
	`CREATE INDEX IF NOT EXISTS ${p}lease_owner ON ${p}lease (owner)`,
];

function checkPrefix(p: string): string {
	if (!/^[a-z][a-z0-9_]{0,30}$/.test(p)) throw new Error(`table prefix ${p}: letters, digits and _ only`);
	return p;
}

/** SQLite through node:sqlite: a file, or ":memory:" (tests). */
export class SQLiteStore implements Store {
	private db: any;
	private readonly path: string;
	private readonly p: string;
	private readonly sync: string;
	private q: Record<string, any> = {};

	constructor(path: string, opts: { prefix?: string; synchronous?: 'FULL' | 'NORMAL' } = {}) {
		this.path = path;
		this.p = checkPrefix(opts.prefix ?? 'kairo_');
		this.sync = opts.synchronous ?? 'FULL';
	}

	async init(): Promise<void> {
		// node:sqlite (Node 22.5+), loaded only when used.
		const sqlite = 'node:sqlite';
		const { DatabaseSync } = (await import(sqlite)) as any;
		this.db = new DatabaseSync(this.path);
		if (this.path !== ':memory:') this.db.exec('PRAGMA journal_mode=WAL');
		this.db.exec(`PRAGMA synchronous=${this.sync}`);
		for (const q of DDL(this.p, 'BLOB', 'INTEGER')) this.db.exec(q);
		const p = this.p;
		this.q = {
			get: this.db.prepare(`SELECT * FROM ${p}run WHERE id = ?`),
			put: this.db.prepare(`INSERT INTO ${p}run (id, plan, hash, state, input, status, output, error, seq, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET state = excluded.state, status = excluded.status,
				output = excluded.output, error = excluded.error, seq = excluded.seq, updated_at = excluded.updated_at`),
			event: this.db.prepare(`INSERT INTO ${p}event (run, seq, body) VALUES (?, ?, ?)`),
			setTimer: this.db.prepare(`INSERT INTO ${p}timer (run, timer, act, at) VALUES (?, ?, ?, ?) ON CONFLICT (run, timer) DO UPDATE SET at = excluded.at`),
			delTimer: this.db.prepare(`DELETE FROM ${p}timer WHERE run = ? AND timer = ?`),
			clearTimers: this.db.prepare(`DELETE FROM ${p}timer WHERE run = ?`),
			due: this.db.prepare(`SELECT run, timer, act, at FROM ${p}timer WHERE at <= ? ORDER BY at LIMIT ?`),
			setLease: this.db.prepare(`INSERT INTO ${p}lease (run, act, attempt, owner, until) VALUES (?, ?, ?, ?, ?)
				ON CONFLICT (run, act) DO UPDATE SET attempt = excluded.attempt, owner = excluded.owner, until = excluded.until`),
			endLease: this.db.prepare(`DELETE FROM ${p}lease WHERE run = ? AND act = ?`),
			clearLeases: this.db.prepare(`DELETE FROM ${p}lease WHERE run = ?`),
			expired: this.db.prepare(`SELECT run, act, attempt, owner, until FROM ${p}lease WHERE until < ? ORDER BY until LIMIT ?`),
			renew: this.db.prepare(`UPDATE ${p}lease SET until = ? WHERE owner = ?`),
			expire: this.db.prepare(`UPDATE ${p}lease SET until = ? WHERE owner = ?`),
		};
	}

	private row(r: any): RunRow | undefined {
		if (!r) return undefined;
		return { id: r.id, plan: r.plan, hash: r.hash, state: new Uint8Array(r.state), input: r.input ?? null, status: r.status, output: r.output, error: r.error,
			seq: Number(r.seq), createdAt: Number(r.created_at), updatedAt: Number(r.updated_at) };
	}

	async withRun<T>(id: string, fn: (row: RunRow | undefined) => Changes<T>): Promise<T> {
		// Synchronous from BEGIN to COMMIT: nothing else runs on this
		// connection meanwhile.
		this.db.exec('BEGIN IMMEDIATE');
		try {
			const before = this.row(this.q.get.get(id));
			const c = fn(before);
			let seq = before?.seq ?? 0;
			for (const ev of c.events) this.q.event.run(id, seq++, JSON.stringify(ev));
			if (c.row) {
				const r = c.row;
				this.q.put.run(r.id, r.plan, r.hash, r.state, r.input, r.status, r.output, r.error, seq, r.createdAt, r.updatedAt);
			}
			if (c.clearTimers) {
				this.q.clearTimers.run(id);
				this.q.clearLeases.run(id);
			}
			for (const t of c.deleteTimers ?? []) this.q.delTimer.run(id, t);
			for (const t of c.setTimers ?? []) this.q.setTimer.run(t.run, t.timer, t.act, t.at);
			for (const a of c.endLeases ?? []) this.q.endLease.run(id, a);
			for (const l of c.setLeases ?? []) this.q.setLease.run(l.run, l.act, l.attempt, l.owner, l.until);
			this.db.exec('COMMIT');
			return c.result;
		} catch (e) {
			this.db.exec('ROLLBACK');
			throw e;
		}
	}

	async get(id: string): Promise<RunRow | undefined> {
		return this.row(this.q.get.get(id));
	}

	async dueTimers(now: number, limit: number): Promise<TimerRow[]> {
		return this.q.due.all(now, limit).map((t: any) => ({ run: t.run, timer: Number(t.timer), act: Number(t.act), at: Number(t.at) }));
	}

	async expiredLeases(now: number, limit: number): Promise<LeaseRow[]> {
		return this.q.expired.all(now, limit).map(lease);
	}

	async renewLeases(owner: string, until: number): Promise<void> {
		this.q.renew.run(until, owner);
	}

	async expireLeases(owner: string, now: number): Promise<void> {
		this.q.expire.run(now - 1, owner);
	}

	async close(): Promise<void> {
		this.db?.close();
	}
}

function lease(l: any): LeaseRow {
	return { run: l.run, act: Number(l.act), attempt: Number(l.attempt), owner: l.owner, until: Number(l.until) };
}

/** A client of the pool (node-postgres's pg.Client fits). */
export interface PgClient {
	query(text: string, params?: unknown[]): Promise<{ rows: any[] }>;
	release(): void;
	on?(event: 'notification', listener: (msg: { channel: string; payload?: string }) => void): unknown;
}

/** The little of a PostgreSQL pool the store uses (node-postgres's pg.Pool fits). */
export interface PgPool {
	connect(): Promise<PgClient>;
	query(text: string, params?: unknown[]): Promise<{ rows: any[] }>;
}

/** PostgreSQL through a pool the application gives (no dependency here). */
export class PostgresStore implements Store {
	private readonly pool: PgPool;
	private readonly p: string;

	constructor(pool: PgPool, opts: { prefix?: string } = {}) {
		this.pool = pool;
		this.p = checkPrefix(opts.prefix ?? 'kairo_');
	}

	async init(): Promise<void> {
		for (const q of DDL(this.p, 'BYTEA', 'BIGINT')) await this.pool.query(q);
	}

	private row(r: any): RunRow | undefined {
		if (!r) return undefined;
		return { id: r.id, plan: r.plan, hash: r.hash, state: new Uint8Array(r.state), input: r.input ?? null, status: r.status, output: r.output, error: r.error,
			seq: Number(r.seq), createdAt: Number(r.created_at), updatedAt: Number(r.updated_at) };
	}

	async withRun<T>(id: string, fn: (row: RunRow | undefined) => Changes<T>): Promise<T> {
		const c = await this.pool.connect();
		const p = this.p;
		try {
			await c.query('BEGIN');
			const before = this.row((await c.query(`SELECT * FROM ${p}run WHERE id = $1 FOR UPDATE`, [id])).rows[0]);
			const ch = fn(before);
			let seq = before?.seq ?? 0;
			for (const ev of ch.events) await c.query(`INSERT INTO ${p}event (run, seq, body) VALUES ($1, $2, $3)`, [id, seq++, JSON.stringify(ev)]);
			if (ch.row) {
				const r = ch.row;
				// A new run's row: two submissions racing for one id insert it
				// once; the loser's insert fails and its transaction rolls back.
				await c.query(`INSERT INTO ${p}run (id, plan, hash, state, input, status, output, error, seq, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (id) DO UPDATE SET state = excluded.state,
					status = excluded.status, output = excluded.output, error = excluded.error, seq = excluded.seq, updated_at = excluded.updated_at`,
					[r.id, r.plan, r.hash, Buffer.from(r.state), r.input, r.status, r.output, r.error, seq, r.createdAt, r.updatedAt]);
			}
			if (ch.clearTimers) {
				await c.query(`DELETE FROM ${p}timer WHERE run = $1`, [id]);
				await c.query(`DELETE FROM ${p}lease WHERE run = $1`, [id]);
			}
			for (const a of ch.endLeases ?? []) await c.query(`DELETE FROM ${p}lease WHERE run = $1 AND act = $2`, [id, a]);
			for (const l of ch.setLeases ?? [])
				await c.query(`INSERT INTO ${p}lease (run, act, attempt, owner, until) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (run, act) DO UPDATE SET attempt = excluded.attempt, owner = excluded.owner, until = excluded.until`,
					[l.run, l.act, l.attempt, l.owner, l.until]);
			for (const t of ch.deleteTimers ?? []) await c.query(`DELETE FROM ${p}timer WHERE run = $1 AND timer = $2`, [id, t]);
			for (const t of ch.setTimers ?? [])
				await c.query(`INSERT INTO ${p}timer (run, timer, act, at) VALUES ($1, $2, $3, $4) ON CONFLICT (run, timer) DO UPDATE SET at = excluded.at`,
					[t.run, t.timer, t.act, t.at]);
			if (ch.notify) await c.query('SELECT pg_notify($1, $2)', [`${p}settled`, id]);
			await c.query('COMMIT');
			return ch.result;
		} catch (e) {
			await c.query('ROLLBACK').catch(() => {});
			throw e;
		} finally {
			c.release();
		}
	}

	async get(id: string): Promise<RunRow | undefined> {
		return this.row((await this.pool.query(`SELECT * FROM ${this.p}run WHERE id = $1`, [id])).rows[0]);
	}

	async dueTimers(now: number, limit: number): Promise<TimerRow[]> {
		const r = await this.pool.query(`SELECT run, timer, act, at FROM ${this.p}timer WHERE at <= $1 ORDER BY at LIMIT $2`, [now, limit]);
		return r.rows.map((t: any) => ({ run: t.run, timer: Number(t.timer), act: Number(t.act), at: Number(t.at) }));
	}

	async expiredLeases(now: number, limit: number): Promise<LeaseRow[]> {
		const r = await this.pool.query(`SELECT run, act, attempt, owner, until FROM ${this.p}lease WHERE until < $1 ORDER BY until LIMIT $2`, [now, limit]);
		return r.rows.map(lease);
	}

	async renewLeases(owner: string, until: number): Promise<void> {
		await this.pool.query(`UPDATE ${this.p}lease SET until = $1 WHERE owner = $2`, [until, owner]);
	}

	async expireLeases(owner: string, now: number): Promise<void> {
		await this.pool.query(`UPDATE ${this.p}lease SET until = $1 WHERE owner = $2`, [now - 1, owner]);
	}

	/** One connection of the pool, kept to LISTEN for settled runs. */
	async listen(settled: (runId: string) => void): Promise<() => Promise<void>> {
		const c = await this.pool.connect();
		if (!c.on) {
			c.release();
			return async () => {};
		}
		c.on('notification', (msg) => {
			if (msg.channel === `${this.p}settled` && msg.payload) settled(msg.payload);
		});
		await c.query(`LISTEN ${this.p}settled`);
		return async () => {
			await c.query(`UNLISTEN ${this.p}settled`).catch(() => {});
			c.release();
		};
	}

	async close(): Promise<void> {}
}
