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
	/** The run that made this one (a workflow, of its calls): removed with it (ADR 0054). */
	parent: string | null;
	/** The workflow of a workflow run (null for a call), and what it was started with (JSON; ADR 0059). */
	workflow: string | null;
	meta: string | null;
}

/** Root runs for list (ADR 0059): of workflow, in status, created in [since, until), after the run after, at most limit. */
export interface ListFilter {
	workflow?: string;
	status?: string;
	since?: number;
	until?: number;
	after?: string;
	limit?: number;
}

/**
 * A step dispatched to a process, held while it runs (ADR 0051). Act 0 (no
 * step has it) is a workflow's drive lease (ADR 0059): the process that
 * runs the workflow's function, with a token in attempt.
 */
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
	/**
	 * When something is next to do (ADR 0053): the earliest timer, or the
	 * earliest lease to expire; null if neither.
	 */
	nextWake(): Promise<number | null>;
	/**
	 * Removes finished trees of runs (ADR 0054): a run with no parent (or
	 * whose parent is gone) that finished before cutoff, with every run
	 * under it, finished or not, and their events, timers and leases. One
	 * transaction per tree; at most limit trees. Returns how many.
	 */
	removeFinished(cutoff: number, limit: number): Promise<number>;
	/** Extends owner's leases to until. */
	renewLeases(owner: string, until: number): Promise<void>;
	/**
	 * Hands a step's lease to l.owner until l.until (ADR 0052: the step runs
	 * elsewhere). Nothing if the lease is gone: its outcome is in already.
	 */
	handOver(l: LeaseRow): Promise<void>;
	/** Ends owner's leases now (a process that knows its earlier self stopped). */
	expireLeases(owner: string, now: number): Promise<void>;
	/**
	 * Calls settled with the id of each run that settles in any process
	 * (Changes.notify), until the returned function is called. Absent:
	 * runs settled in other processes are not heard of.
	 */
	listen?(settled: (runId: string) => void): Promise<() => Promise<void>>;
	/** Sets the drive lease l (act 0) unless another owner holds it unexpired at now; whether it did (ADR 0059). */
	claimDrive(l: LeaseRow, now: number): Promise<boolean>;
	/**
	 * Ends run's drive lease held by owner with token: removed, or (resume)
	 * left expired and ownerless at now, for a tick to take up.
	 */
	endDrive(run: string, owner: string, token: number, now: number, resume: boolean): Promise<void>;
	/** Root runs (no parent) in creation order (ADR 0059). */
	list(f: ListFilter): Promise<RunRow[]>;
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

/** Columns added to tables made before them: parent (ADR 0054), workflow and meta (ADR 0059). */
const ADDED = ['parent', 'workflow', 'meta'];

/** After the tables (and the columns added to older ones): the indexes for removal (ADR 0054) and listing (ADR 0059). */
const DDL_REMOVAL = (p: string) => [
	`CREATE INDEX IF NOT EXISTS ${p}run_parent ON ${p}run (parent)`,
	`CREATE INDEX IF NOT EXISTS ${p}run_done ON ${p}run (updated_at) WHERE status IN ${DONE_SQL}`,
	`CREATE INDEX IF NOT EXISTS ${p}run_roots ON ${p}run (created_at, id) WHERE parent IS NULL`,
];

const RUN_COLS = 'id, plan, hash, state, input, status, output, error, seq, created_at, updated_at, parent, workflow, meta';

/** A new run's row, or the changes to it: workflow and meta are set once (ADR 0059). */
const PUT = (p: string, ph: (i: number) => string) =>
	`INSERT INTO ${p}run (${RUN_COLS}) VALUES (${Array.from({ length: 14 }, (_, i) => ph(i + 1)).join(', ')})
	 ON CONFLICT (id) DO UPDATE SET state = excluded.state, status = excluded.status, output = excluded.output, error = excluded.error,
	 seq = excluded.seq, updated_at = excluded.updated_at`;

/** One statement: of two processes claiming at once, one wins. */
const CLAIM = (p: string, ph: (i: number) => string) =>
	`INSERT INTO ${p}lease (run, act, attempt, owner, until) VALUES (${ph(1)}, 0, ${ph(2)}, ${ph(3)}, ${ph(4)})
	 ON CONFLICT (run, act) DO UPDATE SET attempt = excluded.attempt, owner = excluded.owner, until = excluded.until
	 WHERE ${p}lease.owner = excluded.owner OR ${p}lease.until < ${ph(5)}`;

const DRIVE_WHERE = (ph: (i: number) => string, from: number) => `WHERE run = ${ph(from)} AND act = 0 AND owner = ${ph(from + 1)} AND attempt = ${ph(from + 2)}`;

/** The query of list, and its parameters. */
function listQuery(p: string, f: ListFilter, ph: (i: number) => string): [string, unknown[]] {
	let q = `SELECT ${RUN_COLS} FROM ${p}run WHERE parent IS NULL`;
	const args: unknown[] = [];
	const add = (cond: (x: string) => string, v: unknown) => {
		args.push(v);
		q += ` AND ${cond(ph(args.length))}`;
	};
	if (f.workflow) add((x) => `workflow = ${x}`, f.workflow);
	if (f.status) add((x) => `status = ${x}`, f.status);
	if (f.since) add((x) => `created_at >= ${x}`, f.since);
	if (f.until) add((x) => `created_at < ${x}`, f.until);
	if (f.after) add((x) => `(created_at, id) > (SELECT created_at, id FROM ${p}run WHERE id = ${x})`, f.after);
	q += ' ORDER BY created_at, id';
	if (f.limit && f.limit > 0) {
		args.push(f.limit);
		q += ` LIMIT ${ph(args.length)}`;
	}
	return [q, args];
}

const qmark = () => '?';
const dollar = (i: number) => `$${i}`;

/** A run row as the database gives it. */
function runRow(r: any): RunRow | undefined {
	if (!r) return undefined;
	return { id: r.id, plan: r.plan, hash: r.hash, state: new Uint8Array(r.state), input: r.input ?? null, status: r.status, output: r.output, error: r.error,
		seq: Number(r.seq), createdAt: Number(r.created_at), updatedAt: Number(r.updated_at), parent: r.parent ?? null,
		workflow: r.workflow ?? null, meta: r.meta ?? null };
}

const DONE_SQL = `('completed', 'failed', 'cancelled')`;

/** Roots of finished trees: no parent, or its parent gone (oldest first). */
const ROOTS = (p: string, at: string, lim: string) =>
	`SELECT id FROM ${p}run r WHERE status IN ${DONE_SQL} AND updated_at < ${at}
	 AND (parent IS NULL OR NOT EXISTS (SELECT 1 FROM ${p}run x WHERE x.id = r.parent)) ORDER BY updated_at LIMIT ${lim}`;

/** Whether run id is (still) the root of a finished tree. */
const IS_ROOT = (p: string, id: string, at: string) =>
	`SELECT 1 AS ok FROM ${p}run r WHERE id = ${id} AND status IN ${DONE_SQL} AND updated_at < ${at}
	 AND (parent IS NULL OR NOT EXISTS (SELECT 1 FROM ${p}run x WHERE x.id = r.parent))`;

/** The ids of a tree: its root and every run under it. */
const TREE = (p: string, root: string) =>
	`WITH RECURSIVE tree(id) AS (SELECT ${root} UNION SELECT r.id FROM ${p}run r JOIN tree t ON r.parent = t.id) SELECT id FROM tree`;

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
		const cols = this.db.prepare(`PRAGMA table_info(${this.p}run)`).all() as { name: string }[];
		for (const col of ADDED) if (!cols.some((c) => c.name === col)) this.db.exec(`ALTER TABLE ${this.p}run ADD COLUMN ${col} TEXT`);
		for (const q of DDL_REMOVAL(this.p)) this.db.exec(q);
		const p = this.p;
		this.q = {
			get: this.db.prepare(`SELECT ${RUN_COLS} FROM ${p}run WHERE id = ?`),
			put: this.db.prepare(PUT(p, qmark)),
			claim: this.db.prepare(CLAIM(p, qmark)),
			endDrive: this.db.prepare(`DELETE FROM ${p}lease ${DRIVE_WHERE(qmark, 1)}`),
			resumeDrive: this.db.prepare(`UPDATE ${p}lease SET until = ?, owner = '' ${DRIVE_WHERE(qmark, 2)}`),
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
			nextTimer: this.db.prepare(`SELECT MIN(at) AS t FROM ${p}timer`),
			nextLease: this.db.prepare(`SELECT MIN(until) AS t FROM ${p}lease`),
			handOver: this.db.prepare(`UPDATE ${p}lease SET owner = ?, until = ? WHERE run = ? AND act = ? AND attempt = ?`),
			roots: this.db.prepare(ROOTS(p, '?', '?')),
			isRoot: this.db.prepare(IS_ROOT(p, '?', '?')),
			rmEvents: this.db.prepare(`DELETE FROM ${p}event WHERE run IN (${TREE(p, '?')})`),
			rmTimers: this.db.prepare(`DELETE FROM ${p}timer WHERE run IN (${TREE(p, '?')})`),
			rmLeases: this.db.prepare(`DELETE FROM ${p}lease WHERE run IN (${TREE(p, '?')})`),
			rmRuns: this.db.prepare(`DELETE FROM ${p}run WHERE id IN (${TREE(p, '?')})`),
		};
	}

	private row(r: any): RunRow | undefined {
		return runRow(r);
	}

	async removeFinished(cutoff: number, limit: number): Promise<number> {
		let n = 0;
		for (const { id } of this.q.roots.all(cutoff, limit) as { id: string }[]) {
			this.db.exec('BEGIN IMMEDIATE');
			try {
				// Still a finished root (not resubmitted, not removed by another process)?
				if (this.q.isRoot.get(id, cutoff)) {
					// The tree's rows first, the runs (which the tree is found by) last.
					for (const q of [this.q.rmEvents, this.q.rmTimers, this.q.rmLeases, this.q.rmRuns]) q.run(id);
					n++;
				}
				this.db.exec('COMMIT');
			} catch (e) {
				this.db.exec('ROLLBACK');
				throw e;
			}
		}
		return n;
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
				this.q.put.run(r.id, r.plan, r.hash, r.state, r.input, r.status, r.output, r.error, seq, r.createdAt, r.updatedAt, r.parent, r.workflow, r.meta);
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

	async nextWake(): Promise<number | null> {
		return earliest((this.q.nextTimer.get() as any)?.t, (this.q.nextLease.get() as any)?.t);
	}

	async handOver(l: LeaseRow): Promise<void> {
		this.q.handOver.run(l.owner, l.until, l.run, l.act, l.attempt);
	}

	async expireLeases(owner: string, now: number): Promise<void> {
		this.q.expire.run(now - 1, owner);
	}

	async claimDrive(l: LeaseRow, now: number): Promise<boolean> {
		return Number(this.q.claim.run(l.run, l.attempt, l.owner, l.until, now).changes) > 0;
	}

	async endDrive(run: string, owner: string, token: number, now: number, resume: boolean): Promise<void> {
		if (resume) this.q.resumeDrive.run(now - 1, run, owner, token);
		else this.q.endDrive.run(run, owner, token);
	}

	async list(f: ListFilter): Promise<RunRow[]> {
		const [q, args] = listQuery(this.p, f, qmark);
		return this.db.prepare(q).all(...args).map((r: any) => runRow(r)!);
	}

	async close(): Promise<void> {
		this.db?.close();
	}
}

/** The earlier of two times as the database gives them (null, number, bigint or numeric text). */
function earliest(a: unknown, b: unknown): number | null {
	const ts = [a, b].filter((v) => v !== null && v !== undefined).map(Number);
	return ts.length ? Math.min(...ts) : null;
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
		for (const col of ADDED) await this.pool.query(`ALTER TABLE ${this.p}run ADD COLUMN IF NOT EXISTS ${col} TEXT`);
		for (const q of DDL_REMOVAL(this.p)) await this.pool.query(q);
	}

	async removeFinished(cutoff: number, limit: number): Promise<number> {
		const p = this.p;
		const roots = await this.pool.query(ROOTS(p, '$1', '$2'), [cutoff, limit]);
		let n = 0;
		for (const { id } of roots.rows as { id: string }[]) {
			const c = await this.pool.connect();
			try {
				await c.query('BEGIN');
				// Lock the root, then check it is still a finished root (not
				// resubmitted, not removed by another process).
				await c.query(`SELECT 1 FROM ${p}run WHERE id = $1 FOR UPDATE`, [id]);
				const still = await c.query(IS_ROOT(p, '$1', '$2'), [id, cutoff]);
				if (still.rows.length) {
					for (const t of ['event', 'timer', 'lease']) await c.query(`DELETE FROM ${p}${t} WHERE run IN (${TREE(p, '$1::text')})`, [id]);
					await c.query(`DELETE FROM ${p}run WHERE id IN (${TREE(p, '$1::text')})`, [id]);
					n++;
				}
				await c.query('COMMIT');
			} catch (e) {
				await c.query('ROLLBACK').catch(() => {});
				throw e;
			} finally {
				c.release();
			}
		}
		return n;
	}

	private row(r: any): RunRow | undefined {
		return runRow(r);
	}

	async withRun<T>(id: string, fn: (row: RunRow | undefined) => Changes<T>): Promise<T> {
		const c = await this.pool.connect();
		const p = this.p;
		try {
			await c.query('BEGIN');
			const before = this.row((await c.query(`SELECT ${RUN_COLS} FROM ${p}run WHERE id = $1 FOR UPDATE`, [id])).rows[0]);
			const ch = fn(before);
			let seq = before?.seq ?? 0;
			for (const ev of ch.events) await c.query(`INSERT INTO ${p}event (run, seq, body) VALUES ($1, $2, $3)`, [id, seq++, JSON.stringify(ev)]);
			if (ch.row) {
				const r = ch.row;
				// A new run's row: two submissions racing for one id insert it
				// once; the loser's insert fails and its transaction rolls back.
				await c.query(PUT(p, dollar),
					[r.id, r.plan, r.hash, Buffer.from(r.state), r.input, r.status, r.output, r.error, seq, r.createdAt, r.updatedAt, r.parent, r.workflow, r.meta]);
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
		return this.row((await this.pool.query(`SELECT ${RUN_COLS} FROM ${this.p}run WHERE id = $1`, [id])).rows[0]);
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

	async claimDrive(l: LeaseRow, now: number): Promise<boolean> {
		const r = await this.pool.query(`${CLAIM(this.p, dollar)} RETURNING run`, [l.run, l.attempt, l.owner, l.until, now]);
		return r.rows.length > 0;
	}

	async endDrive(run: string, owner: string, token: number, now: number, resume: boolean): Promise<void> {
		if (resume) await this.pool.query(`UPDATE ${this.p}lease SET until = $1, owner = '' ${DRIVE_WHERE(dollar, 2)}`, [now - 1, run, owner, token]);
		else await this.pool.query(`DELETE FROM ${this.p}lease ${DRIVE_WHERE(dollar, 1)}`, [run, owner, token]);
	}

	async list(f: ListFilter): Promise<RunRow[]> {
		const [q, args] = listQuery(this.p, f, dollar);
		return (await this.pool.query(q, args)).rows.map((r: any) => runRow(r)!);
	}

	async nextWake(): Promise<number | null> {
		const t = await this.pool.query(`SELECT MIN(at) AS t FROM ${this.p}timer`);
		const l = await this.pool.query(`SELECT MIN(until) AS t FROM ${this.p}lease`);
		return earliest(t.rows[0]?.t, l.rows[0]?.t);
	}

	async handOver(l: LeaseRow): Promise<void> {
		await this.pool.query(`UPDATE ${this.p}lease SET owner = $1, until = $2 WHERE run = $3 AND act = $4 AND attempt = $5`, [l.owner, l.until, l.run, l.act, l.attempt]);
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
