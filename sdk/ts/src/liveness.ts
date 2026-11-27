// A process's liveness, where the store can tell it (ADR 0086): a mark the
// process holds while it lives, which the database or the OS takes away
// when it ends. Another process takes a gone owner's leases up without
// waiting for them to expire. "Gone" is said only on proof (the mark could
// be taken); on any doubt, the leases expire by time, as before.

import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import type { PgClient, PgPool } from './store.ts';

/** What a store gives to tell owners' liveness (Store.liveness). */
export interface Liveness {
	/** Takes owner's mark, held until close. */
	hold(owner: string): Promise<void>;
	/** Whether owner's mark is provably gone (false on any doubt). */
	gone(owner: string): Promise<boolean>;
	/** Whether this process has lost its own mark (it must stop, ADR 0086). */
	lost(): boolean;
	/** Gives the mark back. */
	close(): Promise<void>;
}

/**
 * SQLite: a lock file of the owner's, beside the database, held in an
 * exclusive transaction for the process's life. SQLite's lock is the OS's,
 * which it takes away when the process ends; the three SDKs take the same
 * lock, so processes of each language see each other.
 */
export class SQLiteLiveness implements Liveness {
	private readonly dir: string;
	private held: any;
	private heldPath = '';

	constructor(dbPath: string) {
		this.dir = `${dbPath}.owners`;
	}

	private file(owner: string): string {
		return join(this.dir, createHash('sha256').update(owner).digest('hex').slice(0, 32) + '.lock');
	}

	private async open(path: string): Promise<any> {
		const sqlite = 'node:sqlite';
		const { DatabaseSync } = (await import(sqlite)) as any;
		return new DatabaseSync(path, { timeout: 0 });
	}

	async hold(owner: string): Promise<void> {
		mkdirSync(this.dir, { recursive: true, mode: 0o700 });
		const db = await this.open(this.file(owner));
		db.exec('BEGIN EXCLUSIVE');
		this.held = db;
		this.heldPath = this.file(owner);
	}

	async gone(owner: string): Promise<boolean> {
		const path = this.file(owner);
		// No mark: an owner that never held one (an older SDK), not proof.
		if (!existsSync(path)) return false;
		let db: any;
		let gone = false;
		try {
			db = await this.open(path);
			db.exec('BEGIN EXCLUSIVE'); // fails while its owner lives
			db.exec('ROLLBACK');
			gone = true;
		} catch {
			return false;
		} finally {
			try {
				db?.close();
			} catch {}
		}
		return gone;
	}

	lost(): boolean {
		return false; // a process that ends loses it with its life
	}

	async close(): Promise<void> {
		if (!this.held) return;
		// Its own file goes with it; one of a process that stopped stays,
		// as a gone owner's (another's is never removed: an owner of that
		// name may be taking it again).
		rmSync(this.heldPath, { force: true });
		try {
			this.held.close();
		} catch {}
		this.held = undefined;
	}
}

/**
 * PostgreSQL: a session advisory lock of the owner's, on a connection of
 * its own, which the server takes away when the session ends. The key is
 * the server's hash of the owner, the same for the three SDKs. The same
 * connection tries others' locks (and gives back one it took).
 */
export class PgLiveness implements Liveness {
	private readonly pool: PgPool;
	private client: PgClient | undefined;
	private owner = '';
	private broken = false;

	constructor(pool: PgPool) {
		this.pool = pool;
	}

	async hold(owner: string): Promise<void> {
		const c = await this.pool.connect();
		(c as unknown as { on?: (e: string, f: () => void) => void }).on?.('error', () => (this.broken = true));
		await c.query("SELECT pg_advisory_lock(hashtextextended('kairo.owner:' || $1, 0))", [owner]);
		this.client = c;
		this.owner = owner;
	}

	async gone(owner: string): Promise<boolean> {
		const c = this.client;
		if (!c || this.broken || owner === this.owner) return false;
		try {
			// Advisory locks nest in a session: one with our own key (a hash
			// collision) would be taken here, so it is never taken as gone.
			const r = await c.query(
				"SELECT hashtextextended('kairo.owner:' || $1, 0) = hashtextextended('kairo.owner:' || $2, 0) AS same, " +
					"pg_try_advisory_lock(hashtextextended('kairo.owner:' || $1, 0)) AS took",
				[owner, this.owner],
			);
			const { same, took } = r.rows[0];
			if (took) await c.query("SELECT pg_advisory_unlock(hashtextextended('kairo.owner:' || $1, 0))", [owner]);
			return !!took && !same;
		} catch {
			this.broken = true;
			return false;
		}
	}

	lost(): boolean {
		return this.broken;
	}

	async close(): Promise<void> {
		const c = this.client;
		this.client = undefined;
		if (!c) return;
		try {
			if (!this.broken) await c.query("SELECT pg_advisory_unlock(hashtextextended('kairo.owner:' || $1, 0))", [this.owner]);
		} catch {}
		c.release();
	}
}
