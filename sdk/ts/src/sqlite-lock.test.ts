import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

import { SQLiteStore } from './store.ts';

// Another process holds the write lock for a moment, then commits.
const HOLDER = `
const { DatabaseSync } = require('node:sqlite');
const db = new DatabaseSync(process.env.KAIRO_TEST_DB);
db.exec('PRAGMA journal_mode=WAL');
db.exec('CREATE TABLE IF NOT EXISTS hold (x INTEGER)');
db.exec('BEGIN IMMEDIATE');
db.exec('INSERT INTO hold VALUES (1)');
process.stdout.write('locked\\n');
setTimeout(() => { db.exec('COMMIT'); db.close(); }, 300);
`;

test('SQLiteStore waits for another process holding the write lock', async () => {
	const dir = mkdtempSync(join(tmpdir(), 'kairo-lock-'));
	const path = join(dir, 'kairo.db');
	const holder = spawn(process.execPath, ['--no-warnings', '-e', HOLDER], {
		env: { ...process.env, KAIRO_TEST_DB: path },
		stdio: ['ignore', 'pipe', 'inherit'],
	});
	const exited = new Promise<number | null>((resolve) => holder.on('exit', resolve));
	const store = new SQLiteStore(path);
	try {
		await new Promise<void>((resolve, reject) => {
			holder.stdout!.on('data', (b: Buffer) => b.toString().includes('locked') && resolve());
			holder.on('exit', () => reject(new Error('the holder exited before taking the lock')));
		});
		// Its schema setup and a write both need the lock the holder has.
		await store.init();
		await store.withRun('lock-1', () => ({ events: [], result: undefined }));
		assert.equal(await exited, 0);
	} finally {
		holder.kill();
		await store.close();
		rmSync(dir, { recursive: true, force: true });
	}
});
