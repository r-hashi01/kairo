import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

import { SQLiteStore } from './store.ts';

test('SQLite notification is published after commit', async () => {
	const dir = mkdtempSync(join(tmpdir(), 'kairo-notify-'));
	const path = join(dir, 'kairo.db');
	const a = new SQLiteStore(path, { notify: true });
	const b = new SQLiteStore(path, { notify: true });
	try {
		await a.init();
		await b.init();
		const stopA = await a.listen!(() => {});
		let resolve!: (id: string) => void;
		const heard = new Promise<string>((r) => { resolve = r; });
		const stopB = await b.listen!(resolve);
		await a.withRun('n-1', () => ({ events: [], notify: true, result: undefined }));
		assert.equal(await Promise.race([heard, new Promise((_, reject) => setTimeout(() => reject(new Error('notification timeout')), 2000))]), 'n-1');
		await stopB();
		await stopA();
	} finally {
		await a.close();
		await b.close();
		rmSync(dir, { recursive: true, force: true });
	}
});
