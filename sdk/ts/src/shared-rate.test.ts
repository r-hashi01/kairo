import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { SQLiteStore } from './store.ts';
import { Kairo } from './workflow.ts';

test('shared limits require a capable embedded Store', async () => {
	const k = new Kairo();
	k.defineAction('shared', { effect: 'unprotected', rate: 60, sharedLimits: true, handler: async () => null });
	await assert.rejects(k.start(), /embedded runtime with a shared-rate Store/);
});

test('SQLite shares request, actual-weight and Retry-After state', async () => {
	const dir = await mkdtemp(join(tmpdir(), 'kairo-rate-'));
	const path = join(dir, 'rate.db');
	const a = new SQLiteStore(path);
	const b = new SQLiteStore(path);
	try {
		await a.init();
		await b.init();
		assert.equal(await a.admitDestination({ destination: 'provider', requestsPerMinute: 600 }), 0);
		assert.ok(await b.admitDestination({ destination: 'provider', requestsPerMinute: 600 }) > 0);

		await a.withRun('missing', () => ({ events: [], sharedRates: [{ destination: 'weighted', weightPerMinute: 60, weight: 11 }], result: undefined }));
		assert.ok(await b.admitDestination({ destination: 'weighted', weightPerMinute: 60 }) >= 900);

		await a.withRun('missing', () => ({ events: [], sharedRates: [{ destination: 'paused', retryAfter: 1000 }], result: undefined }));
		assert.ok(await b.admitDestination({ destination: 'paused' }) >= 900);
	} finally {
		await a.close();
		await b.close();
		await rm(dir, { recursive: true, force: true });
	}
});
