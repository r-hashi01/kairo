import assert from 'node:assert/strict';
import test from 'node:test';
import { SQLiteStore, type RunRow } from './store.ts';

test('SQLite orders same-millisecond runs by store creation', async () => {
	const store = new SQLiteStore(':memory:');
	await store.init();
	const put = async (id: string, parent: string | null, workflow: string | null) => {
		const row: RunRow = { id, plan: 'p', hash: 'h', state: new Uint8Array([1]), input: null, status: 'running', output: null,
			error: null, seq: 0, createdOrder: 0, createdAt: 42, updatedAt: 42, parent, workflow, meta: null };
		await store.withRun(id, () => ({ events: [], row, result: undefined }));
	};
	await put('z-root', null, 'orders');
	await put('a-root', null, 'orders');
	await put('z-child', 'parent', null);
	await put('a-child', 'parent', null);
	assert.deepEqual((await store.list({ workflow: 'orders', after: 'z-root' })).map((r) => r.id), ['a-root']);
	assert.deepEqual((await store.children('parent')).map((r) => r.id), ['z-child', 'a-child']);
	await store.close();
});
