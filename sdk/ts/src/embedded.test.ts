// The embedded runtime keeps kairo's invariants with a database (ADR 0051).
// Needs go (to build kairo.wasm from this repository).

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { after, before, test } from 'node:test';

import { Embedded } from './embedded.ts';
import { SQLiteStore } from './store.ts';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-embedded-'));
const wasm = join(dir, 'kairo.wasm');
const hasGo = spawnSync('go', ['version']).status === 0;

before(() => {
	if (!hasGo) return;
	const r = spawnSync('go', ['build', '-buildmode=c-shared', '-o', wasm, './cmd/kairo-wasm'], {
		cwd: repo,
		stdio: 'inherit',
		env: { ...process.env, GOOS: 'wasip1', GOARCH: 'wasm' },
	});
	assert.equal(r.status, 0, 'building kairo.wasm');
});

after(() => rmSync(dir, { recursive: true, force: true }));

async function events(path: string, run: string): Promise<Array<{ kind: string; act?: number }>> {
	const sqlite = 'node:sqlite';
	const { DatabaseSync } = (await import(sqlite)) as any;
	const db = new DatabaseSync(path, { readOnly: true });
	try {
		return db.prepare('SELECT body FROM kairo_event WHERE run = ? ORDER BY seq').all(run).map((r: any) => JSON.parse(r.body));
	} finally {
		db.close();
	}
}

test('a real step runs only once its intent is committed; ignored events are not recorded', { skip: !hasGo }, async () => {
	const path = join(dir, 'inv.db');
	const rt = await Embedded.open({ store: new SQLiteStore(path), wasm });
	let seen: Array<{ kind: string; act?: number }> = [];
	let act = 0;
	rt.registerActions([{ action: 'send', effect: 'real' }], async (task) => {
		// What another process would find in the database now.
		act = task.act;
		seen = await events(path, 'r1');
		return { output: { sent: true } };
	});
	rt.registerPlan({ name: 'p', root: { kind: 'seq', nodes: [{ kind: 'step', id: 's', action: 'send' }, { kind: 'wait', id: 'w', signal: 'go' }] } });
	await rt.run('p', null, { runId: 'r1' });
	assert.equal((await rt.run('p', null, { runId: 'r1' })).existing, true, 'a run id is an idempotency key');
	await rt.signal('r1', 'go', 1);
	const r = await rt.wait('r1');
	assert.equal(r.status, 'completed');
	assert.ok(seen.some((e) => e.kind === 'intent' && e.act === act), `the intent was committed before the step ran: ${JSON.stringify(seen)}`);

	const n = (await events(path, 'r1')).length;
	await rt.signal('r1', 'go', 2); // the run is over: ignored
	assert.equal((await events(path, 'r1')).length, n, 'an ignored event is not recorded');
	await rt.close();
});
