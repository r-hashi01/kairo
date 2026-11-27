// kairo.wasm runs on the SDK's own few WASI functions, not node:wasi, which
// Node.js marks experimental and warns of (issue #108). Needs go (to build
// kairo.wasm from this repository).

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { after, before, test } from 'node:test';

const repo = resolve(import.meta.dirname, '../../..');
const dir = mkdtempSync(join(tmpdir(), 'kairo-wasi-'));
const wasm = process.env.KAIRO_WASM || join(dir, 'kairo.wasm');
const hasGo = spawnSync('go', ['version']).status === 0;

before(() => {
	if (!hasGo || process.env.KAIRO_WASM) return;
	const r = spawnSync('go', ['build', '-buildmode=c-shared', '-o', wasm, './cmd/kairo-wasm'], {
		cwd: repo,
		stdio: 'inherit',
		env: { ...process.env, GOOS: 'wasip1', GOARCH: 'wasm' },
	});
	assert.equal(r.status, 0, 'building kairo.wasm');
});

after(() => rmSync(dir, { recursive: true, force: true }));

test('loading the core warns of nothing', { skip: !hasGo }, () => {
	// A process of its own, with warnings on: one that loaded node:wasi
	// would print an ExperimentalWarning to stderr.
	const core = pathToFileURL(join(import.meta.dirname, 'core.ts')).href;
	const script = `const { Core } = await import(${JSON.stringify(core)});
const c = await Core.load(${JSON.stringify(wasm)});
const p = c.compile({ name: 'p', root: { kind: 'step', id: 's', action: 'a' } });
process.stdout.write(typeof p);`;
	const r = spawnSync(process.execPath, ['--input-type=module', '-e', script], { encoding: 'utf8' });
	assert.equal(r.status, 0, r.stderr);
	assert.equal(r.stderr, '');
	assert.equal(r.stdout, 'object');
});
