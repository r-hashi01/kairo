// The public form of recorded events and runs (ADR 0088): only what was
// recorded, each field for the kinds it belongs to.

import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { RunInfo } from './client.ts';
import { progressKind, runEvent } from './progress.ts';

test('runEvent names the kinds and keeps only what each records', () => {
	assert.deepEqual(runEvent({ kind: 'start', at: 1, act: 2, attempt: 9, vars: { x: 1 }, data: 'in' }), { kind: 'started', at: 1 });
	assert.deepEqual(runEvent({ kind: 'intent', at: 2, act: 1, attempt: 1 }), { kind: 'dispatched', at: 2, attempt: 1 });
	assert.deepEqual(
		runEvent({ kind: 'step_err', at: 3, act: 1, attempt: 1, error: 'stopped', error_type: 'process_lost', retryable: true, unknown: true, retry_after_ms: 50 }),
		{ kind: 'failed', at: 3, attempt: 1, error: 'stopped', errorType: 'process_lost', retryable: true, unknown: true, retryAfterMs: 50 },
	);
	assert.deepEqual(runEvent({ kind: 'step_ok', at: 4, act: 1, attempt: 2, data: { big: true } }), { kind: 'succeeded', at: 4, attempt: 2 });
	assert.deepEqual(runEvent({ kind: 'step_wait', at: 5, attempt: 1, deadline: 99 }), { kind: 'waiting', at: 5, attempt: 1, deadline: 99 });
	assert.deepEqual(runEvent({ kind: 'signal', at: 6, name: 'approve', data: 'yes', meta: { signal_id: 's' } }), { kind: 'signal', at: 6, signal: 'approve' });
	assert.deepEqual(runEvent({ kind: 'timer', at: 7, timer: 3, act: 1 }), { kind: 'timer', at: 7 });
	assert.deepEqual(runEvent({ kind: 'recover', at: 8 }), { kind: 'recovered', at: 8 });
	assert.deepEqual(runEvent({ kind: 'resolve', at: 9, act: 1, error: 'no' }), { kind: 'resolved', at: 9, error: 'no' });
	assert.deepEqual(runEvent({ kind: 'cancel', at: 10, error: 'cancelled' }), { kind: 'cancelled', at: 10, error: 'cancelled' });
	assert.deepEqual(runEvent({ kind: 'callback_ok' as 'start', at: 11, attempt: 1 }), { kind: 'other', at: 11 });
});

test('progressKind tells workflows, calls and waits (a wait with a timeout by its signal)', () => {
	const r = (plan: string, workflow?: string) => ({ run_id: 'r', plan, tenant: 'default', status: 'running', workflow }) as RunInfo;
	assert.deepEqual(progressKind(r('kairo.workflow', 'orders')), { kind: 'workflow', name: 'orders' });
	assert.deepEqual(progressKind(r('kairo.call/llm')), { kind: 'call', name: 'llm' });
	assert.deepEqual(progressKind(r('kairo.wait/approve')), { kind: 'wait', name: 'approve' });
	assert.deepEqual(progressKind(r('kairo.wait/approve@60000')), { kind: 'wait', name: 'approve' });
	assert.deepEqual(progressKind(r('kairo.wait/a@b')), { kind: 'wait', name: 'a@b' });
	assert.deepEqual(progressKind(r('graph')), { kind: 'run', name: 'graph' });
});
