import { createHash } from 'node:crypto';
import { access, open, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { HttpBackend, Kairo, Suspended } from '../../sdk/ts/src/index.ts';

const ACTION = 'qualify.ts.once';
const WORKFLOW = 'qualify.ts';
const VERSION = 'qualification-v1';

function required(name: string): string {
	const value = process.env[name];
	if (!value) throw new Error(`${name} is required`);
	return value;
}

function marker(directory: string, runId: string): string {
	return join(directory, createHash('sha256').update(runId).digest('hex') + '.done');
}

async function runOne(k: Kairo, tick: () => Promise<void>, runId: string): Promise<void> {
	try {
		await k.run(WORKFLOW, runId, { id: runId });
		throw new Error(`${runId}: initial run did not suspend`);
	} catch (e) {
		if (!(e instanceof Suspended)) throw e;
	}
	for (;;) {
		await tick();
		const children = await k.children(runId);
		if (children.some((row) => row.plan.startsWith('kairo.wait/continue') && !['completed', 'failed', 'cancelled'].includes(row.status))) {
			await k.signal(runId, 'continue', runId);
			break;
		}
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
	for (;;) {
		try {
			const result = await k.result<string>(runId);
			if (result !== runId) throw new Error(`${runId}: result ${JSON.stringify(result)}`);
			return;
		} catch (e) {
			if (!(e instanceof Suspended)) throw e;
			await tick();
		}
	}
}

const url = required('KAIRO_QUALIFY_URL');
const socket = required('KAIRO_QUALIFY_SOCKET');
const markers = required('KAIRO_QUALIFY_MARKERS');
const prefix = required('KAIRO_QUALIFY_PREFIX');
const runs = Number(required('KAIRO_QUALIFY_RUNS'));
const inflight = Math.min(Number(required('KAIRO_QUALIFY_INFLIGHT')), runs);
const timeoutSeconds = Number(required('KAIRO_QUALIFY_TIMEOUT_SECONDS'));
const barrier = process.env.KAIRO_QUALIFY_BARRIER;
const driver = process.env.KAIRO_QUALIFY_DRIVER;
if (!Number.isInteger(runs) || runs < 1 || !Number.isInteger(inflight) || inflight < 1 || !Number.isInteger(timeoutSeconds) || timeoutSeconds < 1) {
	throw new Error('qualification counts and timeout must be positive integers');
}

const backend = new HttpBackend({ url, worker: socket, concurrency: 64, leaseMs: 1000 });
const k = new Kairo({
	backend,
	mode: 'suspend',
	wake: async () => {},
	observe: (o) => {
		if (o.kind === 'step.started' || o.kind === 'step.finished')
			console.log(JSON.stringify({ phase: 'worker', ...o, at_unix_ms: o.at, duration_ms: o.duration ?? 0 }));
	},
});
k.defineAction<string, string>(ACTION, {
	effect: 'real',
	handler: async (runId) => {
		const file = await open(marker(markers, runId), 'wx', 0o644);
		try {
			await file.writeFile(runId + '\n');
		} finally {
			await file.close();
		}
		return runId;
	},
});
k.workflow<string, string>(
	WORKFLOW,
	async (ctx, runId) => {
		await ctx.call(ACTION, runId);
		await ctx.sleep(25);
		return ctx.waitFor<string>('continue');
	},
	{ version: VERSION },
);

await k.start();
const tick = async (): Promise<void> => {
	const began = performance.now();
	try {
		await k.tick();
	} finally {
		console.log(JSON.stringify({ phase: 'claim_drive', duration_ms: performance.now() - began }));
	}
};
let nextRun = 0;
const worker = async (): Promise<void> => {
	for (;;) {
		const i = nextRun++;
		if (i >= runs) return;
		await runOne(k, tick, `qualify-ts-${prefix}-${i.toString().padStart(6, '0')}`);
	}
};
let timeout: ReturnType<typeof setTimeout> | undefined;
try {
	await Promise.race([
		Promise.all(Array.from({ length: inflight }, () => worker())).then(async () => {
			if (!barrier || !driver) return;
			await writeFile(join(barrier, driver + '.done'), '');
			for (;;) {
				try {
					await access(join(barrier, 'release'));
					return;
				} catch {
					await new Promise((resolve) => setTimeout(resolve, 50));
				}
			}
		}),
		new Promise<never>((_, reject) => {
			timeout = setTimeout(() => reject(new Error('qualification timed out')), timeoutSeconds * 1000);
		}),
	]);
} finally {
	clearTimeout(timeout);
	await k.close();
}
console.log(`typescript driver ${prefix}: ${runs} workflows completed`);
