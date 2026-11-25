// A small user app on the TypeScript SDK's embedded runtime (SQLite), the
// way the README's quick start uses it. kv.sh starts one process per role
// and kills them to prove what a user would see after a crash.
//
//   node app.ts run     <work> <runId> <amount> <sleepMs> [hold]
//   node app.ts signal  <work> <runId> <name>
//   node app.ts resolve <work> <callId>
//   node app.ts inspect <work> <runId>
//
// <work> holds kairo.db, kairo.wasm (built from this checkout) and markers/.
// Every line on stdout is one JSON object.
import { open, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { EmbeddedBackend, Kairo, SQLiteStore } from '../../../../sdk/ts/src/index.ts';

type Req = { order: string; amount: number; sleepMs: number };

const [role, work, id, ...rest] = process.argv.slice(2);
if (!role || !work || !id) {
	console.error('usage: node app.ts run|signal|resolve|inspect <work> <id> [args]');
	process.exit(2);
}
const hold = role === 'run' && rest[2] === 'hold';
const emit = (o: Record<string, unknown>) => console.log(JSON.stringify({ pid: process.pid, role, ...o }));

const backend = await EmbeddedBackend.open({
	store: new SQLiteStore(join(work, 'kairo.db')),
	wasm: join(work, 'kairo.wasm'),
	leaseMs: Number(process.env.KAIRO_VERIFY_LEASE_MS ?? 1000),
});
const k = new Kairo({ backend, observe: (o) => emit({ obs: o }) });

k.defineAction<Req, { risky: boolean }>('classify', {
	effect: 'unprotected',
	handler: async (req) => ({ risky: req.amount > 100 }),
});
k.defineAction<Req, { refunded: string }>('refund-payment', {
	effect: 'real',
	handler: async (req) => {
		if (hold) {
			// Stands for a payment API that has taken the request but not answered.
			await writeFile(join(work, 'markers', `${req.order}.started`), `${process.pid}\n`);
			emit({ event: 'real-action-holding', order: req.order });
			await new Promise(() => {});
		}
		// 'wx' fails if the file exists: a second execution is an error, not a no-op.
		const f = await open(join(work, 'markers', `${req.order}.done`), 'wx');
		try {
			await f.writeFile(`${process.pid}\n`);
		} finally {
			await f.close();
		}
		emit({ event: 'real-action-done', order: req.order });
		return { refunded: req.order };
	},
});
k.workflow<Req, { refunded: string; approved: boolean }>('refund', async (ctx, req) => {
	const verdict = await ctx.call<{ risky: boolean }>('classify', req);
	if (verdict.risky) await ctx.waitFor('approve');
	const paid = await ctx.call<{ refunded: string }>('refund-payment', req);
	await ctx.sleep(req.sleepMs);
	return { refunded: paid.refunded, approved: verdict.risky };
});

await k.start();
try {
	switch (role) {
		case 'run': {
			const req: Req = { order: id, amount: Number(rest[0] ?? 50), sleepMs: Number(rest[1] ?? 0) };
			emit({ event: 'run-begin', id, req });
			const out = await k.run('refund', req, { id });
			emit({ event: 'result', id, output: out });
			break;
		}
		case 'signal':
			await k.signal(id, rest[0] ?? 'approve', { by: 'verify-kairo' });
			emit({ event: 'signalled', id, name: rest[0] ?? 'approve' });
			break;
		case 'resolve':
			await k.resolve(id, { refunded: 'resolved-by-operator' });
			emit({ event: 'resolved', call: id });
			break;
		case 'inspect': {
			const runs = (await k.list({})).filter((r) => r.run_id === id);
			emit({ event: 'inspect', id, run: runs[0] ?? null, children: await k.children(id) });
			break;
		}
		default:
			throw new Error(`unknown role ${role}`);
	}
} catch (e) {
	emit({ event: 'error', name: (e as Error).name, message: (e as Error).message });
	process.exitCode = 1;
} finally {
	await k.close();
}
