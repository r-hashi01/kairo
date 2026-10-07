import assert from 'node:assert/strict';
import net from 'node:net';
import { test } from 'node:test';

import { FrameReader, MsgType, encodeFrame, resultBody } from './protocol.ts';
import { Worker, iterationOf, nodeOf } from './worker.ts';

// A fake runtime: hands out tasks, records what the worker says.
function runtime(onFrame: (type: number, body: any, send: (t: number, b: unknown) => void) => void) {
	return new Promise<{ port: number; close: () => void }>((resolve) => {
		const server = net.createServer((sock) => {
			const reader = new FrameReader();
			const send = (t: number, b: unknown) => sock.write(encodeFrame(t, b));
			sock.on('data', (d) => {
				for (const [t, b] of reader.push(d)) onFrame(t, b, send);
			});
		});
		server.listen(0, '127.0.0.1', () => {
			const { port } = server.address() as net.AddressInfo;
			resolve({ port, close: () => server.close() });
		});
	});
}

test('hello, tasks, results, chunks and cancellation', async () => {
	const seen: Array<[number, any]> = [];
	let worker!: Worker;
	const rt = await runtime((type, body, send) => {
		seen.push([type, body]);
		if (type === MsgType.Hello) {
			send(MsgType.Task, { seq: 1, run_id: 'r', step_id: 'a', act: 1, attempt: 1, idempotency_key: 'k', action: 'echo', input: { x: 1 } });
			send(MsgType.Task, { seq: 2, run_id: 'r', step_id: 'b[3]', act: 2, attempt: 1, idempotency_key: 'k2', action: 'echo', input: null });
			send(MsgType.Task, { seq: 3, run_id: 'r', step_id: 'c', act: 3, attempt: 1, idempotency_key: 'k3', action: 'slow', input: null });
			send(MsgType.Cancel, { seq: 3 });
		}
		if (type === MsgType.Result && seen.filter(([t]) => t === MsgType.Result).length === 3) worker.stop();
	});
	worker = new Worker({
		name: 'ts-1',
		actions: ['echo', 'slow'],
		concurrency: 4,
		token: 'secret',
		handler: async (task, ctx) => {
			if (task.action === 'slow') {
				await new Promise((r) => ctx.signal.addEventListener('abort', r));
				return { error: 'cancelled', unknown: true };
			}
			if (task.step_id === 'b[3]') throw new Error('boom');
			ctx.emit('hi');
			return { output: { echo: task.input, node: nodeOf(task.step_id), round: iterationOf(task.step_id) } };
		},
	});
	await worker.run({ host: '127.0.0.1', port: rt.port });
	rt.close();

	assert.deepEqual(seen[0], [MsgType.Hello, { worker: 'ts-1', actions: ['echo', 'slow'], credit: 4, token: 'secret' }]);
	const results = new Map(seen.filter(([t]) => t === MsgType.Result).map(([, b]) => [b.seq, b]));
	assert.deepEqual(results.get(1), { seq: 1, output: { echo: { x: 1 }, node: 'a', round: 0 } });
	assert.deepEqual(results.get(2), { seq: 2, error: 'boom' });
	assert.deepEqual(results.get(3), { seq: 3, error: 'cancelled', unknown: true });
	assert.ok(seen.some(([t, b]) => t === MsgType.Chunk && b.seq === 1 && Buffer.from(b.data, 'base64').toString() === 'hi'));
});

test('frames split across reads', () => {
	const r = new FrameReader();
	const f = Buffer.concat([encodeFrame(MsgType.Credit, { n: 2 }), encodeFrame(MsgType.Credit, { n: 3 })]);
	assert.deepEqual(r.push(f.subarray(0, 3)), []);
	assert.deepEqual(r.push(f.subarray(3, 12)), [[MsgType.Credit, { n: 2 }]]);
	assert.deepEqual(r.push(f.subarray(12)), [[MsgType.Credit, { n: 3 }]]);
});

test('run ends, asked for in the hello', async () => {
	const seen: Array<[number, any]> = [];
	const ended: string[] = [];
	let worker!: Worker;
	const rt = await runtime((type, body, send) => {
		seen.push([type, body]);
		if (type === MsgType.Hello) {
			send(MsgType.Task, { seq: 1, run_id: 'r1', step_id: 'a', act: 1, attempt: 1, idempotency_key: 'k', action: 'echo', input: null });
		}
		if (type === MsgType.Result) {
			send(MsgType.RunEnd, { run_id: 'r1' });
			send(MsgType.Cancel, { seq: 99 }); // something after it, to know it was read
			setTimeout(() => worker.stop(), 20);
		}
	});
	worker = new Worker({
		name: 'ts-2',
		actions: ['echo'],
		concurrency: 1,
		handler: async () => ({ output: {} }),
		onRunEnd: (id) => ended.push(id),
	});
	await worker.run({ host: '127.0.0.1', port: rt.port });
	rt.close();
	assert.deepEqual(seen[0], [MsgType.Hello, { worker: 'ts-2', actions: ['echo'], credit: 1, run_end: true }]);
	assert.deepEqual(ended, ['r1']);
});

test('a result that waits', () => {
	assert.deepEqual(resultBody(4, { wait: { until: 1700000000000, output: [[{ json: { a: 1 } }]] } }), {
		seq: 4,
		wait: { until: 1700000000000, output: [[{ json: { a: 1 } }]] },
	});
	assert.deepEqual(resultBody(5, { wait: { until: 1.7 } }), { seq: 5, wait: { until: 1, output: null } });
});
