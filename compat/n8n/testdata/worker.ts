// A worker for the interop test: runs n8n.node steps as engine v2's test
// executors do (each node answers {ran: <id>} on slot 0; "node-if" sends
// everything down slot 0 and nothing down slot 1); prints the runs that
// ended (ADR 0044).
import { Worker } from '../../../sdk/ts/src/index.ts';

const [addr, token] = process.argv.slice(2);
const worker = new Worker({
	name: 'interop',
	actions: ['n8n.node', 'n8n.node.pure'],
	concurrency: 4,
	token,
	handler: async (task) => {
		const node = task.params as { id: string };
		if (node.id === 'node-if') return { output: [[{ json: { taken: true } }], null] };
		return { output: [[{ json: { ran: node.id } }]] };
	},
	onRunEnd: (runId) => console.log(`run-end ${runId}`),
});
await worker.run(addr);
