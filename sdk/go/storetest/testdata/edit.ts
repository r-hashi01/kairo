// The workflow of xlang_test.go, in TypeScript: the same actions, the same
// calls in the same order, so the same call ids and plans.
//
//   node edit.ts <sdk/ts/src> <kairo.wasm> <db> start|finish <id>
const [sdk, wasm, db, mode, id] = process.argv.slice(2);
const { EmbeddedBackend, Kairo, SQLiteStore } = await import(sdk + '/index.ts');

const runs = { llm: 0, write: 0 };
const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(db), wasm }) });
k.defineAction('llm', { effect: 'unprotected', handler: async (q: string) => (runs.llm++, q.toUpperCase()) });
k.defineAction('write', { effect: 'real', handler: async (p: string) => (runs.write++, `wrote ${p}`) });
k.workflow('edit', async (ctx: any, files: string[]) => {
	const a = await ctx.call('llm', files[0]);
	const b = await ctx.call('llm', files[1]);
	const wrote = await ctx.call('write', files[0]);
	if (mode === 'start') await new Promise(() => {}); // as a process that stops here
	const again = await ctx.call('llm', 'done');
	return { answers: [a, b], wrote, again };
});
await k.start();
if (mode === 'start') {
	k.run('edit', ['a', 'b'], { id }).catch(() => {});
	while (runs.write < 1) await new Promise((r) => setTimeout(r, 5));
	await k.close();
	console.log(JSON.stringify({ runs }));
} else {
	const out = await k.run('edit', ['a', 'b'], { id });
	await k.close();
	console.log(JSON.stringify({ runs, out }));
}
