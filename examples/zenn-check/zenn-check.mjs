#!/usr/bin/env node
// The pre-publish check of a Zenn article, as a kairo workflow:
//
//   1. at once: the article's front matter and images, its links, the Japanese
//      lint, and two reviews by Claude Code (`claude -p`): as a first-time
//      reader, and of the facts
//   2. a report in .kairo/reports/
//   3. a wait for your approval (no process is held while it waits)
//   4. approved: the article and its images committed and pushed to main,
//      which Zenn syncs as it is (published: false stays a draft)
//
// Every step can be made again without harm (the push commits only what
// changed, and pushing a pushed commit does nothing), so every step is
// `unprotected`: cut off by a crash, it is simply made again.
//
// Each step's result is recorded in .kairo/zenn-check.db. Run `check` again
// after a crash, a closed session or a timeout, and it goes on from there:
// finished checks and reviews are not made again.
//
//   node tools/zenn-check/zenn-check.mjs check <article.md>             start, or go on
//   node tools/zenn-check/zenn-check.mjs approve <article.md> [--deny]  answer the wait, and go on
//   node tools/zenn-check/zenn-check.mjs status <article.md>
//
// Exit codes: 0 finished, 3 waiting for approval, 2 another zenn-check is
// running, 1 an error. The workflow is the article's content: edit the article
// or an image it shows and `check` starts a new one, and `approve` answers only
// the version checked.
import { createHash } from 'node:crypto';
import { appendFileSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { basename, dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { CallError, EmbeddedBackend, Kairo, SQLiteStore, Suspended } from 'kairo-sdk';
import { checkArticle, checkLinks, claudeReview, imagesOf, lintJapanese, publishDraft, stopLeftoverReviews } from './checks.mjs';
import { factsPrompt, readerPrompt, renderReport } from './report.mjs';

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const data = join(repo, '.kairo');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const say = (s = '') => process.stdout.write(`${s}\n`);

const [command, file, ...flags] = process.argv.slice(2);
if (!['check', 'approve', 'status'].includes(command) || !file) {
	say('usage: zenn-check.mjs check|approve|status <articles/....md> [--deny]');
	process.exit(1);
}
const path = relative(repo, resolve(process.cwd(), file));
if (!existsSync(join(repo, path))) {
	say(`記事が見つからない: ${path}`);
	process.exit(1);
}
const content = readFileSync(join(repo, path), 'utf8');
const digest = createHash('sha256').update(content);
for (const image of imagesOf(content)) digest.update(existsSync(join(repo, image)) ? readFileSync(join(repo, image)) : `missing ${image}`);
const hash = digest.digest('hex').slice(0, 12);
const slug = basename(path, '.md');
const id = `zenn-check-${slug}-${hash}`;
const input = { path, content, hash };

mkdirSync(join(data, 'reports'), { recursive: true });
mkdirSync(join(data, 'tmp'), { recursive: true });

// One zenn-check at a time: the runs share one name in kairo's leases, which
// lets a restarted run take over at once, and two running together must not.
const lock = join(data, 'zenn-check.lock');
if (existsSync(lock)) {
	const pid = Number(readFileSync(lock, 'utf8'));
	let alive = false;
	try {
		process.kill(pid, 0);
		alive = true;
	} catch {}
	if (alive) {
		say(`別の zenn-check（pid ${pid}）が動いている。終わるのを待ってから実行する。`);
		process.exit(2);
	}
}
writeFileSync(lock, String(process.pid));
const unlock = () => rmSync(lock, { force: true });
process.on('exit', unlock);

// We hold the lock, so any review still running belongs to a zenn-check that died.
const reviews = join(data, 'reviews');
const leftover = stopLeftoverReviews(reviews);

const log = (msg, attrs) => appendFileSync(join(data, 'zenn-check.log'), `${new Date().toISOString()} ${msg} ${attrs ? JSON.stringify(attrs) : ''}\n`);
const backend = await EmbeddedBackend.open({ store: new SQLiteStore(join(data, 'zenn-check.db')), owner: 'zenn-check' });
const k = new Kairo({ backend, mode: 'suspend', logger: { warn: log, error: log } });

// Checks and reviews change nothing: if cut off, they are simply made again.
k.defineAction('check-article', { effect: 'unprotected', timeout: '1m', handler: (a) => checkArticle({ repo, ...a }) });
k.defineAction('check-links', { effect: 'unprotected', timeout: '5m', handler: (a) => checkLinks(a) });
k.defineAction('lint', {
	effect: 'unprotected',
	timeout: '5m',
	handler: (a) => {
		const copy = join(data, 'tmp', `${a.hash}.md`); // the checked version, not what the file holds now
		writeFileSync(copy, a.content);
		return lintJapanese({ repo, path: relative(repo, copy) });
	},
});
k.defineAction('review', {
	effect: 'unprotected',
	timeout: '25m',
	handler: (a) => claudeReview({ repo, pidDir: reviews, prompt: a.kind === 'reader' ? readerPrompt(a) : factsPrompt(a) }),
});
k.defineAction('write-report', {
	effect: 'unprotected',
	handler: (r) => {
		const out = join(data, 'reports', `${basename(r.path, '.md')}-${r.hash}.md`);
		writeFileSync(out, renderReport(r));
		return relative(repo, out);
	},
});
// Pushing to main is what Zenn syncs. It is written so that making it again
// does no harm, so it is unprotected too: cut off, it is made again.
k.defineAction('publish-draft', { effect: 'unprotected', timeout: '5m', handler: (a) => publishDraft({ repo, ...a }) });

k.workflow('zenn-check', async (ctx, a) => {
	const [article, links, lint, reader, facts] = await Promise.all([
		ctx.call('check-article', a),
		ctx.call('check-links', a),
		ctx.call('lint', a),
		ctx.call('review', { ...a, kind: 'reader' }),
		ctx.call('review', { ...a, kind: 'facts' }),
	]);
	const report = await ctx.call('write-report', { path: a.path, hash: a.hash, article, links, lint, reader, facts });
	// A push that fails (another commit waiting on main, not on main, ...) goes
	// back to the wait: fix what it says, and approve again. Each attempt is a
	// call of its own (a call made again returns its recorded result).
	for (let attempt = 1; ; attempt++) {
		const answer = await ctx.waitFor('approve');
		if (!answer?.approved) return { published: false, report };
		try {
			const pushed = await ctx.call('publish-draft', { path: a.path, attempt });
			return { published: true, report, ...pushed };
		} catch (e) {
			if (!(e instanceof CallError) || e.status !== 'failed') throw e;
		}
	}
});

await k.start();

// What the workflow is doing, as it goes: which steps came back from the
// record, which were cut off last time and run again, which run now and how
// long they take.
const LABELS = {
	'check-article': '記事の形式',
	'check-links': 'リンク',
	lint: '日本語の lint',
	'write-report': 'レポート',
	approve: '承認待ち',
	'publish-draft': '下書きの push',
};
const label = (c) => {
	const name = c.plan.replace(/^kairo\.(call|wait)\//, '');
	if (name === 'review') return c.input?.in?.kind === 'facts' ? 'Claude: 事実の点検' : 'Claude: 初めて読む人の目で';
	return LABELS[name] ?? name;
};
const started = Date.now();
const clock = () => `${String(Math.round((Date.now() - started) / 1000)).padStart(4)}s`;
const seen = new Map();
let watching = false;
async function progress(first = false) {
	if (watching) return;
	watching = true;
	try {
		for (const c of await k.children(id)) {
			const before = seen.get(c.run_id);
			if (before?.status === c.status) continue;
			const l = label(c);
			const waits = c.plan.startsWith('kairo.wait/');
			if (first) {
				if (c.status === 'completed') say(`${clock()}  ↺ 記録から返した: ${l}`);
				else if (c.status === 'running' && !waits) say(`${clock()}  ↻ 前回、実行の途中で止まっていた: ${l}（やり直す）`);
				else if (c.status === 'running') say(`${clock()}  … ${l}`);
				else if (c.status === 'blocked') say(`${clock()}  ⏸ 結果が分からず保留: ${l}`);
			} else if (c.status === 'running') {
				say(`${clock()}  … ${l}`);
			} else if (c.status === 'completed') {
				const secs = before ? (Date.now() - before.since) / 1000 : (c.updatedAt - c.createdAt) / 1000;
				say(`${clock()}  ✓ ${l}${waits ? '' : `（${secs.toFixed(1)}s）`}`);
			} else if (c.status === 'blocked') {
				say(`${clock()}  ⏸ 結果が分からず保留: ${l}`);
			} else if (c.status === 'failed') {
				say(`${clock()}  ✗ ${l}`);
			}
			seen.set(c.run_id, { status: c.status, since: Date.now() });
		}
	} finally {
		watching = false;
	}
}

// Drive the workflow as far as it can go now: until it finishes, or nothing
// is left to do but wait (for approval, or for a person to look).
async function drive() {
	for (;;) {
		try {
			return { done: true, result: await k.run('zenn-check', input, { id }) };
		} catch (e) {
			if (!(e instanceof Suspended)) throw e;
		}
		await k.tick();
		const kids = await k.children(id);
		const busy = kids.some((c) => ['running', 'blocked'].includes(c.status) && !c.plan.startsWith('kairo.wait/'));
		if (!busy) {
			const waiting = kids.some((c) => c.plan === 'kairo.wait/approve' && c.status !== 'completed');
			return { done: false, waiting };
		}
		await sleep(1000);
	}
}

async function summary(result) {
	if (!result.done && !result.waiting) {
		say('進められるところまで進めたが、承認待ちでも完了でもない。status で中身を確かめる。');
		return 1;
	}
	if (!result.done) {
		const kids = await k.children(id);
		const report = kids.find((c) => c.plan === 'kairo.call/write-report' && c.status === 'completed');
		const failedPush = kids.filter((c) => c.plan === 'kairo.call/publish-draft' && c.status === 'failed').at(-1);
		if (failedPush) {
			say(`push に失敗した。原因を片付けてから、もう一度承認する。`);
			say(`  ${String(failedPush.output?.error ?? failedPush.error ?? '（理由は .kairo/zenn-check.log を見る）').replace(/^.*step call: /, '')}`);
		} else {
			say(`チェックが終わり、承認を待っている。`);
		}
		if (report) say(`レポート: ${join('.kairo/reports', `${slug}-${hash}.md`)}`);
		say(`承認する: node tools/zenn-check/zenn-check.mjs approve ${path}`);
		say(`見送る:   node tools/zenn-check/zenn-check.mjs approve ${path} --deny`);
		return 3;
	}
	const r = result.result;
	say(`レポート: ${r.report}`);
	say(r.published ? `下書きとして main に push した（${String(r.pushed).slice(0, 7)}）。` : 'push は見送った。');
	return 0;
}

let code = 1;
try {
	if (command === 'status') {
		const runs = await k.list({ workflow: 'zenn-check', limit: 1000 });
		const mine = runs.find((r) => r.run_id === id);
		if (!mine) {
			say(`この版（${hash}）のチェックはまだない。check で始める。`);
			code = 0;
		} else {
			for (const c of await k.children(id)) say(`  ${c.status.padEnd(9)} ${c.plan.replace(/^kairo\.(call|wait)\//, '')}`);
			code = mine.status === 'completed' ? 0 : 3;
		}
	} else {
		let finished = false;
		if (command === 'approve') {
			const runs = await k.list({ workflow: 'zenn-check', limit: 1000 });
			finished = runs.some((r) => r.run_id === id && r.status === 'completed');
			if (!runs.some((r) => r.run_id === id)) {
				say(`この版（${hash}）はまだチェックしていない。記事か、記事が表示する画像を書き換えたなら、先に check を実行する。`);
				process.exit(1);
			}
		}
		say(`zenn-check: ${path}（版 ${hash}）`);
		if (leftover.length) say(`       前回止まったプロセスが残していたレビュー ${leftover.length} 件を止めた`);
		await progress(true);
		const timer = setInterval(() => progress().catch(() => {}), 500);
		// Answered already (the workflow finished): show what it came to.
		if (command === 'approve' && !finished) await k.signal(id, 'approve', { approved: !flags.includes('--deny') });
		const result = await drive();
		clearInterval(timer);
		await progress();
		say();
		code = await summary(result);
	}
} catch (e) {
	say(`エラー: ${e.message}`);
	log('error', { message: e.message, stack: e.stack });
	code = 1;
} finally {
	await k.close();
}
process.exit(code);
