// The checks the workflow runs, and the steps that touch the outside world.
// Each is an ordinary function; zenn-check.mjs declares how kairo treats it.
import { execFile, spawn } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';

const run = promisify(execFile);

// The images an article shows, as paths in the repository.
export const imagesOf = (content) => [...new Set([...content.matchAll(/\]\((\/images\/[^)\s]+)\)/g)].map((m) => m[1].slice(1)))];

// Zenn's rules for an article that `zenn preview` would otherwise only show
// as broken: the front matter, the images it refers to, and notes left in it.
export function checkArticle({ repo, path, content }) {
	const problems = [];
	const fm = content.match(/^---\n([\s\S]*?)\n---\n/);
	if (!fm) return { ok: false, problems: ['front matter (--- ... ---) がない'] };
	const field = (name) => fm[1].match(new RegExp(`^${name}:\\s*(.*)$`, 'm'))?.[1]?.trim();
	const title = field('title')?.replace(/^"|"$/g, '');
	if (!title) problems.push('title がない');
	else if ([...title].length > 70) problems.push(`title が ${[...title].length} 文字（70 文字まで）`);
	if (!field('emoji')) problems.push('emoji がない');
	if (!['"tech"', '"idea"', 'tech', 'idea'].includes(field('type') ?? '')) problems.push('type は "tech" か "idea"');
	const topics = (field('topics') ?? '').match(/"([^"]+)"/g) ?? [];
	if (topics.length < 1 || topics.length > 5) problems.push(`topics は 1〜5 個（${topics.length} 個）`);
	const published = field('published');
	if (!['true', 'false'].includes(published ?? '')) problems.push('published は true か false');
	for (const [, image] of content.matchAll(/!\[[^\]]*\]\((\/images\/[^)\s]+)\)/g)) {
		if (!existsSync(join(repo, image))) problems.push(`画像がない: ${image}`);
	}
	const notes = [...content.matchAll(/<!--\s*(要確認[^>]*?)-->/g)].map((m) => m[1].trim());
	if (published === 'true') notes.push('published: true なので、push すると公開される（下書きにはならない）');
	return { ok: problems.length === 0, problems, notes, published: published === 'true', title };
}

// Every http(s) link in the article, fetched to see that it answers.
export async function checkLinks({ content }) {
	const urls = [...new Set([...content.matchAll(/https?:\/\/[^\s)>\]"'`]+/g)].map((m) => m[0].replace(/[.,、。]+$/, '')))];
	const results = await Promise.all(
		urls.map(async (url) => {
			try {
				const res = await fetch(url, { redirect: 'follow', signal: AbortSignal.timeout(15_000) });
				return { url, status: res.status };
			} catch (e) {
				return { url, status: 0, error: String(e.cause?.code ?? e.name) };
			}
		}),
	);
	return { broken: results.filter((r) => r.status === 0 || r.status >= 400), checked: results.length };
}

// The natural-japanese skill's lint, if it is installed with uv.
export async function lintJapanese({ repo, path }) {
	const base = join(homedir(), '.claude/plugins/cache/natural-japanese/natural-japanese');
	const version = existsSync(base) ? readdirSync(base).sort().at(-1) : undefined;
	const lint = version && join(base, version, 'skills/natural-japanese/scripts/lint.py');
	if (!lint || !existsSync(lint)) return { skipped: 'natural-japanese の lint が見つからない' };
	try {
		const { stdout } = await run('uv', ['run', lint, '--genre', 'tech', '--json', join(repo, path)], { maxBuffer: 1 << 24 });
		const report = JSON.parse(stdout);
		const findings = (report.findings ?? []).filter((f) => f.severity !== 'info');
		return { findings: findings.map((f) => ({ line: f.line, detail: f.detail, excerpt: f.excerpt })) };
	} catch (e) {
		return { skipped: `lint を実行できなかった: ${e.message.split('\n')[0]}` };
	}
}

// One review by Claude Code (`claude -p`), reading only: the article and the
// pages it links to. Long, and it uses your plan's usage: the reason to keep
// its result in kairo and never ask twice.
// Reviews still running from a zenn-check that was killed: their results
// would never be used, so stop them before starting again.
export function stopLeftoverReviews(pidDir) {
	if (!existsSync(pidDir)) return [];
	const stopped = [];
	for (const name of readdirSync(pidDir)) {
		const pid = Number(name);
		try {
			process.kill(pid, 'SIGTERM');
			stopped.push(pid);
		} catch {}
		rmSync(join(pidDir, name), { force: true });
	}
	return stopped;
}

export function claudeReview({ repo, prompt, pidDir }) {
	// For trying the workflow without calling Claude: a fixed answer after N seconds.
	const fake = Number(process.env.ZENN_CHECK_FAKE_REVIEW);
	if (fake > 0) return new Promise((r) => setTimeout(() => r(`（テスト用の固定のレビュー）${prompt.length} 文字のプロンプトを受け取った`), fake * 1000));
	return new Promise((resolve, reject) => {
		const child = spawn('claude', ['-p', '--output-format', 'text', '--allowedTools', 'Read,Grep,Glob,WebFetch'], {
			cwd: repo,
			stdio: ['pipe', 'pipe', 'pipe'],
		});
		let out = '';
		let err = '';
		const pidFile = pidDir && child.pid ? join(pidDir, String(child.pid)) : undefined;
		if (pidFile) {
			mkdirSync(pidDir, { recursive: true });
			writeFileSync(pidFile, '');
		}
		const timer = setTimeout(() => child.kill('SIGTERM'), 20 * 60_000);
		child.stdout.on('data', (d) => (out += d));
		child.stderr.on('data', (d) => (err += d));
		child.on('error', reject);
		child.on('exit', (code) => {
			clearTimeout(timer);
			if (pidFile) rmSync(pidFile, { force: true });
			if (code === 0 && out.trim()) resolve(out.trim());
			else reject(new Error(`claude -p exited with ${code}: ${err.trim().slice(0, 300)}`));
		});
		child.stdin.end(prompt);
	});
}

// Commits the article and its images, and pushes them to main. Zenn syncs
// main, so this is what makes the draft appear on Zenn. Made again, it does
// no harm: it commits only what changed, and pushing a pushed commit does
// nothing.
export async function publishDraft({ repo, path }) {
	const git = (...args) => run('git', args, { cwd: repo });
	const { stdout: branch } = await git('branch', '--show-current');
	if (branch.trim() !== 'main') throw new Error(`main ブランチで実行する（今は ${branch.trim()}）`);
	// `git push origin main` pushes every commit main has that GitHub lacks:
	// refuse when some other commit is waiting, so that only this article goes up.
	await git('fetch', '--quiet', 'origin', 'main');
	const { stdout: ahead } = await git('log', '--format=%h %s', 'origin/main..HEAD');
	const others = ahead.trim().split('\n').filter((l) => l && !l.endsWith(`${path} を更新（zenn-check）`));
	if (others.length) throw new Error(`main に、まだ push していない別のコミットがある: ${others.join(' / ')}`);
	const images = imagesOf(readFileSync(join(repo, path), 'utf8'));
	await git('add', path, ...images);
	// Only the article and its images: anything else the user has staged stays out.
	const { stdout: staged } = await git('diff', '--cached', '--name-only', '--', path, ...images);
	if (staged.trim()) await git('commit', '-m', `${path} を更新（zenn-check）`, '--', path, ...images);
	await git('push', 'origin', 'main');
	const { stdout: head } = await git('rev-parse', 'HEAD');
	return { pushed: head.trim() };
}

