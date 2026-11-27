// The prompts for the two reviews by Claude Code, and the report.

const article = (a) => `ファイル: ${a.path}\n\n<article>\n${a.content}\n</article>`;

export const readerPrompt = (a) => `次の Zenn の記事を、初めて読む読者の立場で通読し、分かりにくい箇所を指摘してください。ファイルは編集しないでください。

見てほしいこと:
1. タイトルを読んで、何の記事か一度で分かるか。
2. 一度読んで意味が取れない文、日本語として不自然な文、説明なしに出てくる用語。
3. 見出しだけを読んで、話の流れを追えるか。

出力: 問題のある箇所ごとに「該当の文（原文のまま）」「何が分かりにくいか」「書き換え案」を、重要な順に多くても 10 件。問題がなければ「指摘なし」とだけ書いてください。

${article(a)}`;

export const factsPrompt = (a) => `次の Zenn の記事について、書かれている事実が正しいかを点検してください。文章のうまさは見なくてかまいません。ファイルは編集しないでください。記事のリンク先は WebFetch で、このリポジトリのファイルは Read で確かめてかまいません。

出力: 「誤り」「言い過ぎ・条件が抜けている」「確かめられない」に当たるものだけを、重要な順に多くても 10 件。それぞれ、記事の該当箇所（原文）、根拠（リンク先やファイルと、そこに書かれていること）、どう直すべきかを書いてください。見つからなければ「指摘なし」とだけ書いてください。

${article(a)}`;

export function renderReport({ path, hash, article, links, lint, reader, facts }) {
	const lines = [`# 公開前チェック: ${path}（${hash}）`, ''];

	lines.push('## 記事の形式', '');
	if (article.ok) lines.push('- 問題なし');
	for (const p of article.problems) lines.push(`- ${p}`);
	for (const n of article.notes ?? []) lines.push(`- 残っているメモ: ${n}`);
	lines.push(`- published: ${article.published}`, '');

	lines.push('## リンク', '');
	if (!links.broken.length) lines.push(`- ${links.checked} 件すべて応答あり`);
	for (const b of links.broken) lines.push(`- ${b.status || b.error}: ${b.url}`);
	lines.push('');

	lines.push('## 日本語の lint', '');
	if (lint.skipped) lines.push(`- 実行していない（${lint.skipped}）`);
	else if (!lint.findings.length) lines.push('- 警告なし');
	else for (const f of lint.findings) lines.push(`- ${f.line} 行目: ${f.detail}（${f.excerpt}）`);
	lines.push('');

	lines.push('## Claude: 初めて読む人の目で', '', reader, '');
	lines.push('## Claude: 事実の点検', '', facts, '');
	return lines.join('\n');
}
