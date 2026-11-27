# zenn-check

A pre-publish check for Zenn articles kept in a GitHub repository, written as a kairo workflow and run from Claude Code. It checks an article's front matter, images and links, runs a Japanese lint, has Claude Code (`claude -p`) review it twice, writes a report, waits for your approval, and then pushes the article as a draft. Every step is recorded: run it again after a crash or a closed session and it goes on from there. The rest of this README is in Japanese.

---

Zenn の記事を GitHub で管理している人向けの、公開前チェックのワークフローです。kairo で書いてあり、Claude Code から動かします。

1. 次の 5 つを同時に行う
   - 記事の形式（front matter、画像、残っている `<!-- 要確認 ... -->` のメモ）
   - 記事内のリンクが応答するか
   - 日本語の lint（natural-japanese のプラグインと `uv` があるとき）
   - Claude Code（`claude -p`）によるレビュー 2 本: 初めて読む人の目で / 事実の点検（リンク先を読んで照らし合わせる）
2. 結果をレポートにまとめる（`.kairo/reports/`）
3. 承認を待つ。待っている間、プロセスは残らない
4. 承認されたら、記事とその画像だけをコミットして main に push する（Zenn がそれを取り込む。`published: false` なら下書きのまま、`true` なら公開される）

各ステップの結果は `.kairo/zenn-check.db`（SQLite）に記録されます。途中で止まっても、同じ `check` をもう一度実行すれば、終わったステップは記録から返り、止まった瞬間に実行中だったステップだけをやり直します。

## 必要なもの

- Node.js 22.13 以降（`node:sqlite` をフラグなしで使うため）
- ログイン済みの Claude Code の CLI（`claude`）。レビューは、あなたの Claude Code の利用枠を使います
- Zenn と連携した GitHub のリポジトリ。`origin` の `main` を Zenn が取り込んでいること。`approve` は `main` ブランチで実行します
- 日本語の lint を使うなら、Claude Code の natural-japanese のプラグインと `uv`。なければ lint は飛ばします

## 入れ方

Zenn の記事のリポジトリで:

```bash
mkdir -p tools .claude/skills
cp -R <kairo>/examples/zenn-check tools/zenn-check
mv tools/zenn-check/skill .claude/skills/zenn-check
npm install --prefix tools/zenn-check
echo '.kairo/' >> .gitignore
```

`tools/zenn-check` に置く前提で、リポジトリのルートを `tools/zenn-check` の 2 つ上として扱います。

## 使い方

Claude Code に「この記事をチェックして」と頼むと、スキル（`.claude/skills/zenn-check/SKILL.md`）に書いた手順で進みます。コマンドで直接動かすこともできます。

```bash
node tools/zenn-check/zenn-check.mjs check articles/<slug>.md            # 始める。途中なら続きから進める
node tools/zenn-check/zenn-check.mjs approve articles/<slug>.md          # 承認して push する
node tools/zenn-check/zenn-check.mjs approve articles/<slug>.md --deny   # 見送る
node tools/zenn-check/zenn-check.mjs status articles/<slug>.md           # どこまで進んだか
```

終了コード: 0 は完了、3 は承認待ち、2 は別の zenn-check が動いている、1 はエラー。

API キーなしで流れだけを試すときは、`ZENN_CHECK_FAKE_REVIEW=3`（Claude のレビューの代わりに、3 秒後に決まった文を返す）を付けます。

## 設計のメモ

- **すべてのステップが `unprotected`。** チェックとレビューは何も変えません。push は、差分があるときだけコミットし、push 済みのコミットをもう一度 push しても何も起きないように書いてあるので、もう一度実行しても困りません。そのため、途中で切れたステップは kairo が自動でやり直します。メール送信のように、何度実行しても同じ結果になるようには書けない処理を足すなら、それは `real` にします（実行中に止まると、もう一度は実行せず保留にする）。
- **ほかのコミットは push しない。** `git push origin main` は、main にあってまだ GitHub にないコミットをすべて送ります。zenn-check は、記事のコミットのほかに未 push のコミットがあれば、push せずに失敗します。push が失敗したら、ワークフローは承認待ちに戻ります。原因を片付けてから `approve` し直すと、新しい呼び出しとして push をやり直します（kairo では、同じ呼び出しは記録された結果を返すので、やり直しは別の呼び出しにします）。
- **承認を待つ間、プロセスを残さない。** kairo の suspend モードを使い、`check` は承認待ちに入ったところで終了します。`approve` が承認を送り、続きを進めます。
- **版は記事と画像の中身で決まる。** ワークフローの ID に、記事の本文と、記事が表示する画像の中身のハッシュを入れています。どちらかを書き換えると別のチェックになり、`approve` は、チェックした版のままの記事にしか効きません。
- **取り残されたレビューを止める。** zenn-check のプロセスが強制終了されても、それが起動した `claude -p` は動き続けます。その結果は誰にも使われません。起動したレビューのプロセス番号を `.kairo/reviews/` に残しておき、次に起動したときに、残っていれば止めてから始めます。kairo が記録するのはワークフローの呼び出しと結果で、アクションが起動した別のプロセスの後始末まではしません。
- **同時に 1 つだけ。** 再起動したプロセスがすぐ引き継げるように、kairo の `owner` を固定の名前にしています。同時に動くプロセスどうしで `owner` が同じだと、動いている作業まで止まったものとして扱われるので、ロックのファイル（`.kairo/zenn-check.lock`）で同時に 1 つしか動かないようにしています。
