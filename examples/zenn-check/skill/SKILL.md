---
name: zenn-check
description: Zenn の記事（articles/*.md）を公開前にチェックし、ユーザーが承認したら下書きとして main に push する。チェックは kairo のワークフローで動くので、途中で止まっても続きから再開できる。「記事をチェックして」「公開前にチェック」「この記事を push して」「下書きを上げて」などで使う。
---

# zenn-check

`tools/zenn-check/zenn-check.mjs` を実行して、記事の公開前チェックを kairo のワークフローとして動かす。

ワークフローの流れ:

1. 次の 5 つを同時に行う
   - 記事の形式（front matter、画像、残っている `<!-- 要確認 -->` などのメモ）
   - 記事内のリンクが応答するか
   - 日本語の lint（natural-japanese のスキルが入っていれば）
   - Claude Code（`claude -p`）によるレビュー 2 つ: 初めて読む人の目で / 事実の点検
2. 結果をレポートにまとめる（`.kairo/reports/<slug>-<hash>.md`）
3. ユーザーの承認を待つ。待つ間、プロセスは動いていない
4. 承認されたら、記事とその画像だけをコミットして main に push する（Zenn が同期する。`published: false` なら下書きのまま、`true` なら公開される）。main に、ほかにまだ push していないコミットがあるときは、push せずに承認待ちに戻る

各ステップの結果は `.kairo/zenn-check.db` に記録される。同じ `check` をもう一度実行すると、終わったチェックとレビューはやり直さずに続きから進む。

## 手順

1. 対象の記事を決める。どれか分からなければユーザーに聞く。
2. `tools/zenn-check/node_modules` がなければ、先に `npm install --prefix tools/zenn-check` を実行する。
3. チェックを始める。Claude のレビューで数分かかるので、Bash の `run_in_background` で実行し、終了の通知を待つ（ポーリングしない）。

   ```bash
   node tools/zenn-check/zenn-check.mjs check articles/<slug>.md
   ```

4. 終了コードで分ける。

   - **3（承認待ち）**: 出力にあるレポートを Read で読み、要点をユーザーに短く伝える。伝えるのは、形式の問題、残っているメモ、応答しないリンク、lint の警告、Claude の指摘のうち重要なもの。
     - 直したほうがよい点があれば、記事を直すかどうかをユーザーに聞く。記事を直すと内容が変わるので、`check` を最初から実行し直す（新しいチェックになる）。
     - そのまま push してよいかを、AskUserQuestion でユーザーに確認する。
       - 承認されたら: `node tools/zenn-check/zenn-check.mjs approve articles/<slug>.md`
       - 見送るなら: `node tools/zenn-check/zenn-check.mjs approve articles/<slug>.md --deny`
     - 出力に「push に失敗した」とあれば、その理由（例: main に、まだ push していない別のコミットがある）をユーザーに伝える。原因の片付け方をユーザーと決め、片付いたら、もう一度 push してよいかを確認して `approve` を実行する。zenn-check のほうで勝手にコミットを動かしたり消したりしない。
   - **0（完了）**: 出力（push したコミット、または見送り）をユーザーに伝える。
   - **2**: 別の zenn-check が動いている。それが終わるのを待ってから実行する。
   - **1（エラー）**: 出力と `.kairo/zenn-check.log` の末尾を見て、何が起きたかをユーザーに伝える。

5. 途中で止まった場合（セッションが切れた、コマンドがタイムアウトした、プロセスが落ちた）は、同じ `check` をもう一度実行する。続きから進む。`approve` の途中で止まった push も、次の `check` がもう一度 push する（push は、何度実行しても同じ結果になるように書いてある）。
6. どこまで進んだかを知りたいときは `status` を使う。

   ```bash
   node tools/zenn-check/zenn-check.mjs status articles/<slug>.md
   ```

## 守ること

- ユーザーの承認なしに `approve` を実行しない。`--deny` も、ユーザーが見送ると言ったときだけ。
- `approve` は main ブランチで実行する。チェックしたあとに記事や、記事が表示する画像を書き換えると、`approve` は拒否される。書き換えたら `check` からやり直す。
- `.kairo/` の中身（データベースや記録）を直接編集したり消したりしない。
- `ZENN_CHECK_FAKE_REVIEW` は試験用の環境変数なので、普段は設定しない。
- Claude のレビューは、ユーザーの Claude Code の利用枠を使う。同じ版の記事で `check` を繰り返しても、終わったレビューはやり直さない。
