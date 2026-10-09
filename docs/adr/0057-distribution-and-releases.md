# 0057. 配布の形: SDK は kairo-sdk として npm と PyPI に、kairo.wasm を同梱し、タグからの 1 本のリリースで出す

- 状態: 承認
- 日付: 2026-10-08
- 関連: ADR 0018（依存のある実装は別モジュール）、0048（kairod を同梱した配布。0051 により置き換え）、0051（常駐しない kairo）、0055（kairo-jev）

## 背景

kairo は GitHub（`r-hashi01/kairo`、MIT）で公開した。利用者（KairoCode を含む）が使えるようにするには、次の 3 つが要る。今はどれもない。

- **SDK を入れる手段:** TypeScript と Python の SDK は、ソースから作るしかない。
- **Go から取り込む手段:** Go のモジュールのパスが `kairo` なので、`go get github.com/r-hashi01/kairo` で取り込めない。別モジュール（`kairo/store/sqlite` など）も同じである。
- **バイナリ:** kairod と kairo-jev は、ソースから作るしかない。

SDK は、純粋なコアを WASM（`kairo.wasm`、4.4MB、gzip で 1.2MB）として抱える（ADR 0051）。SDK と `kairo.wasm` は ABI の版で結びついている（`ABI_VERSION`。今は 1。読み込むときに照合する）。

名前の空きは確かめた（2026-10-08）。`kairo` は npm と PyPI の両方で別のパッケージが使っている。`kairo-sdk` は両方で空いている。

## 決定

### 名前

- **npm:** `kairo-sdk`（`import { Kairo } from 'kairo-sdk'`）。
- **PyPI:** `kairo-sdk`。Python の import 名も `kairo_sdk` に改める（今は `kairo_worker`）。
  - 配布の名前と import の名前を揃えるためである。
  - 古い名前の別名は残さない。まだ配布していないので、使っている人がいない。Dify の統合（`compat/dify` の e2e テスト）は、評価のためのもので、新しい名前に合わせて直す。
- **Go のモジュール:** パスを `github.com/r-hashi01/kairo` に改める。別モジュールも `github.com/r-hashi01/kairo/store/sqlite` などにする。
  - 全ファイルの import を書き換える。機械的な置き換えで、`go build` と全テストで確かめる。
  - 別モジュールの `replace kairo => ../..` は、新しいパスで残す（リポジトリの中では手元の本体を使う）。

### パッケージの中身

- **npm（`kairo-sdk`）:** `dist/`（コンパイルした JavaScript と型定義）と `wasm/kairo.wasm`。実行時の依存はない（Node 22 以上）。
- **PyPI（`kairo-sdk`）:** 純粋な Python の wheel（`py3-none-any`）と sdist。`kairo_sdk/wasm/kairo.wasm` を同梱する。
  - 埋め込みのランタイムは、`pip install "kairo-sdk[embedded]"` で wasmtime を足して使う（今のまま）。
- **GitHub Release:**
  - `kairo.wasm` と、その sha256。
  - kairod と kairo-jev のバイナリ（linux と darwin、amd64 と arm64）と、その sha256。

### 版

- リポジトリで 1 つの版にする。タグ `vX.Y.Z` が、Go のモジュール、npm、PyPI の版になる。
  - 別モジュールのタグは、Go の決まりに従って `store/sqlite/vX.Y.Z` のように打つ（同じ版の番号）。
- 1.0 までは 0.x とし、マイナー版で互換性を壊しうる（semver の 0.x の慣例）。
- ABI の版（`kairo_abi_version`）は、版の番号とは別に保つ。SDK と同梱の `kairo.wasm` は必ず同じ版から作るので、利用者は ABI の版を意識しない。

### リリースの流れ（`.github/workflows/release.yml`）

タグ `v*` の push で動く。

1. `scripts/check.sh` を流す。通らなければ、何も出さない。
2. `kairo.wasm` を作る。Go の版は `go.mod` に固定し、`-trimpath` を付ける。sha256 を記録する。
   - もう一度作って同じ sha256 になることを確かめる（同じソースから同じバイト列ができること）。
3. SDK に同梱した `kairo.wasm` の ABI の版が、SDK の `ABI_VERSION` と同じであることを確かめる。
4. npm のパッケージと、Python の wheel と sdist を作る。どちらも中身の `kairo.wasm` の sha256 が、2. の値と同じであることを確かめる。
5. 公開する。鍵は持たず、**信頼された発行者（OIDC）**を使う。
   - npm: `npm publish --provenance`（npm の Trusted Publishing）。
   - PyPI: PyPI の Trusted Publishing（`pypa/gh-action-pypi-publish`）。
6. GitHub Release を作り、`kairo.wasm`、バイナリ、sha256 の一覧を付ける。

### 利用者（リポジトリの持ち主）の準備

公開の前に、一度だけ次を行う。コードの側からはできない。

- npm に `kairo-sdk` を作り、Trusted Publishing に、このリポジトリの `release.yml` を登録する。
- PyPI に `kairo-sdk` の「保留中の発行者（pending publisher）」として、同じワークフローを登録する。

### 今はしないこと

- Homebrew、Docker のイメージ、Windows のバイナリ。
- 0.1.0 の公開そのもの。この ADR で、公開できる状態まで作る。最初のタグを打つのは、利用者が決める。

## 結果

- `npm install kairo-sdk`、`pip install "kairo-sdk[embedded]"`、`go get github.com/r-hashi01/kairo` で使えるようになる。
- 配布物の `kairo.wasm` が、どのソースから作られたかを、sha256 と provenance で確かめられる。
- **悪くなること:** Go の import のパスが長くなる。全ファイルの書き換えになり、Dify と n8n の統合のブランチ（ほかのリポジトリ）との差が広がる。統合は評価のためのものなので、追いかけない。
- **新しく守ること:**
  - リリースは、タグからの `release.yml` だけで行う。手元から公開しない。
  - SDK と `kairo.wasm` は、同じコミットから作る。
- **テスト（足す）:**
  - `release.yml` を、タグなしで手動で動かせる形（`workflow_dispatch`、公開の手順を飛ばす）にして、公開の前の手順（作る、照合する、sha256 を比べる）を確かめる。
  - パッケージから入れた SDK で、README の quick start が動くこと（npm の tarball と wheel を、一時的な場所に入れて動かす）。

## 検討した代替案

- **名前を `@kairo/sdk`（npm の組織のスコープ）にする:** 組織の `kairo` を取れるかが分からない。PyPI には組織のスコープがないので、両方で同じ名前にできる `kairo-sdk` を選んだ。
- **Python の import 名を `kairo_worker` のままにする:** 配布の名前と食い違う。ワーカーだった頃の名前でもある。配布の前の今が、直す費用がいちばん小さい。
- **`kairo.wasm` を SDK に同梱せず、初回に取りに行く:** オフラインやサーバーレスで、初回の起動が遅くなるか失敗する。1.2MB（gzip）なら同梱してよい。
- **SDK と Go の版を別々にする:** SDK とコアの組み合わせを利用者が考えることになる。1 本の版なら、同じタグの SDK と `kairo.wasm` が必ず合う。
- **npm と PyPI のトークンを CI の秘密に置く:** 漏れたときの影響が大きい。Trusted Publishing（OIDC）なら、長く使う鍵を持たずに済む。

## 実装で詰めた詳細（2026-10-08 追記、ADR 0017）

- **組み立てと公開を分けた:** 組み立てと照合は `scripts/release.sh VERSION [OUT]` に置いた。タグがなくても、手元でも CI でも動く。`release.yml` は、これを呼んでから公開する。
  - 手で動かした（`workflow_dispatch`）ときは、組み立てと照合だけを行い、公開の 3 つのジョブは動かない。
- **`release.sh` の中身:**
  - SDK は作業ツリーの写しから作る（作業ツリーは変えない）。
  - 版は引数から入れる。npm は `npm pkg set version`、Python は `pyproject.toml` の書き換えで入れる。
  - Python は版を正規化する（`0.0.0-dev` → `0.0.0.dev0`）ので、wheel はファイル名の版に頼らずに探す。
  - quick start は README のものをそのまま使う。データベースの場所と金額（承認を待たない額）だけを変える。npm の tarball と wheel を、一時的な場所に入れて動かす。README の例が動かなくなったら、リリースが止まる。
- **バイナリ:** `CGO_ENABLED=0`、`-trimpath`、`-ldflags='-s -w'` で作る。
- **見つかって直したもの:**
  - TypeScript 6 は `types` を書かないと Node の型を読まないので、`sdk/ts/tsconfig.json` に `"types": ["node"]` を足した。今までは、手元の型検査で引数として渡していたので気づかなかった。
  - Python の `kairo_sdk/wasm` は、setuptools がパッケージとして扱うので、`packages` と `package-data` に明記した。
- **別モジュールを `go get` する場合の制約:** 別モジュール（`store/*`、`executor/jev`）の `go.mod` は、本体を `v0.0.0` で求め、リポジトリの中では `replace` で手元を使う。`replace` は取り込む側では効かない。そのため、外から `go get github.com/r-hashi01/kairo/store/sqlite@vX.Y.Z` で取り込めるようにするには、リリースの前に本体の版を求めるよう直す必要がある。今回は本体（標準ライブラリだけ）の `go get` と、バイナリの配布までにとどめた。別モジュールの取り込みは、必要になったら別に扱う。
- **検証:** `scripts/release.sh 0.0.0-dev` を手元（macOS）で通しで流した。
  - `kairo.wasm` は 2 回作って同じ sha256 だった。
  - npm と PyPI のパッケージの中の `kairo.wasm` も、同じ sha256 だった。
  - パッケージから入れた SDK で、README の quick start が TypeScript と Python の両方で動いた。
  - バイナリ 8 つと `SHA256SUMS` を作れた。
  - npm と PyPI への公開は、まだ試していない。利用者が Trusted Publishing を登録した後の、最初のタグで確かめる。
- **一緒に見つかって直したもの（この ADR の外）:** 名前の変更の後の検証で、`TestFeedChunksWhileDisconnected` がまれに落ちた。変更前の `main` でも 900 回に 1 回落ちる、もとからある競合だった。
  - 原因: 接続の前に出たチャンクがまだ中継のキューにある間に、`Subscribe` が境目を決めていた。
  - 直し方: `Subscribe` が区切りをキューに積み、それまでのチャンクがバッファに入るのを待つようにした（`engine/feed.go`）。
  - 確かめたこと: 直した後は 1,500 回流して一度も落ちなかった。race 検出器つきでも通った。`invariant-reviewer` のレビューで、不変条件への違反とデッドロックの経路がないことも確かめた。
- **npm の最初の 1 回だけは手で公開する（決定からの例外。2026-10-08 に利用者と合意）:**
  - npm の Trusted Publishing は、すでにあるパッケージにしか登録できない。登録の画面は、一度公開した後に現れる。
  - そこで、中身のない `kairo-sdk@0.0.0`（README だけ）を利用者が手で公開して名前を確保し、すぐ deprecate する。そのあと Trusted Publishing に `release.yml` を登録する。
  - 中身のある版は、0.1.0 からすべて `release.yml` で公開する（provenance 付き）。
  - PyPI は、まだない名前にも「保留中の発行者」を登録できるので、例外は要らない。
- **npm は段階的な公開にする（2026-10-08、利用者と合意）:** npm の Trusted Publishing の「Allowed actions」で、`npm publish` を許さない（npm 自身の推奨）。
  - `release.yml` は `npm stage publish` で版を仮置きにする。npmjs.com の Staged Packages か `npm stage approve <id>` で、メンテナが二段階認証で承認したときに公開される。
  - CI の認証（OIDC）が悪用されても、承認なしには公開されない。
  - `npm stage publish` は npm 11.15.0 以上が要るので、CI で npm を上げる。tarball のパスを受け付けるかが文書にないため、展開したディレクトリの中で実行する。
  - PyPI には仮置きの仕組みがないので、GitHub の `release` 環境の承認者で、公開の前に人の確認を挟む。

### 版を 1 か所で持つ（2026-10-09 追記）

v0.1.0 と v0.1.1 では、決定の「1 つの版」が守れていなかった。npm と PyPI はタグの版で出ていたが、リポジトリの `package.json` と `pyproject.toml` は 0.1.0 のままだった。Go の別モジュールにはタグがなく、互いの `require` も `v0.0.0` だったので、`go get` で同じ版を取れなかった。そこで次のようにした。

- **版はリポジトリ直下の `VERSION` に 1 つだけ持つ。** npm（`package.json` と `src/version.ts` の `version`）、PyPI（`pyproject.toml` と `kairo_sdk.__version__`）、Go の SDK（`kairo.Version`）、Go のモジュールどうしの `require` が、この版を持つ。
- **`scripts/version.sh` で扱う。**
  - `check` は、これらがそろっていることを確かめる。`check.sh` から呼ぶ。
  - `set X.Y.Z` は、すべてを書き換える。
  - `tags` は、リリースで打つタグを並べる。`vX.Y.Z` と、別モジュールの `<dir>/vX.Y.Z` を出す。対象の別モジュールは `store/*`、`executor/jev`、`sdk/go/pgnotify`。`compat/*`（評価用）と `sdk/go/storetest`（テスト用）にはタグを打たない。
- **リリースのタグは `VERSION` と同じでなければならない。** `release.yml` が、タグで動いたときに `scripts/version.sh check <タグの版>` で確かめる。
- **リリースの手順。** `scripts/version.sh set X.Y.Z` でコミットし、`scripts/version.sh tags` のタグをすべて同じコミットに打って push する。別モジュールのタグは `v*` に当たらないので、`release.yml` を二重に動かさない。
- **言語ごとに版がずれる場合は、そのとき別の ADR で決める。** 言語のメジャーアップデートなどで、いずれずれることは見込んでいる。
