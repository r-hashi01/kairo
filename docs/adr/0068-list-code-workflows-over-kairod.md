# 0068. kairod のコードワークフローを shard 横断で一覧する

- 状態: 承認（2026-11-06）
- 日付: 2026-11-06
- 関連: ADR 0016（ログ退役）、0027（終了マーカー）、0041（実行ごとの費用）、0049（コードで書く API）、0050（出力保持）、0059（埋め込みの List）、0067（kairod の親子）/ GitHub Issue #11

## 背景

ADR 0059 は、埋め込みランタイムのコードワークフローを `workflow`、`status`、作成時刻で絞り、作成順で一覧する `List` を 3 SDK に足した。しかし kairod の engine は、実行の親子を除くコードワークフロー固有の情報（ワークフロー名、利用側の `meta`、作成・更新時刻）を保存していない。各 SDK は kairod 接続時の `List` を拒否している。

kairod では RunID ごとに shard が分かれ、終わった実行はログを退役して終了マーカーだけになる。一覧を足すには、active・evicted・終了済みを同じ順序で集め、再起動とログ退役の後にも cursor を解釈できなければならない。

全実行の開始・更新ごとに別の一覧表や索引ファイルを書けば検索は速いが、WAL と別の真実ができ、すべての graph 実行が追加の書き込みを払う。Issue #11 が必要とするのはコードワークフローのセッション一覧であり、graph API の全実行を検索する管理 API ではない。

## 決定

### 1. 一覧に要る情報を、コードワークフローの実行メタデータとして保存する

`engine.SubmitRequest` と HTTP の `POST /v1/runs` に、任意の `workflow` と JSON object の `meta` を足す。3 SDK の HttpBackend は、`kairo.workflow` の根を開始するときだけ、コード上のワークフロー名と利用者が指定した `meta` を送る。action call、wait、子の内部実行は `workflow` を持たない。

engine の実行情報に次を持たせ、`RunInfo` と `GET /v1/runs/{id}` でも返す。

- `workflow`: コード上のワークフロー名。空なら一覧の対象外。
- `meta`: 開始時に指定した JSON object。後から変えない。
- `created_at`: `EvStart` を適用した時刻。
- `updated_at`: 最後に適用した event の時刻。

同じ RunID の再投入では、最初に耐久化した `workflow` と `meta` を変えない。SDK は既存実行の情報を読み、その版・親と同じく保存済みの値を正とする。

start / checkpoint metadata は tier byte の任意 flag と後置 field で `workflow`、`meta`、作成・更新時刻を保存する。flag のない旧 record と、親だけを持つ ADR 0067 の record を読み続け、workflow のない graph 実行の record は byte 単位で変えない。start の作成時刻は直後の `EvStart` から復元できるが、ログ退役後の checkpoint では復元できないため、checkpoint にも残す。

終了マーカーを次の version に上げ、同じ情報を残す。これにより、終了ログだけになった実行も `IdempotencyTTL` の間は一覧できる。旧 version のマーカーは workflow が空のため、コードワークフロー一覧には現れない。新しい表や書き換える索引ファイルは作らない。

### 2. engine が各 shard の snapshot を集め、要求時にだけ並べる

`engine.List(ctx, filter)` を足す。各 shard の inbox に一覧要求を送り、その shard が所有する active / evicted run と終了マーカーから、条件に合う `workflow != ""` かつ親なしの実行を返す。shard loop はメモリを見るだけで、I/O せず、他 shard を待たない。

engine の呼び出し側が shard の返答を集め、`(created_at, RunID の byte 順)` で全体を安定に並べ、`after` と `limit` を適用する。実行の開始・event 適用・終了マーカーの追加は各 shard loop が所有するため、各 shard の snapshot はそれぞれ一貫している。全 shard を同時刻に止めた snapshot にはしない。要求と同時に始まる実行が今回と次回のどちらに現れるかは未規定だが、1件の行を部分的な状態では返さない。

filter の意味は ADR 0059 と同じにする。

- `workflow` と `status` は完全一致。
- `since <= created_at < until`。省略した側は無境界。
- `after` は、その RunID の `(created_at, RunID)` より後。cursor が保持期間を過ぎて存在しなければ空を返す。
- `limit <= 0` は無制限。正なら先頭からその件数。

一覧要求の費用は要求時の `O(保持中の実行数 + 結果の sort)` とする。実行の開始・更新ごとに全体索引を保守せず、常駐 goroutine やポーリングを足さない。件数や検索頻度から永続索引が必要になった場合は、別の ADR で決める。

### 3. HTTP と 3 SDK を、埋め込みと同じ API へ接続する

次を足す。

`GET /v1/runs?workflow=&status=&since=&until=&after=&limit=`

`since` と `until` は unix millisecond、`after` は RunID とする。応答は `RunInfo` の配列で、上記の順序を保つ。不正な時刻・limit は HTTP 400 にする。

既存 API を kairod 接続時にも使えるようにする。

- Go: `k.List(ctx, kairo.Filter{...})`
- TypeScript: `k.list({...})`
- Python: `await k.list(...)`

日時の言語別表現、filter、返す `meta`・時刻は ADR 0059 のままにする。埋め込み側の保存形式と query は変えない。ただし、存在しない `after` の扱いは上記へそろえる。

### 4. 保持期間と対象

kairod の一覧は、現在の active / evicted 実行と、ADR 0027・0050 の冪等期間内にある終了実行だけを返す。`IdempotencyTTL` を過ぎた終了実行は cursor とともに消える。監査ログや無期限の履歴 API にはしない。

対象は `workflow` が設定された親なしのコードワークフローだけとする。action call、wait、子ワークフロー、通常の graph API の root は返さない。子ワークフローを辿る用途は ADR 0067 の `Children` を使う。

### 5. 費用

通常の graph API の実行は `workflow` と `meta` を送らず、record の extension も書かない。コードワークフローの根は、既存の start / checkpoint / done record にワークフロー名、meta、時刻を追加する。これは一覧対象の状態を耐久化する仕事である。

event ごとの追加書き込みはしない。`updated_at` は既存 event の `At` から更新し、checkpoint と終了マーカーにまとめて保存する。一覧を呼ばない実行が、一覧用の別 write や全 shard 走査を払うことはない（ADR 0041）。

## 結果

- 3 SDK の `List` が、埋め込みと kairod で同じ filter・順序・pagination を持つ。
- kairod の再起動、snapshot、ログ退役後も、保持期間内のコードワークフローを一覧できる。
- 一覧のための別の永続索引を持たず、要求時にだけ全 shard を走査する。
- Issue #11 の残りは suspend mode だけになる。駆動の所有権と scheduler は、この ADR では変えない。

少なくとも次をテストする。

- active、evicted、completed、failed、cancelled の根を、作成時刻と RunID の順で返す。子と通常の graph root は返さない。
- workflow、status、since、until、after、limit の組み合わせを、埋め込みの 3 SDK と同じ結果にする。
- 同じ millisecond に始まった実行を RunID の byte 順にし、page の境界で重複・欠落を作らない。
- start / checkpoint / done marker の旧 version を読み、新 version の workflow・meta・時刻を round-trip する。
- snapshot とログ退役の後、および kairod 再起動後も一覧でき、終了マーカーの期限後には消える。
- 一覧と開始・event・終了を競合させても race がなく、各行が自己矛盾しない。
- Go・TypeScript・Python の実 kairod テストで、meta と時刻、filter、pagination を確認する。
- workflow のない graph 実行の record byte と開始経路に extension が増えない。

## 検討した代替案

- **一覧専用の追記ログを足す:** WAL と終了マーカーに加えて別の真実ができ、開始・更新・終了との回復順序を解く必要がある。
- **書き換えるグローバル索引ファイルを足す:** 全実行の event ごとに追加 write が必要になり、原子的な置き換えと crash recovery の費用を通常経路へ持ち込む。
- **各 shard の結果をそのまま連結する:** shard 数や RunID の hash で順序が変わり、既存 SDK の作成順と一致しない。
- **全 shard を barrier で止めて snapshot を取る:** unrelated な実行の開始と終端を一覧要求が止める。session 一覧にその一貫性は不要である。
- **終了実行を IdempotencyTTL より長く保持する:** kairod の保持方針（ADR 0027・0050）を変える別の要件である。
- **通常の graph root も返す:** `workflow`、meta、コードの版を持つ session 一覧と意味が異なる。必要なら管理 API として別に決める。
- **この ADR で suspend mode も足す:** suspend は、誰がコードを駆動するか、決着後に何が再開するか、scheduler がどう起こすかを変える。一覧の読み取り経路とは独立した判断である。
