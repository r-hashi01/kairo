# 0066. コードワークフローの Resolve を、既存の kairod API へ接続する

- 状態: 承認（2026-11-06）
- 日付: 2026-11-06
- 関連: ADR 0003（結果不明の実の作用）、0049（コードで書く API）、0050（kairod の出力保持）、0051（埋め込みランタイム）、0060（実行中のコード変更）/ GitHub Issue #11

## 背景

コードで書くワークフローの `Resolve` / `ResolveFailed` は、結果が不明になって `blocked` で止まった実の呼び出しを、人が「成功した」または「失敗した」と決着させる。埋め込みランタイムでは Go、TypeScript、Python の 3 SDK から使えるが、`kairo.Connect` / `HttpBackend` で kairod へ接続した場合は、埋め込みが必要だというエラーになる。

kairod 自体には、決着に必要な HTTP API がすでにある。

- `GET /v1/runs/{id}` は `blocked` の実行に `reviews`（`act` と `step_id`）を返す。
- `POST /v1/runs/{id}/resolve` は `act` と成功時の出力または失敗理由を受け、`EvResolve` を適用する。
- `GET /v1/runs/{id}/wait` は、終端 event が耐久化した後に応答する。

不足しているのは、コードワークフローの「呼び出し ID だけを渡す」API と、kairod の「活性化番号を渡す」API の接続である。コア、ログ、HTTP の形式を変えなくても埋められる。

Issue #11 には、このほか `Children`、`List`、suspend mode も含まれる。しかし `Children` は kairod で親子関係を耐久化する変更、`List` は全 shard と退避済みの実行を列挙する変更、suspend mode はコードワークフローの駆動所有権を常駐ランタイムへ持たせる変更になる。Resolve とは保存形式、費用、障害時の意味が異なるため、この ADR では扱わない。

## 決定

### 1. kairod に接続した 3 SDK でも、結果不明で止まった呼び出しを決着できるようにする

次を kairod 接続時にも使えるようにする。

- Go: `k.Resolve(ctx, callID, output)` / `k.ResolveFailed(ctx, callID, message)`
- TypeScript: `k.resolve(callId, output)` / `k.resolveFailed(callId, message)`
- Python: `await k.resolve(call_id, output)` / `await k.resolve_failed(call_id, message)`

HTTP backend は、呼び出し ID を受けたら次の順に既存 API を使う。

1. `GET /v1/runs/{id}` で実行を読む。
2. 終了済みなら、すでに決着しているものとして成功を返す。これにより、HTTP 応答を受け取れなかった呼び出し元が同じ操作を安全に再試行できる。
3. `reviews` が空なら、決着させられる手順がないという競合エラーを返す。
4. `reviews` の先頭の `act` を、`POST /v1/runs/{id}/resolve` へ出力または失敗理由とともに送る。コードワークフローが作る call plan は 1 つの step だけなので、review は高々 1 件である。先頭を使う規則は埋め込みランタイムと同じにする。
5. `GET /v1/runs/{id}/wait` で呼び出しの終端を待ってから成功を返す。`POST` の `202 Accepted` だけでは event の適用と耐久化を表さないためである。

Go の remote、TypeScript と Python の `HttpBackend` にこの処理を足す。各言語の低水準 HTTP client にも、review の型と resolve endpoint の呼び出しを足す。既存の endpoint と JSON の形は変えない。

### 2. HTTP の再試行では「すでに終わっている」を成功とする

`POST /resolve` が受理された後、終端を待つ前に接続や kairod のプロセスが切れると、呼び出し元には決着が耐久化したか分からない。同じ `callID` で再試行したときに実行がすでに終わっていれば、Resolve の目的は達成済みなので成功を返す。

競合する 2 つの Resolve が異なる結果を送った場合は、コアで先に記録された `EvResolve` だけが効く。後の操作は、すでに終わった呼び出しを決着済みとして扱い、先の結果を書き換えない。この API は compare-and-set ではなく、未決着の呼び出しを決着させる冪等な操作とする。

存在しない ID は従来どおり unknown-run / HTTP 404 相当を返す。未完了だが `reviews` がない実行は、通常実行中の実の試行を誤って決着させないため、競合エラーにする。

### 3. kairod 接続で扱うのは、`blocked` の review だけにする

ADR 0060 では、埋め込みランタイムで実行中に action の作用を実から弱めた場合、結果不明の印がまだない実の試行も、`Unknown` を付けた `EvResolve` で手動決着できるようにした。この判定は、実行に保存した旧 plan hash と、そのプロセスが持つ現在の plan を比較する埋め込みランタイム固有の処理である。

kairod 接続では、`RunInfo.reviews` に現れる通常の要確認だけを対象にする。review がない試行へ SDK が `Unknown` を推測して送る機能は足さない。kairod では登録済み plan をサーバーが所有し、HttpBackend は ADR 0060 の版固定と作用を弱めた判定を行わないためである。

### 4. 費用

通常の実行と、埋め込みランタイムの Resolve には仕事を足さない。kairod 経由で Resolve したときだけ、現在の review を読む GET、既存の resolve POST、耐久化した終端を待つ long-poll を行う。

待機のための goroutine、常駐する対応表、実行ごとのポーリングは足さない。既存の `/wait` を、その操作を要求した呼び出し元だけが使う。これは決着させる対象の確認、結果の記録、耐久化の確認に必要な仕事である（ADR 0041）。

## 結果

- kairod に接続したコードワークフローでも、unknown outcome を運用者が SDK から決着できる。
- SDK の Resolve は、kairod が単に event を受理した時点でなく、呼び出しの終端が耐久化した後に返る。
- 応答喪失後の再試行で、記録済みの決着を上書きせず成功を返せる。
- コア、engine の event、ログ、スナップショット、ワーカープロトコル、HTTP API の形式は変わらない。
- Issue #11 の Children、List、suspend mode は未解決のままであり、それぞれの保存・実行モデルを決める後続 ADR で扱う。

## 実装の詳細（2026-11-06）

- Go の `remoteInfo`、TypeScript の `RunInfo`、Python の既存の辞書から `reviews` を読み、Go の `remote.resolve` と TypeScript・Python の `HttpBackend.resolve` が先頭の `act` を既存 endpoint へ送るようにした。
- 3 SDK とも、POST の後に既存の wait endpoint で呼び出しの終端を待つ。終了済みの呼び出しへの再送は POST を増やさず成功にする。未完了で review がない場合は競合エラーにする。
- 各 SDK の実 kairod 統合テストに、実の action が unknown outcome を返して `blocked` になり、成功出力または失敗理由で決着するケースを追加した。成功後に同じ Resolve を再送するケースも確認する。
- engine、HTTP handler、event、ログ、スナップショット、ワーカープロトコルには変更を加えていない。

3 SDK について、少なくとも次をテストする。

- kairod が返した review の `act` と、成功出力または失敗理由を既存の resolve endpoint へ送る。
- resolve の POST だけで返らず、`wait` が終端を返してから SDK の操作が完了する。
- 終了済みの呼び出しへの再試行は成功し、2 件目の resolve event は送らない。
- 存在しない ID と、未完了だが review のない呼び出しはエラーになる。
- 埋め込みランタイムの既存の Resolve と、通常の kairod 実行の経路は変わらない。

## 検討した代替案

- **`POST /resolve` を同期 API に変える:** 既存利用者に対する `202 Accepted` の契約を変え、engine に適用・耐久化の応答経路を足す必要がある。コードの call plan は resolve で終端するため、既存の `/wait` で同じ確認ができる。
- **SDK の利用者に `act` を渡させる:** コード API の呼び出し ID だけで扱う抽象を壊す。`act` は engine 内部の活性化番号であり、既存の GET から SDK が得られる。
- **POST が 202 を返した時点で SDK の Resolve を成功にする:** 直後の障害で event が失われても成功を返してしまい、埋め込みランタイムより弱い契約になる。
- **review がない実の試行にも `Unknown` を付ける:** 通常実行中の作用を人が二重に決着できる。作用を弱めた場合だけ許す ADR 0060 の判定材料も HttpBackend にはない。
- **Issue #11 の 4 機能を 1 つの ADR で決める:** Resolve は既存 API の接続だけだが、残りは親子の永続化、全体索引、駆動の所有権という別々の判断を要する。実装と承認の単位を分ける。
