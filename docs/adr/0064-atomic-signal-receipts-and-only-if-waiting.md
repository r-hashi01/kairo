# 0064. Signal の適用を原子的に確認し、待ちの番号と先取りかどうかを返す

- 状態: 承認（2026-11-06）
- 日付: 2026-11-06
- 関連: 不変条件 2・3・6 / ADR 0041（実行 1 回あたりのコスト）、0049（コードで書く API）、0051（常駐しないランタイム）、0059（先に来たシグナル）/ GitHub Issue #6・#7

## 背景

コードで書くワークフローの `WaitFor(name)` は、同じ名前を繰り返し待てる。呼び出しの ID は、ワークフロー ID、`kairo.wait/<name>`、0 から始まる回数で決まる。埋め込みランタイムの `Signal` は、若い番号から調べ、まだ終わっていない wait に signal を送る。wait がまだ作られていなければ、その wait を signal 済みの実行として先に作る（ADR 0059）。ワークフローが後から同じ番号へ来ると、その signal を受け取る。

この API には 2 つ不足がある。

1. **今待っているときだけ届けられない。** 承認を端末、エディタ、CLI など複数のクライアントから答えると、最初の答えが現在の wait を終わらせ、次の答えは次回の wait への先取りとして残る。次の無関係な承認が、古い答えで終わってしまう。
2. **どの wait に届いたか分からない。** メッセージを受けて始めたターンの番号を送信側へ返したくても、`Signal` は結果を返さない。利用側が `kairo.wait/` という SDK 内部の名前の子を数えると、複数プロセスが同時に送った場合に同じ番号を答えうる。

今の実装は、wait の状態を読んだ後に signal を別の処理で適用する。2 つのプロセスが同じ未完了の wait を読むと、どちらも signal を送る。コアは最初の signal だけを記録し、後の signal を `ErrIgnored` として正しく捨てるが、SDK の保存先処理は「適用されたか」を返していない。そのため、後の送信側も成功したと思い、次の wait を探せない。

プロセス内のロックでは、同じ DB を使う別プロセスやサーバーレスの呼び出しを直列化できない。wait の実行行をロックしてイベントを適用する、既存の保存先トランザクションを順序の根拠にする必要がある。

## 決定

### 1. 埋め込みランタイムに、receipt を返す送信 API を足す

既存の `Signal` は互換のため残し、結果を要る送信には次を足す。

- Go: `k.SendSignal(ctx, id, name, payload, ...SignalOption) (SignalReceipt, error)`
- TypeScript: `k.sendSignal(id, name, payload, { onlyIfWaiting? })`
- Python: `await k.send_signal(id, name, payload, only_if_waiting=False)`

receipt は 3 つを返す。

- wait の実行 ID
- 同じ名前の `WaitFor` の番号（`index`、0 から始まる）
- `early`: wait がまだ関数から到達されておらず、この送信が先取りの wait を作ったか

Go は `SignalReceipt{RunID, Index, Early}`、TypeScript は `{ runId, index, early }`、Python は `SignalReceipt(run_id, index, early)` とする。

既存の `Signal` は、埋め込みランタイムでは同じ実装を呼び、receipt を捨てる。これにより、既存の利用側も、同時送信の負け側を次の番号へ正しく送るようになる。既定の「wait がなければ先取りする」という意味は変えない。

### 2. `only if waiting` は、現在の wait がなければ先取りしない

新しい送信 API の option を指定すると、まだ関数が到達していない wait を作らない。

- Go: `kairo.OnlyIfWaiting()`
- TypeScript: `{ onlyIfWaiting: true }`
- Python: `only_if_waiting=True`

現在、同じ名前を待つ未完了の wait があれば、そこへ届けて `early = false` の receipt を返す。なければ、Go は `ErrNotWaiting`、TypeScript と Python は `NotWaitingError` を返す。

ワークフローが存在しない場合は、これまでの「ワークフローがない」エラーを保つ。ワークフローが終わっている場合は、現在の wait がないので `ErrNotWaiting` / `NotWaitingError` とする。

「現在」は、対象になりうる wait の行に signal イベントを適用する保存先トランザクションの順で決める。そのトランザクションより前に wait が作られていれば届け、後なら `NotWaiting` になる。送信側で先に `Get` した結果だけでは判断しない。

### 3. signal イベントが記録されたかを、保存先トランザクションの結果として返す

埋め込みランタイムのイベント適用は、コアが event を受け付けたか（記録する event が 1 つ以上あったか）を、トランザクションの戻り値に含める。

- signal が wait を終わらせ、その event が追記された: `applied = true`
- 別の送信、timeout、取り消しなどが先に決着させ、コアが signal を無視した: `applied = false`
- 実行がまだない: `applied = false`

状態の書き込みと `applied` の計算は同じ `WithRun` / `withRun` / `with_run` の中で行う。新しい表、列、event、ログ形式は足さない。無視された event は今までどおり記録しない（不変条件 3）。

送信は若い番号から次のように進む。

1. 未完了に見える wait へ signal を適用する。`applied = true` なら、その番号の receipt を返す。
2. `applied = false` なら、同時送信などが先に決着させたものとして次の番号へ進む。
3. 実行がない番号に来たら、通常の送信は ADR 0059 と同じく signal 済みの wait を原子的に作り、`early = true` を返す。作成競合に負けたら、その番号を読み直す。
4. `only if waiting` は、存在しない番号へ signal の適用だけを試す。このトランザクションで `applied = false` なら先取りを作らず、親ワークフローの存在を確かめて `NotWaiting` を返す。

この順により、2 つの送信が同じ wait を未完了として読んでも、その行のトランザクションで一方だけが `applied = true` になる。負けた側は、通常なら次の番号を先取りし、`only if waiting` なら `NotWaiting` になる。

### 4. 今回は埋め込みランタイムだけに適用する

kairod の signal HTTP API は、イベントを shard の inbox に入れた時点で `202` を返し、イベントが適用されたか、耐久ログに記録されたかを待たない。SDK も、kairod がコードワークフロー間の親子を保存しないため、先取りの wait を作らない。

新しい `SendSignal` / `sendSignal` / `send_signal` は、埋め込みランタイム以外では `ErrNeedsEmbedded` または同等のエラーにする。既存の `Signal` は、kairod 経由では今までどおり、すでに存在する wait へ非同期に送る。

kairod で原子的な receipt を返すには、signal の適用と必要な耐久化を shard から HTTP の応答まで返す新しい同期経路が要る。ホットパス、HTTP API の意味、失敗時の応答を別の ADR で決める。

### 5. 費用

通常の signal は、今も wait の実行ごとに状態を読み、対象へイベントを適用している。新しく増えるのは、そのイベントが記録されたかをトランザクションの戻り値で返す処理だけで、永続化は増えない。

競合した送信は、負けた wait について無視された signal を 1 回適用し、次の番号へ進む。`only if waiting` で未到達の番号を確かめる場合も、存在しない行への no-op のトランザクションが 1 回ある。どちらも signal が来たときだけ払う仕事であり、ワークフロー実行ごとの費用や常駐する構造は増やさない（ADR 0041）。

## 結果

- 複数プロセスが同じ承認へ同時に答えても、`only if waiting` では 1 つだけが成功し、ほかは `NotWaiting` になる。次の承認へ古い答えを残さない。
- 通常の先取り signal を同時に送った場合は、1 つが現在の wait、次が次回の wait へ入り、それぞれ違う receipt を返す。
- 送信側は SDK 内部の plan 名や子の数を読まず、receipt の `index` をターン番号に使える。
- コア、状態の符号化、ログ、保存先の表、WASM ABI、ワーカープロトコルは変えない。
- kairod では新しい receipt API をまだ使えない。既存の signal の意味は変えない。

## 実装の詳細（2026-11-06）

- 3 SDK の埋め込みランタイムは、従来のイベント処理を互換の wrapper として残し、同じ保存先トランザクションから `applied` も返す内部処理を足した。`applied` は、その処理で記録対象の event が 1 件以上できたときだけ真になる。
- 既存の `Signal` / `signal` は、埋め込みランタイムでは新しい receipt API を呼んで receipt を捨てる。kairod の経路は従来の非同期 API のままにした。
- 共通の振る舞いとして、別プロセスからの同時送信、wait 作成との競合、終了済み・存在しないワークフロー、先取りを許す送信、従来 API の同時送信を検証した。

次のテストを 3 SDK の埋め込みの共通の振る舞いへ足す。

- 1 つの wait に、別プロセスから `only if waiting` の送信を同時に 2 つ行う。receipt は 1 つだけで `index = 0, early = false`、もう 1 つは `NotWaiting` になる。ワークフローの次の wait は、signal なしで待つ。
- 先取りを許して同時に 2 つ送る。receipt は `index = 0, early = false` と `index = 1, early = true` になり、ワークフローの 2 つの wait が 1 つずつ受け取る。
- wait の作成と `only if waiting` を競合させ、保存先トランザクションの順に応じて「その wait に届く」か `NotWaiting` のどちらか一方になる。signal を次の番号へ残さない。
- 存在しないワークフローと、終わったワークフローのエラーを区別する。
- 従来の `Signal` でも、同時送信の負け側が次の番号へ進み、signal を捨てない。

## 検討した代替案

- **送信側のプロセス内ロックで `Get` と `Signal` を囲む:** 別プロセスとサーバーレスの呼び出しを直列化できない。
- **親ワークフローに signal の通し番号を持つ:** すべての signal で親の行も更新し、表またはコアの状態の形式を変える。対象 wait の行のトランザクションだけで勝者を決められる。
- **子の一覧を数えて番号を返す:** 同時送信が同じ数を読み、SDK 内部の plan 名へ依存する。event が実際に適用されたかも分からない。
- **payload を読み返して、自分の signal が勝ったか比べる:** 同じ payload を送れる。読み返しまでに別の状態へ進みうる。適用したトランザクション自身が答えるほうが確かである。
- **`only if waiting` でも、まず `Get` して存在しなければ返す:** wait の作成との間に競合が残る。存在しない行への signal 適用を線形化点にする。
- **既存の `Signal` の戻り値を Go でも receipt に変える:** `if err := k.Signal(...)` を含む既存のソースを壊す。新しい `SendSignal` を足し、従来の API は receipt を捨てる。
- **この ADR で kairod の HTTP signal も同期化する:** shard の耐久化 ack を外へ返す新しい待ち合わせが要り、エンジンのホットパスと API の意味が変わる。埋め込みの DB 共有で必要な問題を先に解く。
