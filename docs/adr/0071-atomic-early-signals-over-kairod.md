# 0071. kairod の Signal を耐久化まで同期し、先取りの wait と receipt を原子的に作る

- 状態: 提案
- 日付: 2026-11-09
- 関連: 不変条件 2・3・6 / ADR 0023（冪等な Submit）、0041（実行ごとの費用）、0059（先取り signal）、0064（receipt と only-if-waiting）、0067（kairod の親子）、0069（kairod の suspend）/ GitHub Issue #12

## 背景

埋め込みランタイムの `SendSignal` は、同じ名前の wait を番号順に調べ、現在の wait へ原子的に届けるか、まだ到達していない次の wait を signal 済みの子実行として先に作る。receipt は wait の run ID、番号、先取りかどうかを返す。`OnlyIfWaiting` は現在の wait がなければ何も残さない（ADR 0059・0064）。

kairod の signal API は、event を shard inbox に入れた時点で 202 を返す。適用されたか、別の signal / timeout / cancel が先だったか、ログが耐久化したかを呼び出し側へ返さない。SDK は既存の wait だけを探して非同期 signal を送り、wait がまだない場合は `ErrNeedsEmbedded` とする。このため、serverless や複数クライアントでは、承認が wait より先に来る場合と、同時の回答を現在の wait だけに限定する場合に、埋め込みと同じ契約を使えない。

先取りは単なる親への signal ではない。後で `WaitFor` が使う決定的な run ID で子 wait を作り、親の木へ付け、signal event とともに耐久化する必要がある。同時送信の勝者は、対象 child shard の event 順序で一つに決めなければならない。

## 決定

### 1. event の適用と耐久化を返す engine 経路を足す

`Engine.ApplySignal(ctx, runID, name, payload)` を足し、次を返す。

- `applied = true`: signal event がコアに受け付けられ、ログがその event まで耐久化した。
- `applied = false`: 実行がない、signal 名が違う、または別の signal / timeout / cancel / 終端が先で event が無視された。無視された event はログへ書かない（不変条件 3）。

要求と返答は対象 run の shard inbox を通す。shard loop は I/O を待たず、通常の log ack の hold に小さな応答を結び付ける。tier が none の場合は適用時点、memory / file はその tier の ack 後に返す。context が切れても event は適用済みかもしれないため、呼び出し側は同じ payload を同じ探索手順で再試行する。

既存の非同期 `Engine.Signal` と HTTP 202 API は互換のため残す。新しい同期 API を使った要求だけが応答用の state を払い、通常の event hot path に waiter を割り当てない。

### 2. HTTP に receipt 用の同期 signal を足す

`POST /v1/runs/{run_id}/signals/{name}/apply` は JSON payload を受け、`{"applied":true|false}` を返す。適用と必要な耐久化が終わってから 200 を返す。存在しない run も探索上の通常結果として `applied=false` とし、通信エラーと区別する。

body 上限と JSON の規則は既存 signal API と同じにする。この endpoint は一つの wait run だけを対象にし、workflow の番号探索や先取り作成は行わない。

### 3. 先取りの wait は Submit の initial signal と同じ durable batch で作る

`engine.SubmitRequest` と `POST /v1/runs` に、SDK 内部の wait plan だけが使える任意の `initial_signal`（name と payload）を足す。新しい run の start event に続けて signal event を同じ shard の batch に適用し、両方が耐久化してから Submit を返す。

次を検査する。

- plan 名は `kairo.wait/<name>` で、root もその signal を待つ既登録 plan である。
- parent は未完了の `kairo.workflow` run である。
- run ID は 3 SDK 共通の call ID 規則で SDK が作った値である。engine は hash の再実装をせず、親子 attach と RunID の冪等性を信頼境界にする。

同じ run ID がすでにあれば、最初の start と initial signal を変更せず `existing=true` を返す。呼び出し側はその番号を読み直して同期 signal を試す。新しい run なら、親への attach、start、signal の順に一つの作成として扱う。start / signal の既存 event と record 形式を使い、新しい event 種類は足さない。

### 4. 3 SDK は埋め込みと同じ番号探索を kairod でも行う

Go の `SendSignal`、TypeScript の `sendSignal`、Python の `send_signal` を kairod 接続でも使えるようにする。

同じ名前について番号 0 から繰り返す。

1. 決定的な wait run ID を `Get` する。未完了なら同期 signal を試し、`applied=true` なら `early=false` の receipt を返す。false なら次の番号へ進む。
2. run がなければ、まず同期 signal を送る。対象 shard で wait の作成が先なら true になり、通常 receipt を返す。false が「この順序では存在しなかった」の線形化点になる。
3. `only_if_waiting` なら、親 workflow が存在し未完了であることを確認して `NotWaiting` を返す。先取りは作らない。
4. 通常送信なら、wait plan を登録し、parent と initial signal を付けて同じ run ID を Submit する。新規なら `early=true` の receipt を返す。既存なら同じ番号を読み直す。

receipt と error の公開型・名前は ADR 0064 のままとする。既存の `Signal` は、kairod でも新しい実装を呼んで receipt を捨てる。これにより、kairod の `Signal` も wait より先に届いた payload を保持する。互換性上、先取りを望まない利用者は `OnlyIfWaiting` を使う。

suspend mode では、signal が child wait を終端して親が ready になった後、ADR 0069 と同じ Tick 経路で進める。同期 signal の HTTP handler 自身が workflow code を実行しない。

### 5. 費用と保持

signal が来たときだけ、番号ごとの Get と、勝者を決める同期 event 適用を行う。競合で負けた番号について、無視された event をログへ書かない。先取り時に増える start と signal の記録は、後で同じ wait が作る状態と payload の耐久化である。

通常の workflow 実行、graph run、signal のない run に state、goroutine、polling、対応表を足さない。先取り wait は parent の子であり、ADR 0067 の冪等期間と親子の終端規則に従う。kairod の終了 child は親より先に期限切れになりうる既存の制約を変えない（ADR 0067）。

## 結果

- 3 SDK の signal receipt、only-if-waiting、先取り signal の契約が、埋め込みと kairod で同じになる。
- 同時送信の一つだけが現在の wait を終わらせ、負けた通常送信は次の wait、only-if-waiting は `NotWaiting` になる。
- wait より先に送った payload は、親子関係と event log に耐久化され、SDK process や kairod の再起動をまたいで後の `WaitFor` が受け取る。
- 新しい log event、snapshot field、表、外部依存は増えない。HTTP API と Submit の入力契約、同期 ack の engine 経路は増える。

少なくとも次を 3 SDK と engine でテストする。

- 現在の wait への別 process からの同時 only-if-waiting は一つだけ成功し、次の wait に payload を残さない。
- 通常 signal の同時送信は現在と次の wait に異なる receipt で入り、payload が一つずつ返る。
- wait の作成と同期 signal を競合させ、shard の順序どおり applied または NotWaiting の一方になる。
- initial signal の start と signal の間、耐久 ack の前後で kairod を停止しても、signal だけまたは空の wait だけを残さない。
- signal の再送、HTTP 応答切断、kairod 再起動が payload を二つの番号へ複製しない。
- 存在しない、終了済み、workflow でない親を区別し、終端と先取り child attach の競合で親より後の子を作らない。
- context cancellation の同期 waiter が残らず、`scripts/check.sh --race` が通る。通常の非同期 signal と graph event の benchmark に不要な回帰がない。

## 検討した代替案

- **SDK が Get の後に既存の 202 signal を送る:** Get と適用の間に競合があり、どの wait が受け取ったか分からない。
- **親 workflow に signal queue と通し番号を持つ:** 親の state / codec / log をすべての signal で更新し、wait child と別の真実を作る。既存の決定的な child ID と event log で表せる。
- **kairod が SDK の call ID を再実装する:** 3 言語の canonical JSON と番号規則を engine に重複させる。SDK が ID を作り、engine は parent attach と冪等 Submit を検証すればよい。
- **Submit 後に普通の signal を別要求で送る:** 両方の間で停止すると、payload のない先取り wait が残る。initial signal を start と同じ durable batch に入れる。
- **すべての signal API を同期へ変える:** graph API の既存 202 契約と通常 event の費用を変える。receipt を要るコードワークフローだけが新 endpoint を使う。
- **先取り signal 専用の表を作る:** wait run が後で同じ payload を持つため、二重の保存と原子性が要る。signal 済み wait 自体を先に作る方が既存の意味に合う。
