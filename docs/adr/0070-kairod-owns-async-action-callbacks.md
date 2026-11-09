# 0070. kairod が非同期 HTTP アクションのコールバックと期限切れを所有する

- 状態: 提案
- 日付: 2026-11-09
- 関連: 不変条件 3〜7 / ADR 0004（出力コミット）、0009（ワーカープロトコル）、0026（打ち切り）、0041（実行ごとの費用）、0052（HTTP アクション）、0069（kairod の suspend）/ GitHub Issue #12

## 背景

ADR 0052 の URL アクションは、埋め込みランタイムでは同期応答（200）と非同期応答（202 と後のコールバック）の両方を扱える。kairod 接続では SDK process が worker protocol で task を受け、URL を呼べるが、202 を受けた後の結果の所有者を kairod へ渡せない。今は 202 を結果不明として返し、非同期 URL アクションを拒否している。

suspend mode では、SDK invocation は子 call が quiescent になれば終了する。コールバック先をその一時 process にすると、invocation の終了後や再配備後に結果を受け取れない。kairod は常駐し、実行のログ、timer、worker task を所有しているので、202 の後の待機とコールバックも kairod が所有する必要がある。

ただし、worker が 202 を返しただけで普通の成功結果にすると誤って完了する。worker 切断として扱うと、すでに外部で動いている実のアクションを結果不明にして早すぎる。202、コールバック、期限切れを再起動後も区別できる耐久状態が要る。また、インターネットから kairod へ結果を送る認証と到達可能な URL を決めなければならない。

## 決定

### 1. コールバック先は kairod が task ごとの capability URL として作る

kairod に、外から到達できる `callback_base` と、運用者が再起動後も同じ値を渡す `callback_key` を設定できるようにする。どちらかがなければ、非同期 URL アクションは kairod 接続では起動時に拒否する。秘密をデータディレクトリへ自動保存せず、環境変数または秘密管理から渡す。

kairod は task ごとに、`run_id`、`act`、`attempt`、期限を HMAC-SHA256 で署名した capability を作る。worker protocol の `Task` に任意の `callback` を足す。URL は kairod の callback endpoint と capability を含み、worker は ADR 0052 の action request の `callback` にそのまま入れる。

callback URL 自体を 256 bit 相当の bearer capability とする。HTTPS を必須とし、手元のテストだけ loopback HTTP を許す。kairod は capability、対象の試行、期限を検証する。アクション側が付ける従来の `Kairo-Signature` は送ってよいが、kairod はアクションごとの共有 secret を持たないため認証の根拠にはしない。古い試行または二度目の結果は、コアが無視する。

### 2. worker protocol に `pending` 結果を足す

worker protocol の `Result` に、任意の次の形を足す。

```json
{"pending":{"owner":"remote:https://example/action","until":1794182400000}}
```

worker が URL から 202 を受けた場合だけ返す。`until` は action timeout の期限で、worker が決めた相対時間ではなく unix milliseconds とする。`pending` は `output`、`error`、`wait` と同時に持てない。

新しい kairod は `Task.callback` を理解しない古い worker に非同期アクションを任せない。SDK worker は callback がない task で 202 を受けた場合、従来どおり unsupported の結果不明を返す。任意フィールドなので、同期結果だけの既存 worker と新しい kairod の互換性は保つ。

### 3. pending はコアの耐久状態にし、期限切れだけを結果不明にする

新しい event `step_pending` を足す。内容は `act`、`attempt`、owner、期限である。worker の pending result を受けた engine は dispatcher の credit を返し、`step_pending` を、ほかの task result と同じ順序でログへ書く。

コアは対象の試行を inflight のまま pending として保持し、その期限の command を出す。callback が先なら、通常の `step_ok` / `step_err` / `step_wait` を適用する。期限が先なら、既存の process-lost と同じ結果不明を適用する。不変条件 5 により、非冪等な実の作用は blocked、`idempotent_retry` のある作用だけが同じ冪等キーで再試行される。取り消しと timeout は ADR 0026 の規則を変えない。

`core.State` / `Act` の符号化と codec version、engine の event record を拡張する。新しい reader は旧状態と旧ログを読み続ける。pending を含む新しい snapshot / log は旧 binary では読めない。回復時は期限前なら timer を再武装し、期限後なら結果不明を 1 回だけ適用する。

### 4. kairod の callback API は結果を耐久化してから返す

`POST <callback_base>/v1/action-results/{run_id}/{act}/{attempt}?cap=...` は ADR 0052 と同じ result body を受ける。本文の上限と JSON の検証を行い、capability が合わなければ 401、期限切れなら 410、対象がなければ 404 とする。

有効な結果は shard inbox へ入れ、event が適用され、必要な tier まで耐久化された後に 200 を返す。すでに同じ結果または別の終端が先に記録されていれば、冪等な成功として 200 を返す。接続が切れたアクション側は同じ callback を再送できる。

callback により child call が終端すると、ADR 0069 の parent ready 通知を使う。kairod 自身は SDK のコードを実行しない。suspend の利用者は、callback と同時に設定した外部 webhook を増やさず、既存の一度きりの `wake` または fallback の Tick で ready workflow を取り上げる。

### 5. SDK の契約を 3 言語でそろえる

Go・Python・TypeScript は、kairod worker の task に callback があればそれを action request に使い、202 を pending result として返す。callback がない埋め込みランタイムでは ADR 0052 の自分の callback URL を使う。

kairod 接続の URL アクションにも従来どおり SDK 側の action secret が要る。これは SDK worker から action service への呼び出しを署名するためであり、kairod の `callback_key` とは別である。URL、作用、timeout、retry の宣言は今の action spec のままにし、秘密を kairod の plan / node spec やログへ入れない。

### 6. 費用

URL がなく、または同期応答する通常の task には、任意の callback field の生成以外の永続化を足さない。callback capability は task を dispatch するときだけ HMAC で作る。

202 を返した task だけが、pending event 1 件、期限 timer 1 件、後の callback または期限切れ event 1 件を払う。これは外で続く作用の状態の耐久化と結果の記録に必要な仕事である。実行ごとの goroutine、固定間隔の polling、callback の対応表は足さない（ADR 0041）。

## 結果

- SDK invocation と worker process が終了しても、kairod が非同期アクションの結果を受け取れる。
- 202 の直後や callback の直前に kairod が落ちても、pending の期限と結果不明の規則を回復できる。
- 非冪等な実の作用を、callback が遅いだけで二重に実行しない。
- worker protocol、core event、state codec、engine log の形式が増える。実装は承認後に行い、過去のログと snapshot を読む互換テストを必須にする。
- kairod を外から到達可能にする運用では、HTTPS、callback base、安定した callback key の管理が必要になる。

少なくとも次をテストする。

- 3 SDK の実 kairod テストで、async action が 202 を返した後に worker / SDK process を閉じ、別 process と Tick で workflow が完了する。
- pending のログ耐久化前、直後、callback 適用前、適用後、期限直前で kairod を停止して回復する。
- callback の重複、改ざん capability、別の run / act / attempt への付け替え、期限切れを拒む。
- callback と期限切れ、cancel、step timeout、worker disconnect を競合させ、結果が 1 つだけ記録される。
- 結果不明の非冪等な実は blocked になり再実行しない。冪等な実は同じ idempotency key でだけ再試行する。
- 旧 worker、旧 log、旧 snapshot を読める。通常 task の record byte と主要 benchmark に不要な回帰がない。

## 検討した代替案

- **SDK process を callback の所有者のままにする:** suspend invocation や再配備の後に到達できず、常駐 process が必要になる。
- **worker が callback まで接続を保持する:** 関数の時間上限と費用を解かず、worker 切断で結果が不明になる。
- **202 を成功または普通の wait として記録する:** 成功ではなく、期限時の出力もない。callback 前の状態と結果不明を表せない。
- **kairod が action の共有 secret をすべて持つ:** tenant ごとの秘密を plan や登録 API で運ぶ必要がある。worker が呼び出しを署名し、kairod は自分が発行した一回限りの capability だけを検証する方が秘密の境界が小さい。
- **callback 対応表を別ファイルまたはメモリに置く:** WAL / snapshot と別の真実になり、再起動かログ退役で失う。pending は実行状態そのものである。
- **組み込みの `http.*` executor だけを使う:** 常駐 worker で解ける利用者には有効だが、3 SDK の同じ action 宣言と serverless action handler を kairod 接続でも使う要件を満たさない。
