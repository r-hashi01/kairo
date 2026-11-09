# 0069. kairod のコードワークフローを、期限付きの駆動権と起床通知で suspend できるようにする

- 状態: 承認（2026-11-09）
- 日付: 2026-11-06
- 関連: 不変条件 4〜6 / ADR 0026（打ち切り）、0041（実行ごとの費用）、0049（コードで書く API）、0051（常駐しない形）、0053（scheduler entry point）、0058（Go SDK）、0059（suspend と駆動 lease）、0060（workflow version）、0062（親の replay reach）、0067（kairod の親子）、0068（kairod の List）/ GitHub Issue #11

## 背景

埋め込みランタイムの suspend mode は、コードワークフローを、記録済みの call を読みながら関数の先頭から再生する。未完了の call、timer、signal wait に着いたら、プロセスを占有せず `Suspended` / `ErrSuspended` で返る。call の決着か scheduler の `tick` が、期限付きの駆動権を取って関数をもう一度再生する（ADR 0051・0053・0059）。

kairod 接続では、call の実行状態と timer は kairod が持つが、コードの関数は SDK のプロセスにしかない。今の `Connect` / `HttpBackend` は、call が終わるまでそのプロセスで待ち、コードの駆動 lease を持たない。このため suspend mode を拒否している。

単に「call を投入した直後に返る」だけでは足りない。

- action handler が結果または durable wait を返す前に worker connection を閉じると、結果が不明になり得る。
- timer や signal で call が終わっても、次の serverless invocation を起こし、親の関数を再生する入口がない。
- cron が active workflow を全件 replay すると、仕事のない間も polling し、同じ workflow を複数 invocation が同時に駆動できる。
- workflow version を持たない deployment が、別の版の実行を取ってはいけない。

コードワークフローの作用は、記録される action / child / wait の call として行う。関数本体の再生は副作用を持たず、記録済みの call は同じ決定的な RunID で読み直す、という既存のモデルを保つ。その上で、同時の replay を防ぎ、必要なときだけ serverless process を起こす仕組みが要る。

## 決定

### 1. suspend は「call が決着または durable wait に入るまで」で返す

kairod 接続でも 3 SDK の既存の mode を受け付ける。

- Go: `Connect(..., kairo.Options{Mode: kairo.Suspend})`
- TypeScript: `new Kairo({backend: new HttpBackend(...), mode: 'suspend'})`
- Python: `Kairo(backend=HttpBackend(...), mode="suspend")`

suspend の workflow driver は、call を Submit した後、次のいずれかになるまで kairod の HTTP API で待つ。

- terminal（completed / failed / cancelled）
- blocked（人の review が必要）
- core state が quiescent（timer または signal を待ち、実行中の task がない）

terminal なら記録済みの結果で関数を続ける。blocked / quiescent なら workflow の replay を止め、`Suspended` / `ErrSuspended` で invocation を返す。したがって、SDK の一時的な worker connection は、action handler が結果または durable wait を kairod に返す前には閉じない。長い user action は invocation の実行時間を使う。別の常駐 worker で実行してもよい。

engine に quiescent waiter を足し、次を HTTP に出す。

`GET /v1/runs/{id}/quiescent?timeout=...`

応答は `RunInfo` と、その run が持つ最も早い timer の unix millisecond（なければ null）を返す。状態変化は shard の inbox で waiter を起こす。固定間隔の polling、run ごとの goroutine、shard loop 内の I/O は足さない。HTTP request 自身の goroutine / task だけが待ち、context が終われば waiter を外す。

### 2. workflow の駆動権を、親 shard が期限つきで 1 process に渡す

kairod にある `kairo.workflow` run ごとに、コードを replay するための一時的な `driveState` を持つ。通常の graph run の `run` 構造を太らせず、workflow run にだけ作る map とする。

`driveState` は次を持つ。

- ready generation: replay が必要になった世代
- seen generation: 最後に claim した世代
- claim token、owner、lease deadline

新しい workflow run は ready とする。直接の child が terminal になったとき、ADR 0067 の `mChildDone` と同じ親 shard の順序で ready generation を進める。claim は、ready generation が未処理か、以前の claim が期限切れのときだけ、1 process に token を渡す。claim 時点の generation を seen にする。claim 中に別の child が終われば generation がさらに進むため、release 後も ready のままになる。

driver は process 単位の 1 本の更新処理で、所有する claim を lease の半分より前に更新する。workflow ごとの goroutine / timer は作らない。正常に durable wait へ入ったときは claim を release する。保存先の一時エラーや process の停止では ready のまま release するか、lease expiry で別 invocation が取る。token が一致しない古い renew / release は何も変えない。

この状態は scheduling hint であり、workflow の真実ではないため WAL / snapshot / done marker には書かない。真実は、workflow root、child run とその結果として既に耐久化されている。kairod の再起動では claim をすべて失効させ、次の規則で安全側に作り直す。

- 親なしの未完了 workflow は ready。
- 親つき workflow は ADR 0067 のとおり、親の replay が同じ ID を再 Submit して到達するまで ready にしない。
- 外部 Parent の workflow も、同じ ID と親で外から再 Submit された時点で ready にする。

再起動後に余分な replay が 1 回起きることは許す。記録済み call の結果を読むだけで、実の作用を再発行しない。ready を失って永久に止まることは許さない。

### 3. claim API は workflow の版を選び、待てるようにする

API に、特定 run の claim と、ready workflow の次の claim を足す。wire の詳細は実装時に固定するが、少なくとも次を 1 request で渡す。

- process owner と lease duration
- 特定の RunID、または、この deployment が持つ `workflow -> versions` の集合
- 1 request で取る上限
- ready がまだなければ待つ timeout

成功した claim は、fencing token、lease deadline、`GetInput` 相当の workflow input を返す。workflow input に記録済みの version があるため、その版を持つ deployment だけが claim する。版の判定はコード workflow 用 API の層で行い、汎用の engine が SDK の input JSON を解釈しない。

ready がない request は、timeout まで engine の ready 通知を待てる。timer の期限と同時に request が到着しても、child の terminal event と `mChildDone` が耐久化されて ready になるまで、短い間隔で polling しない。

renew と release も HTTP API に出す。claim・renew・release・child terminal・workflow terminal は、対象 workflow の shard loop で 1 本の順序になる。

### 4. `Run`、`Signal`、`Resolve`、`Tick` を同じ ready 経路へつなぐ

- `Run` / `Submit`: workflow root を Submit した後、その ID を claim する。別 invocation が claim 済みなら `Suspended` / `ErrSuspended` で返す。claim できた invocation だけが関数を replay する。
- child workflow: 親の replay が Submit した時点を ADR 0067 の reach とし、その child workflow の claim を取って replay する。
- action / wait call: workflow の claim を持ったまま、決定 1 の quiescent / terminal まで進める。未完了なら workflow claim を正常 release する。
- `Signal` と `Resolve`: child call の決着を耐久化した後、suspend mode では ready になった親を claim して、その invocation の中で進められるだけ進める。
- `Tick`: この deployment が持つ workflow version に合う ready run を claim し、上限まで replay する。各 replay は再び suspended または terminal になるところまで進め、すべての claim を renew / release してから返す。

同じ workflow run を kairod の wait mode と suspend mode から同時に駆動する使い方は認めない。suspend の claim を使わない既存 wait mode の挙動は、この ADR では変えない。

### 5. timer と止まった invocation は、ADR 0053 の `wake` で起こす

quiescent 応答の timer deadline と、workflow claim の lease deadline を、SDK が invocation 中に集める。戻る前に最も早い時刻を、既存の `wake` callback へ 1 回だけ渡す。利用者は一度きりの scheduler で SDK 側の `/tick` を呼ぶ。

予定時刻の `/tick` は、ready claim request を待機つきで出す。kairod の timer shard が先に動いていなくても、timer event、child terminal、親の ready 通知までを同じ request の中で待てる。早すぎる重複 tick は何も claim せず、その deployment が取れる workflow の timer または現在の claim lease のうち、次の deadline を返す。renew 後の古い予約が発火しても、更新済み lease を奪わず、新しい deadline だけを予約し直せる。

signal wait には時刻がない。signal を受けた SDK invocation 自身が親を replay する。process が claim 中に停止した場合は、claim 時に予約した lease deadline の tick が replay を取り直す。`wake` を使わず periodic scheduler から `/tick` を呼ぶ運用も、ADR 0053 と同じ fallback として残す。

`/tick` の bearer token、`tickHandler`、`wake` の言語別の型は ADR 0053 のままにする。kairod 自身は常駐して timer と task を処理するが、SDK のコードを実行するために serverless process を常駐させたり polling したりしない。

### 6. 対象外と費用

ADR 0052 の async URL action は、kairod の worker protocol から callback の所有権を移せないため、従来どおり kairod 接続では対象外とする。suspend invocation の外まで続く action は、別の常駐 worker、または kairod の `http.*` executor として実行する。

通常の graph run は driveState、ready 通知、claim scan を払わない。コード workflow は、開始・child terminal・claim / renew / release のときだけ、親 shard 内の小さな状態を更新する。ready workflow の全体走査は `Tick` / claim request のときだけ行う。通常の event ごとに別ログを書かず、常駐 goroutine、run ごとの goroutine、固定間隔の polling を足さない（ADR 0041）。

## 結果

- 3 SDK の同じコードが、kairod を状態・timer・task の所有者にしたまま、serverless invocation ごとに replay できる。
- action handler の結果を記録する前に一時 worker を閉じず、実の結果を不明にしない。
- 同時の tick / Run は fencing された drive claim で 1 つだけが replay する。
- timer と停止した invocation は、一度きりの `wake` で起こせる。仕事のない間の SDK process と polling は要らない。
- driveState は永続形式を増やさない。kairod restart 後は余分な replay を許して、取りこぼしを作らない。
- 既存 wait mode、graph API、WAL / snapshot / worker protocol の形式は変えない。

少なくとも次をテストする。

- Go・TypeScript・Python の実 kairod テストで、毎回新しい SDK process / Kairo を開き、action、`sleep`、signal wait を含む workflow を `Run -> Suspended -> Tick / Signal` で完了する。
- action が結果または task wait を記録する前に suspend invocation が返らず、返った後には一時 worker を閉じても結果が不明にならない。
- 同じ workflow に対する同時 claim / Tick の一方だけが fencing token を得る。古い renew / release は新しい claim を壊さない。
- claim 中の child terminal、release、workflow terminal を競合させ、ready の取りこぼしと terminal workflow の再 claim を作らない。
- invocation を claim の途中で失い、lease deadline の tick が同じ workflow を取り直す。実の action は二重に実行しない。
- workflow version の新旧 deployment が、自分の持つ版だけを claim する。draining version の途中の run を新しい版で動かさない。
- kairod restart で親なし workflow を ready に戻す。親つき workflow は、親の replay reach 前には動かさない。
- quiescent wait の context cancellation で waiter が残らない。timer / result / cancel / review と競合させても race がない。
- `wake` が timer deadline と claim lease deadline の早い方を 1 回受け、予定時刻の待機つき Tick で workflow が進む。
- 通常の graph run の開始・event・保存 record byte と、既存 wait mode の経路に drive 用の追加処理がない。

## 検討した代替案

- **Submit の直後に常に `Suspended` を返す:** handler の結果が記録される前に worker が消え、実の outcome を不明にし得る。
- **Tick が active workflow を全件 replay する:** 仕事がなくても全件を読み、同時 tick が同じ関数を二重に動かす。版の違う deployment も区別できない。
- **SDK の process を worker として常駐させる:** serverless の要件を満たさず、今の wait mode と同じになる。
- **workflow ごとの goroutine / long polling process を常駐させる:** 不変条件 6 と、待機中の費用を状態の byte 数だけにする要件を破る。
- **drive claim と ready generation を WAL に記録する:** crash 後の正しさは durable call の記録から replay すれば得られる。scheduler hint の書き込みを workflow の通常経路に増やす必要はない。
- **child terminal のたびに kairod から利用者の webhook を呼ぶ:** callback URL、認証、retry、配備 version の routing が kairod の新しい永続仕事になる。既存の `wake` と待機つき claim で足りる。
- **worker protocol で workflow code も task として配る:** workflow の関数・版・言語別 runtime を kairod の worker task に変える別の実行モデルになる。既存 SDK の replay model と HTTP API で実現できる。
- **この ADR で async URL action も kairod に移す:** callback の所有権と protocol を変える独立した判断であり、Issue #11 の suspend mode には必須でない。
