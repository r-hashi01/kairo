# 0067. kairod の親子を耐久化し、親 shard で子の開始と親の終端を直列化する

- 状態: 承認（2026-11-06）
- 日付: 2026-11-06
- 関連: 不変条件 3・4・6・7・8・9 / ADR 0016（ログ退役）、0027（終了マーカー）、0041（実行ごとの費用）、0049（コードで書く API）、0050（出力保持）、0062（リプレイが到達した子だけを回復）、0065（外から親へ付ける）/ GitHub Issue #11、#4

## 背景

コードで書くワークフローを kairod で動かす場合も、ワークフロー関数が作る action call、wait、子ワークフローには論理上の親がある。しかし HttpBackend は `parent` を kairod へ送らず、engine も実行間の親子を保存しない。このため SDK の `Children` は kairod では使えず、ADR 0062 の「親のリプレイが到達した子だけを回復し、親より先に子孫を終端する」規則も適用できない。ADR 0065 の外から始める子も、kairod では親へ付けられない。

単に start record に親 ID を足すだけでは正しくない。RunID の FNV により親子は別 shard へ割り当てられうる。親が子を列挙した直後に別 shard で子を開始できると、親の終端後にその子の作用が動く。全 shard を止めるロック、shard loop からの待機、実行ごとの goroutine は採れない。

また、親のある実行を kairod の再起動時に従来どおり単独で `EvRecover` すると、親のコードがもう到達しない作用を先に動かす。親子の記録を足す変更では、ADR 0062 が明記した回復規則も同時に適用する必要がある。

## 決定

### 1. 親 ID を実行の耐久メタデータとして保存する

`engine.SubmitRequest` と HTTP の `POST /v1/runs` に任意の `parent` を足す。親を持つ実行は、start metadata と checkpoint metadata に親 ID を持つ。`engine.RunInfo` と `GET /v1/runs/{id}` も `parent` を返す。

start / checkpoint record は、既存の tier byte の未使用 bit で親の有無を示し、親がある場合だけ、その直後に長さ付きの親 ID を置く。親なしの record の byte 列は変えない。新しい decoder は旧形式を親なしとして読み続ける。

終了した子も冪等期間中は `Children` から見える必要があるため、終了マーカーを version 3 にして親 ID を残す。version 1・2 は親なしとして読み続ける。終了マーカーが期限切れで退役した時点で、その親子関係も忘れる。新しい表や、書き換える索引ファイルは作らない。真実は追記ログと終了マーカーに置き、メモリの索引は起動時に再構築する。

最初に耐久化した親を変更しない。同じ RunID の再投入で同じ親または親の省略は既存実行として扱い、別の親は parent-unavailable エラーにする。

### 2. 直接の子の集合は、親を所有する shard が持つ

親と子が別 shard でも、直接の子の集合と「親が新しい子を受け入れられるか」は親側の shard loop が所有する。

新しい親つき実行の Submit は、子を開始する前に親 shard の inbox へ attach を送り、次を確認して子 ID を予約する。

- 親が存在する未完了の `kairo.workflow` 実行である。
- 親が終端処理を開始していない。
- 子自身と同じ ID でない。
- 既存の子なら、保存済みの親と一致する。

Submit の goroutine は attach の返答を待てるが、親 shard loop は待たない。attach が成功した後に admission または子の開始が失敗した場合は、親 shard へ detach を返す。子の start が耐久化した後は、start record が関係の真実になる。

子が終端したときと終了マーカーが退役したときは、子 shard から親 shard の inbox へ通知する。終端後も ID と関係はマーカーの期限まで残すが、「未完了の子」からは外す。通知は I/O をせず、親 shard は待たない。

起動時は、全 shard の start / checkpoint record と終了マーカーを読んだ後、親つきの実行を親 shard の索引へ登録してから通常の回復を始める。shard 数と FNV の割り当ては変えない。

### 3. コードワークフローの終端を、親 shard の guard と同じ順序に置く

コードワークフローが成功または失敗を記録するための専用 API を足す。

`POST /v1/runs/{id}/finish`

body は既存の `done` signal と同じ payload とする。この要求は親 shard の inbox で attach と直列化し、直接の未完了の子がない場合だけ「新しい子を受け付けない」状態へ移してから、`done` signal を適用する。

- attach が先なら、finish は event を記録せず HTTP 409（children unsettled）を返す。SDK は `Children` で子孫を終端してから finish をやり直す。
- finish が先なら、その後の attach は parent-unavailable で失敗する。

`POST /v1/runs/{id}/cancel` も、親子索引のある workflow について同じ guard を通す。未完了の子があれば 409 を返し、SDK が子孫を終端して再試行する。従来どおり親の耐久化した終端は `/wait` で確認する。

通常の graph API の signal と event の意味は変えない。専用の finish は、`kairo.workflow` が終端用の wait に `done` を送る操作だけに使う。これにより、すべての engine event に「適用後に終端するか」の試行を足さない。

### 4. `Children` を HTTP と 3 SDK へ足す

`GET /v1/runs/{id}/children` は、親 shard が持つ直接の子 ID の snapshot を取り、各 ID の現在の `RunInfo` または終了マーカーを読む。結果は RunID の byte 順に並べる。子孫の再帰は SDK が行い、API は直接の子だけを返す。

次を kairod 接続時にも使えるようにする。

- Go: `k.Children(ctx, id)`
- TypeScript: `k.children(id)`
- Python: `await k.children(id)`

HttpBackend は、action call、wait、子ワークフローを開始するときに、すでに `RunStart` / `runSpec` にある親を HTTP の `parent` として送る。SDK の終端処理と `Cancel` は、埋め込みと同じ ADR 0062 の子孫走査を kairod にも行い、children-unsettled の 409 を受けたら走査からやり直す。

### 5. 親のある実行は、再投入で到達するまで回復しない

kairod の起動時、親なしの未完了実行は従来どおり `EvRecover` を適用する。親つきの未完了実行は、状態と親子索引だけを復元し、単独では `EvRecover` を適用せず、timer も再武装しない。

親の関数がリプレイされて同じ子 ID を Submit した時点を、その子への到達とする。既存 ID と保存済みの親が一致していれば、その child shard で `EvRecover` を適用し、期限切れの試行や timer を処理する。終わった子は記録済みの結果を返し、回復 event を増やさない。

外から親を付けた ADR 0065 の子も、同じ ID と親で外部から再投入された時点を到達とする。親のコードがその子を知らないため、親のリプレイだけでは回復しないことは変えない。

### 6. 外からの `Parent` を kairod でも受け付ける

決定 2 と 3 により、親の未完了確認、子の予約、親終端との順序を engine 内で原子的に決められる。そのため ADR 0065 の外部 Parent option を kairod でも受け付ける。

- Go: `kairo.Parent(parentID)`
- TypeScript: `{ parent }`
- Python: `parent=`

親なしの既存根または別の親を後付けすること、存在しない親、workflow でない親、終端済みまたは終端中の親は拒否する。エラーの意味は ADR 0065 と同じにする。

kairod の保持期間は ADR 0027・0050 の冪等期間であり、埋め込みの ADR 0054 の木ごとの削除へは変えない。親が未完了でも、終了した子の marker と kept output は kairod の `IdempotencyTTL` を過ぎれば消える。この既存の制約は `Children` と親指定を足しても変えず、長期保持を変える場合は別の判断とする。

### 7. 費用

親を指定しない既存の graph 実行には、attach、親子索引、問い合わせを足さない。start / checkpoint / done record の親 extension も、親が空なら書かない。

親つき実行は、開始時の親 shard への inbox message、直接の親子関係 1 件分のメモリ、終端時の通知を払う。これは実行の状態と寿命を正しい木へ耐久化する仕事である。workflow の finish / cancel は、直接の未完了の子がある場合だけ 409 と子孫走査を増やす。

常駐する追加 goroutine、実行ごとの goroutine、ポーリング、全 shard の停止、shard loop 内の I/O や待機は足さない（ADR 0041）。

## 結果

- kairod でも直接の親子を再起動後まで列挙でき、3 SDK の `Children` が同じ API になる。
- 親の終端後に競合で新しい子を作れず、親が返る前に未完了の子孫を終端する ADR 0062 の規則を kairod にも適用できる。
- 再起動後、親のコードが到達しない古い子の作用を単独で回復しない。
- 外部の仕事を kairod 上の未完了 workflow の木へ安全に付けられる。
- ログと終了マーカーの形式が拡張される。旧形式は親なしとして読み続けるが、新形式を読むには新しい kairod が必要になる。
- 親が動いている間の子の終了記録を冪等期間より長く保持する問題は解かない。

少なくとも次をテストする。

- 親子を別 shard になる ID で開始し、`Children` が active、evicted、finished、再起動後の子を同じ親の下に返す。終了マーカーの期限後には消える。
- 旧 start / checkpoint / done record を親なしで読める。親つき record は checkpoint とログ退役後にも関係を復元できる。
- 子の attach と親の finish / cancel を競合させる。子が先なら finish は 409 になり、親が先なら子は作られない。終端済みの親の下に未完了の子を残さない。
- 親なしの既存根、別の親、存在しない親、workflow でない親への attach を拒否し、同じ ID・親の再投入は冪等に扱う。
- kairod を、親つきの action call、wait、子 workflow の途中で再起動する。子を単独回復せず、親の再投入が同じ子へ到達した後だけ回復する。
- 親の成功、失敗、明示した cancel は、深い子孫を終端してから耐久的に返る。並列枝と外から付けた子についても同じにする。
- 親なしの graph 実行のログ byte、開始経路、回復経路に親子用の追加処理がないことを確認する。

## 検討した代替案

- **子 shard だけに親 ID を置き、`Children` で全 shard を走査する:** 列挙はできるが、親の終端と子の開始に 1 つの順序が付かない。問い合わせのたびに全実行を走査する。
- **engine 全体の mutex で親子を守る:** shard loop が共有ロックを待ち、関係のない shard の終端と開始まで干渉する。
- **親子専用の永続 DB / 書き換える索引を足す:** file WAL と別の真実ができ、両方の原子性と回復を解く必要がある。start metadata と done marker にあれば再構築できる。
- **RunID の `/` を親として読む:** 利用者の ID と区別できず、外部 Parent と一致しない。ADR 0054 で捨てた案を戻す。
- **kairod 再起動時に親つきの子もすぐ `EvRecover` する:** ADR 0062 が解いた、親の新しいリプレイが到達しない作用を先に動かす問題を戻す。
- **親を終端してから子孫を取り消す:** 終端直後の障害で子が残り、後から作用できる。attach との競合にも勝てない。
- **親 ID だけを足し、外部 Parent は引き続き拒否する:** 原子的な attach と finish の仕組みが同じ要件を満たすため、拒否を続ける理由がなくなる。
- **この ADR で List と suspend mode も足す:** List は全 root の索引と pagination、suspend はコード駆動の所有権と scheduler を決める別の変更である。Issue #11 の後続 ADR に分ける。
