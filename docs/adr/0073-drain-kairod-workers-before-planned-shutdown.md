# 0073. 計画停止では、kairod のworkerが新しい仕事を止め、実行中の結果を記録してから閉じる

- 状態: 承認（2026-11-09）
- 日付: 2026-11-09
- 関連: 不変条件 4・5・6 / ADR 0026（実行中タスクの取り消し）、0041（実行ごとのコスト）、0049（SDK API）、0059（駆動権）、0069（kairod のコードワークフロー）/ GitHub Issue #12

## 背景

kairodへ接続したSDKの `Close` は、worker接続とワークフロー駆動を停止する。workerがnon-idempotent real actionを実行中なら、kairodからは結果が届いたか分からないため、その手順は正しく `blocked` になる。

クラッシュや強制終了では、この安全側の停止が必要である。一方、deploy、autoscaling、利用者が明示したshutdownのような計画停止では、新しいtaskの受け取りを止め、すでに実行中のtaskの結果をkairodへ返して耐久化してから接続を閉じられる。

2026-11-09の複数process qualificationでは、自分が開始したworkflowを終えたprocessが先にworkerを閉じたとき、別processのworkflowから渡されたreal actionを1件実行中だった。その結果は不明になり、親は確認待ちになった。試験harnessは全processのbarrierで回避したが、実運用のprocessには全体barrierがない。

埋め込みランタイムには `Drain` があるが、kairodへ接続したworkerには使えない。`Close`を常に待つ動作へ変えると、打ち切りたいprocessが終われなくなるため、計画停止と強制停止をAPIで分ける必要がある。

## 決定

### 1. 3 SDKのDrainをkairod接続でも使えるようにする

- Goの `Kairo.Drain(ctx)`、TypeScriptの `k.drain({ signal })`、Pythonの `await k.drain()` を、埋め込みだけでなくkairod接続でも使えるようにする。
- Drainを始めたprocessは、新しいworkflow駆動をclaimせず、新しいworker taskのcreditを出さない。
- すでにclaimしたworkflowは、次のcall境界まで進めて駆動権を譲る。新しいaction callをこのprocessで始めない。
- すでにworkerが受け取ったtaskは、handlerが返り、その結果をkairodが受理するまで待つ。kairodの耐久化ackがある結果は確定済みとして扱える。
- すべてのin-flight taskと駆動が上の地点へ着いたらDrainが返る。その後に `Close` すると、結果不明を新しく作らず接続を閉じられる。

### 2. Closeとクラッシュの意味は変えない

- `Close`だけを呼んだ場合は、現在と同じくprocess停止として速やかに接続を切れる。永遠に返らないhandlerを待たない。
- processの強制終了、ネットワーク切断、Drainの期限切れ後にCloseした場合、実行中だったreal actionはこれまでどおり結果不明として `blocked` になり得る。
- Drainは安全性を緩める機能ではなく、計画停止時に結果不明を作る確率を下げる機能である。

### 3. workerは受信停止と完了待ちを分ける

- `protocol.Worker` に、接続を直ちに切る停止とは別のdrain制御を足す。
- drain開始後は新しいpull creditを送らず、すでに受信したtaskのhandlerとresult送信を待つ。新しいframe形式は足さない。既存のcredit制御で、受け取らないことを表す。
- kairod側が停止または切断した場合は、Drainも結果を確定できないのでエラーで返す。SDKはそのtaskを成功扱いしない。
- taskごとのgoroutineや定期pollingは増やさない。workerがすでに持つin-flight数と完了通知で待ち合わせる。

### 4. 観測可能にする

Drainの開始、受信停止、待っているworker task数、待っているworkflow駆動数、完了、期限切れをSDKのlogger / observationへ出す。耐久ログのイベントは増やさない。

## 結果

- rolling deployやscale-inで、処理中のreal actionを不要に `blocked` へ送らずにprocessを止められる。
- 終わらないhandlerがあるとDrainも終わらないため、利用者はcontext / AbortSignal / asyncio timeoutで待つ上限を決める。上限後にCloseするかは利用者の停止方針である。
- processはDrain中もin-flight taskの結果を返すために生きている必要がある。クラッシュ耐性の代わりではない。
- worker protocolのframe形式、コアのイベント、WAL、スナップショットは変えない。
- テスト:
  - real action実行中にDrainを始める。新しいtaskはそのworkerへ渡らず、実行中の結果を耐久化してからDrainが返る。
  - 複数taskをcredit内で実行中でも、全結果を返すまで返らない。
  - Drain中のworkflowは次のcallを始めず、別processが駆動を引き継ぐ。
  - Drainのcontextを切ってCloseすると、終わらなかったreal actionは結果不明になり、再実行されない。
  - Drain後のCloseでworker disconnectのoutstanding taskが0件になる。

## 実装

2026-11-09にprotocol serverと3 SDKへ実装した。既存のCredit frameを次のように使う。

- workerからの負のCredit: この接続の配信を止める要求。
- kairodからのCredit 0: writerを止め、未送信taskをdispatcherへ戻したack。このackより前に送ったTaskだけがdrain対象になる。
- drain中のkairodからの正のCredit: Resultを1件受理したack。workerはhandler完了だけでなく、このackまで待つ。

kairodがResultを受理した後は、接続を閉じてもengineがそのイベントの耐久化を所有する。worker接続自体を耐久化ackの経路にはしない。接続がack前に切れた場合、Drainはエラーとなり、従来どおり結果不明を成功扱いしない。

## 検討した代替案

- **Closeを常にgracefulにする:** 終わらないhandlerが1つあるとprocessを停止できない。強制停止との区別も失う。
- **利用者が全workflowの完了barrierを作る:** workerは別processが開始したworkflowのactionも受けるため、自分のworkflowだけを待っても安全ではない。全体barrierはautoscalingに使えない。
- **Drain開始時に実行中taskを別workerへ再配布する:** non-idempotent real actionはすでに作用した可能性があり、不変条件5に反する。
- **kairodへ新しいdrain frameを送る:** pullとcreditがすでにあり、workerが新しいcreditを出さなければ受信停止を表せる。まずframe形式を増やさず実現する。
- **Drain中も新しいworkflowをclaimし、actionだけ止める:** 新しいcall地点で止まるworkflowを増やし、停止時間を延ばす。計画停止を始めたprocessは新しい駆動を取らない。
