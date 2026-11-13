# 0079. SQLite の別 process の決着はローカル通知で起こし、保存先を再読取して確定する

- 状態: 提案
- 日付: 2026-11-13

## 背景

ファイルの SQLite を保存先にする埋め込みランタイムは、同じデータベースを複数の process から開き、借用によってワークフローを引き継げる。一方、実行の決着を待つ待ち手は各 process のメモリにあり、SQLite の保存先には、別 process が実行を決着させたことを知らせる経路がない。

待ちモードでは、process ごとに一つある既存の sweep が、借用の期間ごと（既定 30 秒）に待機中の実行を保存先から読み直す。このため、別 process が送った signal、終えた子の呼び出し、または決着させた実行は正しく反映されるが、待っている process が気付くまで最大で一つの借用期間を要する。PostgreSQL の保存先は、実行を決着させるトランザクションで通知を送り、process ごとの一つの listener が受けて行を読み直すため、この遅れがない。

SQLite は process 内で動くライブラリであり、別の server process や process 間通信を持たない。別接続による変更は `data_version` を問い合わせれば検知できるが、変更を即時に押し出す API ではない。短い間隔で問い合わせれば遅れを縮められるものの、待機中のための polling が増える。

SQLite と同じホストで動く普通のアプリや CLI では、別 process から承認やメッセージを送った直後にワークフローが続く必要がある。同時に、常駐する別サービスを必須にせず、実行ごとの goroutine・task・thread と polling を足さず、データベースを正しい状態の唯一の根拠に保つ必要がある。SQLite の commit と外部の通知を一つの原子的な操作にはできないため、commit 後で通知前に process が停止する場合も残る。

## 決定

### 決定項 1（同一ホストのファイル SQLite に任意のローカル通知を設ける）

Go・TypeScript・Python の埋め込みランタイムに、ファイルを使う SQLite の同じデータベースと名前空間を共有する process 間で、実行の決着を知らせる任意のローカル通知能力を設ける。この能力は、待ちモードで別 process の決着を直ちに再確認したい配置が明示して使う。指定しない配置は、既存の sweep による再確認を続ける。

通知を有効にした名前空間で即時の起床を保証するには、その名前空間へ書くすべての process が同じ通知能力を有効にする。対応しない process が決着を書いた場合も正しさは失わないが、次の sweep まで遅れ得る。メモリ内の SQLite、別ホストから共有するデータベース、kairod、PostgreSQL はこの通知の対象にしない。

### 決定項 2（process ごとのファイルソケットへ commit 後に通知する）

通知を使う各 process は、データベースの正規化したファイル名と保存先の名前空間から定める sidecar directory に、process ごとに一つのローカルな stream socket を開く。実行を決着させた process は、SQLite の commit が成功した後、その directory にある listener へ、決着した実行の ID を fan-out する。別の broker process、実行ごとの listener、固定間隔の確認は設けない。

ローカル通知は、三つの SDK が相互に送受信できる、版を持つ有界な取り決めにする。process ごとの socket は process の終了時に除き、異常終了で残った socket は到達不能であることを確認した送信側が有界に片付けられるようにする。通知能力を明示したのに listener を開始できない場合は、ランタイムの開始を失敗させ、即時通知があるように見せたまま黙って sweep だけへ弱めない。

### 決定項 3（通知は起床のヒントとし、保存先を正典にする）

通知を受けた process は、通知の内容を実行結果として使わず、対象の実行を SQLite から読み直す。保存先の状態が実際に決着している場合だけ待ち手と親の再駆動を起こす。重複、順序の入れ替わり、古い通知、関係のない通知は、余分な読取を生むだけで実行の状態を変えない。

通知は必ず commit の後に送り、送信の失敗を、成功した SQLite のトランザクションの失敗として返さない。commit 後の停止、socket の一時的な失敗、通知を使わない process からの書込みに備え、既存の process 単位の sweep は、listener の有無にかかわらず、待機中の実行を借用期間ごとに再確認する。通知の欠落は応答を遅らせても、ワークフローを失わせない。

### 決定項 4（通知の信頼境界を SQLite ファイルとそろえる）

sidecar directory と socket は、対応する SQLite ファイルへ書ける process と同じ信頼範囲に置き、外部ネットワークから到達させない。通知には、取り決めの版、名前空間、実行 ID 以外の、入力・出力・資格情報・作用の結果を含めない。受信する長さと形式を制限し、検証できない通知は捨てる。

socket や sidecar directory は耐久状態、実行記録、監査記録にしない。それらを失っても SQLite の状態と既存の sweep から回復できなければならない。同じデータベースを異なるパスや異なる名前空間として開いた process 間には、即時通知を保証しない。

### この ADR で決定しないこと

- 公開 API の名前、sidecar directory と socket の具体的な命名、フレームの符号化、接続期限、同時送信の内部手順は、各 SDK の公開 API 文書と保存先実装に委ねる。
- Windows の named pipe など、三つの SDK が同じ取り決めでファイルソケットを使えない platform の即時通知は扱わない。その platform では既存の sweep、PostgreSQL、または kairod を使う。
- SQLite をネットワークファイルシステム上で別ホストから共有する配置と、複数ホストへ通知を配送する仕組みは扱わない。
- 任意の表の変更を配る change feed、耐久イベント購読、実行の途中経過、可観測性のイベントは扱わない。通知の対象は、既存の保存先契約が別 process の待ち手を起こすために印を付ける実行の決着に限る。
- PostgreSQL の `LISTEN/NOTIFY` と kairod の通知経路は変更しない。

## 結果

- 良い影響: 同じホストで一つの SQLite ファイルを使う process は、常駐する別サービスと polling を足さずに、別 process の signal や実行の決着で待ちを直ちに進められる。
- 良い影響: 通知を正典にしないため、欠落・重複・異常終了があっても、SQLite の状態と既存の sweep から回復できる。
- 良い影響: listener は通知を使う process ごとに一つで、待っている実行の数に応じた goroutine・task・thread・socket を持たない。
- 悪い影響: 決着ごとに、そのデータベースを聞く process の数に比例するローカル接続と送信が増える。多数 process の高頻度な決着には、PostgreSQL または kairod の方が適する。
- 悪い影響: ファイルソケットを共通に使えない platform と、異なるパスから同じファイルを開く配置では、即時通知を利用できない。
- 中立な影響: sidecar directory と socket は一時的な起床経路であり、SQLite の表、イベントの意味、リプレイ結果、実行の耐久性は変わらない。
- 新しく守る制約: 通知は commit 後にだけ送る。通知失敗で成功済み commit を失敗として返さない。受信時は SQLite を再読取する。listener の有無にかかわらず既存の sweep を回復経路として残す。process ごとの listener と送信先の掃除を有界にし、実行ごとの listener や polling を設けない。

## 検討した代替案

- `PRAGMA data_version` を短い間隔で問い合わせる: 別接続の commit は検知できるが、変更がない間も問い合わせ続け、どの実行が変わったかを知るにはさらに読取が要る。
- SQLite のデータベースまたは WAL ファイルを OS の file watcher で監視する: platform ごとの API または依存が要り、ファイルの変化と Kairo のトランザクションの commit・対象実行を直接対応させられない。
- 一つの well-known socket を一つの process が所有して broker になる: 送信は一か所で済むが、所有 process の停止、再選出、接続の張り直しが必要になり、埋め込み配置に別の coordinator を作る。
- SQLite を使う配置でも kairod を必須にする: 一つの process が通知と実行を所有できるが、DB だけを共有する埋め込み配置に常駐サービスを追加する。
- 通知を SQLite の commit と同じ成功条件にする: SQLite と socket 送信を原子的にできず、commit 後の送信失敗を返すと、呼び出し側が成功したイベントを再適用する危険がある。

## 関連する ADR

- ADR 0041 の決定項 1（原則）: listener を process ごとに一つにし、実行ごとの監視と polling を足さない根拠。
- ADR 0051（常駐するサーバーを前提にしない）: DB だけを共有する埋め込み配置を保つ。承認された場合、同 ADR の実装で詰めた詳細「段階 3b: ほかの process で終わった実行の待ち」にある「SQLite は通知を持たない」という範囲を、この ADR が一部置き換える。
- ADR 0058 の決定項 2（埋め込みのランタイム）: Go の任意の通知差し込み口と、三つの SDK が同じ保存先の取り決めを使う性質を SQLite に広げる。
- ADR 0059 の決定項 1（ワークフローを動かす側にも借用を付ける）: 借用期間ごとの sweep と再確認を回復経路として残したまま、別 process の決着を早く伝える。

## 参照

- [Issue 8: With SQLite, other processes do not learn of new runs, signals or finished runs](https://github.com/r-hashi01/kairo/issues/8)（2026-11-13確認）: SQLite の process 間通知と承認の遅延に関する要求。
- [SQLite: How SQLite Works](https://www.sqlite.org/howitworks.html)（2026-11-13確認）: SQLite が別 server process と process 間通信を持たないことの一次資料。
- [SQLite: PRAGMA data_version](https://www.sqlite.org/pragma.html#pragma_data_version)（2026-11-13確認）: 別接続の commit を問い合わせで検知できることの一次資料。
- [Go standard library: `net.Listen`](https://pkg.go.dev/net#Listen)（2026-11-13確認）: Go が Unix domain socket の stream listener を標準ライブラリで提供することの一次資料。
- [Node.js `net.Server.listen(path)`](https://nodejs.org/api/net.html#serverlistenpath-backlog-callback)（2026-11-13確認）: TypeScript のホストが Unix domain socket の stream listener を標準機能で提供することの一次資料。
- [Python `asyncio.start_unix_server`](https://docs.python.org/3/library/asyncio-stream.html#asyncio.start_unix_server)（2026-11-13確認）: Python のホストが Unix domain socket の stream listener を標準ライブラリで提供することの一次資料。
- [Go SDK の通知契約と fallback](../../sdk/go/store.go): `Notifier` と通知がない場合の保存先契約の一次資料。
- [Go SDK の待機と再確認](../../sdk/go/runtime.go): 通知受信時と lease 周期の再確認で保存先を読み直す現行動作の一次資料。
- [Go SDK の PostgreSQL listener](../../sdk/go/pgnotify/pgnotify.go): process ごとに一つの listener で実行の決着を受ける現行実装の一次資料。
- [TypeScript SDK の保存先契約](../../sdk/ts/src/store.ts): SQLite と PostgreSQL の通知能力の差の一次資料。
- [Python SDK の保存先契約](../../sdk/python/kairo_sdk/store.py): SQLite と PostgreSQL の通知能力の差の一次資料。
