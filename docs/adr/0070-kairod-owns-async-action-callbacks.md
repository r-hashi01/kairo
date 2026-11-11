# 0070. kairod が非同期 HTTP アクションのコールバックと期限切れを所有する

- 状態: 提案
- 日付: 2026-11-11

## 背景

HTTP(S) のアクションは、同期応答で結果を返すか、受理だけを応答して後からコールバックできる。埋め込みランタイムでは、アクションを呼んだ process が結果の受け口と期限付きの借用を持つ。これに対して kairod 接続では、SDK process は worker protocol で task を実行するだけであり、後から届く結果の所有権を kairod へ渡す形式がない。そのため各 SDK は、HTTP アクションが後から結果を返そうとした場合を結果不明として扱っている。

suspend mode の SDK process は、task が確定した結果または耐久的な待ちへ入れば終了できる。同じ process の URL をコールバック先にすると、invocation の終了、再配備、process 障害の後に結果を受け取れない。常駐する SDK process を必要とすれば、待機中の費用を状態の byte 数だけにする目的を満たさない。

kairod は実行状態、期限 timer、ログを所有するが、既存の状態は「executor に渡して結果を待つ試行」と「結果不明になった試行」しか区別しない。executor に渡したままにすると run は quiescent にならず、worker が切断するか kairod が再起動すると、実の作用は結果不明になる。一方、受理応答を成功や通常の wait として記録すると、まだ存在しない結果を確定したことになる。executor の task は終わっているが、外部で続く同じ試行の結果を待つ耐久状態が必要である。

worker protocol と callback API には互換性と信頼境界も必要になる。能力合意なしに新しい pending 結果を旧 daemon へ送ると、旧 decoder が未知のフィールドを無視し、空の成功と解釈し得る。また、kairod の control API は既定で loopback にあり、callback のために外部公開する認証面として設計されていない。公開する callback 入口から control API を到達可能にしてはならない。

## 決定

### 決定項 1（外部で続く試行を、quiescent な耐久状態として所有する）

kairod は、HTTP アクションが受理応答を返した試行を、executor から結果を待つ状態とは別の pending 状態として耐久化する。pending は同じ activation と attempt の継続であり、成功、失敗、通常の wait、新しい retry ではない。

pending へ移ると executor の task と worker credit は完了させ、run の executor inflight 数から外す。run はほかに実行中の task がなければ quiescent になれる。期限だけを timer として持ち、run ごとの goroutine、接続、polling は持たない。kairod の再起動後は期限 timer だけを再武装し、pending の試行を executor へ再発行しない。

期限までに結果が届けば、その試行の確定結果として適用する。期限が先なら結果不明として適用し、作用の型と冪等再試行の宣言に応じた既存の規則へ渡す。非冪等な実の作用は自動で再実行せず、確認待ちまたは定義で明示した失敗になる。

コールバックは pending の記録と競合できる。能力 URL が指す attempt がまだ executor 待ち、pending、または同じ attempt の結果不明だけを理由に確認待ちであり、その後の結果や attempt が記録されていなければ、コールバックを確定結果として受け付けられる。これにより、受理応答と pending の耐久化の間に process が停止しても、同じ試行の真正な結果を捨てない。別の結果、operator の解決、取り消し、期限切れ、または新しい attempt が先なら、コールバックは状態を変えない。

### 決定項 2（非同期の引き渡しは worker と daemon が接続ごとに合意する）

worker と kairod は、非同期 HTTP アクションの引き渡し能力を Hello の能力交換で双方向に合意する。worker は、その接続で daemon の callback を使える HTTP アクションを宣言する。kairod は callback の受け口と署名鍵を使えるときだけ能力を選択し、宣言されたアクションの task だけに callback capability を渡す。

能力が選択された接続だけが pending 結果を送る。新 worker と旧 daemon、旧 worker と新 daemon、または callback を設定していない新 daemon の組み合わせでは、同期結果は従来どおり扱い、受理応答は unsupported の結果不明として安全側に倒す。未知の pending を空の成功として扱わない。

能力と pending は既存フレームへの任意フィールドとして追加し、既存の同期 task と既存メッセージの意味を変えない。

### 決定項 3（callback は試行と期限に限定した bearer capability で認証する）

kairod は対象の run、activation、attempt、期限に結び付いた、256 bit の MAC 強度を持つ callback URL を発行する。URL の改変、別の試行への付け替え、期限後の利用は拒む。期限は kairod が action timeout から決め、worker または callback の送信者が延長できない。

callback URL は bearer capability である。外部から到達する URL は HTTPS とし、平文は loopback の開発・試験だけに限る。URL と capability をログ、trace、metrics、エラー本文へ出さない。callback の公開入口は control API と別の信頼境界に置き、その入口から plan 登録、run 操作、stats その他の control API を到達可能にしない。

受理した結果の監査可能性は通常の耐久 event が持つ。認証拒否と期限切れは、capability と本文を残さず、有限な理由別の運用記録として数えられるようにする。秘密値や結果 payload を access log へ出して監査の代わりにしない。

MAC の鍵は data directory、plan、log、snapshot、task の入力へ保存せず、process の起動時に秘密管理から渡す。kairod の再起動をまたいで同じ鍵を使えるようにする。無停止で鍵を更新するときは、発行には新しい鍵を使い、古い capability の最長期限までは以前の検証鍵も受け付ける。重複期間を設けずに鍵を失効した場合、未完了の試行は期限時に結果不明となり、成功扱いにはしない。

アクション側との共有 secret は worker が HTTP 呼び出しを署名するために引き続き使い、kairod へ渡さない。kairod は自分が発行した capability を callback の認証根拠とし、アクションごとの secret を保存しない。

### 決定項 4（結果を耐久化してから callback を確認する）

callback は HTTP アクションの既存の結果形式を、上限付きの本文で一つだけ受ける。有効な結果は対象 run の shard 順序へ入れ、run の durability tier が要求するところまで記録してから、受理済みと応答する。応答を確認できなかった送信者は、同じ capability と同じ結果を再送できる。

同じ callback の再送、callback と pending の順序逆転、別の終端との競合は、対象 shard の一つの順序で決める。最初に記録された結果だけが状態を変える。有効期限内の同じ capability が、すでにその attempt を確定した後に届いた場合は冪等に受理済みと応答し、状態とログを増やさない。不正な capability、期限切れ、存在しない対象は受理済みとしない。HTTP path、status code、JSON field の詳細は API 仕様で固定する。

callback で child call が終端した場合は、既存の parent ready 通知を使う。kairod から SDK process を起動する新しい webhook は追加しない。suspend の利用者は既存の一度きりの wake、または Tick の fallback で ready workflow を取り上げる。

### 決定項 5（非同期を使った試行だけが耐久化の費用を払う）

callback capability は、能力合意済みの worker が宣言した HTTP アクションを dispatch するときだけ作る。URL を使わない action と、能力を合意していない接続には作らない。

受理応答を返した試行だけが、pending の記録、期限 timer、後の callback または期限切れの記録を払う。pending は実行状態そのものとして既存の log と snapshot に含め、別の対応表や保存先を真実にしない。通常の同期結果には追加の耐久記録を足さない。

### この ADR で決定しないこと

- callback API の path、HTTP status、JSON field、設定名、内部の型と符号化手順は、実装と公開 API の仕様へ委ねる。
- 移行手順、試験ケース、障害注入の実行順、担当、進捗は、この判断を追跡する Issue と PR へ委ねる。
- TLS 終端、外部 URL の routing、秘密管理製品の選択は運用構成へ委ねる。ただし、callback 入口だけを公開し、control API を公開しない制約は変えない。
- callback 送信側の再試行間隔と回数は action service の運用へ委ねる。曖昧な応答では同じ capability を再送できるという契約だけを定める。
- kairod の signal の耐久応答、先取り、receipt は別の判断の範囲とする。

## 結果

- 良い影響: SDK invocation と worker process が終了しても、kairod が外部で続く同じ試行の結果と期限を所有できる。
- 良い影響: pending の前後で worker または kairod が停止しても、真正な callback、期限切れ、結果不明のいずれかへ安全に収束できる。
- 良い影響: pending は quiescent なので、serverless invocation を結果が来るまで占有せず、再起動後にも外部作用を再発行しない。
- 悪い影響: worker protocol、core event、snapshot、engine log、callback HTTP API の互換性を保つ対象が増える。
- 悪い影響: 公開 callback URL、TLS、安定した署名鍵とローテーション用の検証鍵を運用する必要がある。
- 中立な影響: callback capability は URL を持つ action ごとに作るが、同期応答だけなら pending の耐久記録は増えない。
- 新しく守る制約: 能力合意なしに pending を送らない。pending は executor inflight に数えない。callback は対象 attempt と期限を越えて状態を変えない。受理応答は結果の耐久化より先に返さない。callback の公開入口から control API を公開しない。

## 検討した代替案

- SDK process を callback の所有者のままにする: suspend invocation、再配備、process 障害の後に到達できず、常駐 process が必要になる。
- worker 接続を callback まで保持する: serverless invocation の時間上限と費用を解かず、接続断で結果不明になる。
- 受理応答を成功または通常の wait として記録する: 成功結果も期限時の出力もまだ存在せず、外部で続く試行と確定結果を区別できない。
- pending を executor inflight のままにする: run が quiescent にならず、suspend invocation が終了できない。
- 能力合意なしに任意の pending field を送る: 旧 daemon が未知の field を無視し、空の成功と解釈し得る。
- kairod が action ごとの共有 secret を持つ: tenant ごとの secret を登録・保存する境界が増える。daemon が発行した試行限定 capability だけを検証する方が秘密の範囲が小さい。
- callback 対応表を別ファイルまたはメモリに置く: log と snapshot とは別の真実になり、再起動や log 退役との整合が必要になる。
- callback と一緒に SDK 用 webhook を追加する: 認証、retry、deployment version の routing という別の永続仕事が増え、既存の ready と wake で表せる。

## 関連する ADR

- ADR 0004 の決定「出力コミット」: 実の作用は intent の耐久化後にだけ外へ出し、callback の結果も通常の event 順序へ入れる根拠。
- ADR 0009 の決定「ワーカープロトコル」: pull と credit の既存契約を保ち、pending で executor task を完了させる境界。
- ADR 0026 の決定「効果の種類との関係」: 取り消しは外部作用の結果を成功へ変えず、後から届く結果との競合を一つの順序で決める根拠。
- ADR 0041 の決定項 1（原則）: 非同期を使わない実行へ、対応表、polling、常駐処理、追加の耐久書き込みを課さない根拠。
- ADR 0052 の決定「コールバック（アクション → ランタイム）」: HTTP アクションの同期・非同期結果、署名、結果不明の既存契約。
- ADR 0069 の決定項 1（suspend は「call が決着または durable wait に入るまで」で返す）: pending を quiescent にして SDK invocation を終了できるようにする根拠。
- ADR 0069 の決定項 4（`Run`、`Signal`、`Resolve`、`Tick` を同じ ready 経路へつなぐ）: callback 後の親 workflow を既存の ready 経路へつなぐ根拠。
- ADR 0075 の決定項 1（Hello で任意能力を双方向に合意する）: pending の新旧 protocol を接続ごとに安全に合意する根拠。
- ADR 0075 の決定項 4（新旧の組み合わせを接続単位で安全側に倒す）: 能力を合意できない組み合わせを結果不明へ倒す根拠。

## 参照

- [Issue 18: Let kairod own async URL action callbacks and expiry](https://github.com/r-hashi01/kairo/issues/18)（2026-11-11確認）: 適用範囲、互換性、安全性、障害試験の受け入れ条件。
- [kairod worker が非同期 HTTP 結果を拒否する処理](../../sdk/go/kairod.go)（2026-11-11確認）: kairod 接続で pending の所有権を渡せない現状の一次資料。
- [worker 切断時に outstanding task を結果不明にする処理](../../protocol/server.go)（2026-11-11確認）: 接続が所有する task と切断時の扱いの一次資料。
- [step、timer、quiescent、restart の状態遷移](../../core/apply.go)（2026-11-11確認）: executor inflight、期限、再起動後の再発行と結果不明の現行意味論の一次資料。
- [HTTP アクションの request、callback、署名](../../httpaction/httpaction.go)（2026-11-11確認）: 3 SDK と共有する HTTP result 契約の一次資料。
- [kairod の control API handler](../../api/api.go)（2026-11-11確認）: callback を外部公開するときに分離が必要な既存の control surface の一次資料。
