# 0075. worker と kairod は graceful Drain の能力を接続ごとに合意してから使う

- 状態: 承認（2026-11-09）
- 日付: 2026-11-09

## 背景

worker protocol の graceful Drain は、worker から負の credit を送り、kairod が配信停止を zero credit で知らせ、その後に受理した Result を正の credit で知らせる。この意味を知る両者が接続した場合、worker は新しい task を受けず、手元の Result が kairod に受理されるまで待てる。

この意味を持たない kairod は、負の credit を未知または無効な値として無視する。新しい worker は配信停止の応答を待ち続けるため、相手が未対応なのか、対応した相手の応答が遅いのかを区別できない。呼び出し側の期限で打ち切れば停止できるが、rolling deploy のたびに期限まで待つ動作になり、計画停止が対応可否ではなく待ち時間で決まる。

Hello は worker から kairod へ一方向にだけ送られ、worker 名、action、最初の credit と任意の機能要求を持つ。JSON の未知フィールドと未知の frame type は読み飛ばすため、任意の能力情報を足しても古い peer の通常の task 配信は壊れない。一方、kairod から能力を肯定する応答はなく、worker は server の版だけから接続単位の意味を確認できない。

互換性は process 全体でなく接続ごとに必要である。同じ kairod に、Drain を知らない worker、負の credit だけを知る worker、能力合意を知る workerが同時に接続し得る。能力がない場合も `Close` と切断時の結果不明の扱いは維持し、non-idempotent な実の action を成功扱いしてはならない。

## 決定

### 決定項 1（Hello で任意能力を双方向に合意する）

worker は、利用したい任意の protocol 能力を名前の集合として Hello に載せる。kairod は能力集合を提示したHelloに対してだけ、理解して選択した能力の集合をHelloの応答として返す。選択は接続ごとに行い、worker が提示していない能力を kairod が有効にしてはならない。

kairod は能力応答を、その接続へ Task、Cancel、RunEnd その他の仕事を送る前に確定する。能力名は個別の意味と互換性を表し、server 全体の版番号から推測しない。能力の不在、未知の能力、選択されなかった能力は、その接続では未対応である。

### 決定項 2（graceful Drain は肯定された接続だけで始める）

新しい worker は、graceful Drain の能力が選択された接続でだけ配信停止を要求する。相手が能力応答を返さない、能力を選択しない、または能力合意を完了できない場合、Drain は未対応を表す固有の誤りで有界時間内に失敗する。未対応を配信停止の成功として扱わず、応答のない相手へ graceful Drain の要求を送って待ち続けない。

能力合意を始める前の workerとの互換性のため、対応する kairod は、能力提示のない接続から届く従来の負の credit による Drain も受理する。ただし、この受理は新しい workerが相手の対応を推測する根拠にはしない。

### 決定項 3（能力応答と Drain の完了確認を分ける）

能力応答は「この接続で graceful Drain の手順を使える」ことだけを表す。配信停止の完了、個々の Result の受理、Drain 全体の完了を表してはならない。

能力を合意した後も、Drain の成功には、kairod がその接続への配信を止めた確認と、worker がすでに受け取ったすべての task の Result をkairodが受理した確認の両方を要する。能力応答、配信停止の確認、Result の確認は互いに取り違えられない形にする。

### 決定項 4（新旧の組み合わせを接続単位で安全側に倒す）

- 新しい worker と新しい kairod は能力を合意し、graceful Drain の完了条件をすべて確認する。
- 新しい worker と古い kairod は通常の task を処理できるが、Drain は未対応の固有の誤りで失敗する。呼び出し側は `Close` するか、別の停止方針を選ぶ。
- 古い worker と新しい kairod は従来どおり task を処理する。能力合意を必須にせず、未知の応答を処理させない。
- 能力合意前の graceful Drain を持つ worker と新しい kairod は、従来の負の credit による手順を続けられる。
- mixed fleet では各接続の合意だけを使い、あるworkerの能力を別のworkerへ引き継がない。

Drain が未対応、失敗、または期限切れになった後に接続を閉じる場合、未確認の実の action は結果不明のまま扱う。能力の不一致を成功、確定した失敗、または再実行許可へ変換しない。

### この ADR で決定しないこと

- 能力名の文字列表現、合意を待つ内部タイマー、SDKごとの誤り型の名前は、worker protocol の実装仕様へ委ねる。
- process の停止期限、Drain 失敗後に `Close` するかどうか、rolling deploy の順序は、利用者の運用手順へ委ねる。
- start confirmation、workflow claim、WAL、Drain の運用指標は、運用metricsの課題へ委ねる。
- worker protocol 以外のHTTP API、保存形式、coreのイベントと命令は変更しない。

## 結果

- 良い影響: 新しいSDKは未対応のkairodでDrain期限まで待たず、対応可否を固有の誤りとして扱える。
- 良い影響: rolling deployとmixed fleetで、接続ごとの新旧の組み合わせを明示できる。
- 良い影響: 能力の肯定とDrainの成功を分け、未確認の実のactionを誤って成功扱いしない。
- 悪い影響: Hello が双方向になり、接続開始に能力状態と有界な合意待ちが増える。
- 悪い影響: 能力合意前のworkerを支えるため、kairodは提示済みの接続と従来の負のcreditを送る接続の両方を扱う。
- 中立な影響: 能力を持たない接続でも通常のtask処理と高速な`Close`は変わらない。graceful Drainだけが利用できない。
- 新しく守る制約: workerは肯定されていない任意能力を使わない。kairodは仕事の配信前に能力応答を確定する。能力応答だけでDrainを成功にしない。能力不一致は実のactionの結果を確定させない。

## 検討した代替案

- kairod の版番号を Hello で返す: feature の有無を版の大小へ結びつけ、backportや部分的な実装を正しく表せない。
- 負の credit を送って呼び出し側の期限まで待つ: 未対応と遅延を区別できず、計画停止が毎回期限まで滞留する問題を残す。
- 最初の zero credit を能力応答にも使う: Drain開始と能力応答が交差すると、能力の肯定を配信停止の完了と誤認し得る。
- 能力合意を必須にして、古いworkerとの接続を拒否する: rolling deployの新kairod・旧workerを動かせず、通常のtask処理まで同時upgradeを要求する。
- HTTP APIでkairodの能力を調べる: worker socketだけを使う独立workerに別のendpointと認証設定を要求し、確認したprocessと実際の接続が同一である保証も弱い。

## 関連する ADR

- ADR 0009（ワーカープロトコルは長さ付きframeとJSONを使い、pull・credit制にする）: 任意能力を合意する既存の通信境界。
- ADR 0026（中断した手順は実行中taskにも取消を届ける）: 新旧workerが未知のframeを読み飛ばす既存の互換性方針。
- ADR 0073 の決定項 2（Closeとクラッシュの意味は変えない）: 能力がない場合も高速なCloseと結果不明の扱いを維持する。
- ADR 0073 の決定項 3（workerは受信停止と完了待ちを分ける）: 能力合意後に使うgraceful Drainの完了条件。

## 参照

- [Issue 16: Negotiate graceful worker-drain capability across kairod versions](https://github.com/r-hashi01/kairo/issues/16)（2026-11-09確認）: 新旧peerの互換性要件と受け入れ条件。
- [`protocol.Hello` とworker protocolのframe定義](../../protocol/protocol.go): 一方向のHello、credit、未知frameを読み飛ばす前提の一次資料。
- [`protocol.Server` の接続処理](../../protocol/server.go): Hello受理後にpollerを作り、負のcreditで配信停止する実装の一次資料。
- [`protocol.Worker.Drain`](../../protocol/worker.go): 配信停止確認とResult受理確認を待つ実装の一次資料。
