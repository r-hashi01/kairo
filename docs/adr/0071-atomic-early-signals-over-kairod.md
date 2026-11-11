# 0071. kairod の論理 Signal は耐久化した wait outcome を receipt とし、再送で二重配送しない

- 状態: 提案
- 日付: 2026-11-09

## 背景

コードで書くワークフローでは、同じ名前の `WaitFor` を繰り返せる。各 wait はワークフロー、signal 名、0 から始まる出現番号で決まる子実行である。埋め込みランタイムは、若い番号から現在の wait を探し、まだ到達していなければ次の wait を signal 済みで先に作る。送信側には、受理した wait の実行 ID、出現番号、先取りかどうかを receipt として返す。

kairod の既存の signal 要求は、event を shard の inbox に渡した時点で成功を返す。呼び出し側は、その event が wait に適用されたか、別の signal・timeout・取り消しが先だったか、必要なログが耐久化したかを区別できない。このため、現在の wait だけに届ける判断、先取りの作成、受理した wait の receipt を、複数 process から同じ意味で使えない。

HTTP 応答が失われる場合も考慮が要る。payload だけでは、同じ payload を持つ別の送信と自分の再送を区別できない。応答を確認できないまま次の番号へ進むと、1 つの論理 signal が複数の wait に届きうる。一方、すべての workflow に signal queue や対応表を常設すると、signal が来ない実行にも費用を足し、wait child と別の真実を作る。

先取りでは、親 workflow と wait child が異なる shard に置かれうる。親への attach と子の作成全体を 1 つの保存先トランザクションにはできない。親の終端との順序は親 shard が決め、子 shard では「先取りの wait と最初の signal」を分離して復旧しない性質が必要になる。

## 決定

### 決定項 1（受理した signal の判断を耐久化してから成功を返す）

kairod に、1 つの wait child を対象として signal の適用結果を同期して返す経路を設ける。

signal が wait を終わらせた場合は、signal event と receipt の再現に必要な情報が、その実行の tier が求めるところまで耐久化してから成功を返す。別の signal、timeout、取り消し、終端が先で signal を受理しなかった場合は、従来どおり event を記録しない。存在しない wait への不受理も、将来の signal を作らない時点判断とする。

要求の context が成功応答より先に終了した場合、signal は受理済みかもしれない。呼び出し側は、決定項 2 の同じ論理 signal ID で再送して結果を確定する。

### 決定項 2（論理 signal ID を受理した wait outcome に残す）

receipt を要する送信には、payload とは別の論理 signal ID を持たせる。SDK は送信ごとに衝突しにくい ID を 1 つ作り、内部の通信再試行では同じ ID を使う。process の再起動や別 process から同じ送信を再試行する利用者は、同じ ID を指定できる。

wait が signal を受理した outcome には、利用者へ渡す payload を変えずに、論理 signal ID と先取りかどうかを残す。この outcome と決定的な wait の実行 ID から、同じ ID の再送は最初と同じ receipt を返す。別の ID は、その wait を既に決着させた別の送信として次の番号を調べる。

同じ論理 signal ID は、最初の payload と送信方式に結び付ける。異なる内容での再利用は新しい signal とみなさず、競合として拒否する。再送保証の保持期間は、wait child の結果を冪等に読める期間と同じにする。その期間を過ぎた ID を新しい送信へ再利用してよいとはしない。

### 決定項 3（wait の出現順で勝者を決め、only-if-waiting は先取りを作らない）

3 つの SDK は同じ signal 名の wait を出現番号順に調べる。未完了の wait では、その wait の shard が signal の受理と不受理の順序を決める。自分の論理 signal ID を持つ完了済みの wait を見つけた場合は、その receipt を再び返す。

only-if-waiting の送信は、現在の wait が signal を受理した場合だけ成功する。現在の wait がなくなった時点で `NotWaiting` とし、将来の wait、mailbox、予約を作らない。`NotWaiting` はその時点の状態を表し、将来も同じ結果を予約する記録ではない。確定した `NotWaiting` の後に改めて送る操作は、新しい時点の判断になる。

先取りを許す送信は、次の出現番号の wait child を signal 済みで作る。同じ番号を競合して作った別の送信は、作成済みの outcome を読み、自分の ID でなければ次の番号へ進む。これにより、各論理 signal は高々 1 つの wait にだけ受理される。

### 決定項 4（先取りの wait と最初の signal を 1 つの復旧単位にする）

先取りで新しく作る wait child は、その開始と最初の signal を子 shard 上の 1 つの復旧単位として保存する。障害後に見える状態は、signal 済みの wait が存在するか、その wait 自体が存在しないかのどちらかにする。payload のない空の先取り wait だけを復旧してはならない。

親 workflow への attach は、既存の親 shard の guard を先に通す。親の終端が先なら child を作らず、attach が先なら child の耐久メタデータから親子関係を再構築する。これは親 shard と子 shard をまたぐ原子トランザクションを追加する判断ではない。

この復旧単位を表すために保存形式を拡張してよい。新しい reader は既存のログと snapshot を読み続ける。形式の具体的な符号化はこの ADR では固定しない。

### 決定項 5（適用範囲をコードワークフローの wait に限定する）

この receipt、先取り、論理 signal ID の契約は、SDK が所有するコードワークフローの wait child に適用する。既存の receipt を返さない送信 API は、この経路を使って receipt を捨てる。これにより、埋め込みと kairod で、wait より先に来た signal の意味を揃える。

任意の graph run に対する既存の非同期 signal API は変更しない。新しい能力を持たない kairod へ receipt 付き送信を接続した場合は、保証を弱めて非同期送信へ切り替えず、未対応として失敗させる。

論理 signal ID は冪等性のための識別子であり、認証情報にはしない。signal を送れる主体の認証・認可は、既存の API の信頼境界に従う。

### 決定項 6（signal が来た経路だけが追加費用を払う）

論理 signal ID と receipt の情報は、受理した wait outcome に置く。signal 専用の常設 queue、全 workflow が持つ対応表、実行ごとの goroutine、polling は追加しない。

同期の耐久化待ち、競合した番号の確認、先取り child の作成は、signal を送った要求だけが払う。signal のない通常実行と任意の graph run の event hot pathには、receipt 用の状態や待ち合わせを追加しない。

### この ADR で決定しないこと

- HTTP の path、request / response の JSON、言語ごとの option 名、保存形式の具体的な符号化は、実装と公開 API の仕様で決める。
- 実装順、個別の障害注入・競合テスト、完了条件は、この判断を追跡する Issue で管理する。
- wait child の結果を既存の冪等期間より長く保持することと、期間を過ぎた receipt の永続的な検索は決定しない。
- 任意の graph run の signal を同期 API へ移すことは決定しない。

## 結果

- 良い影響: 成功した送信は receipt と payload が耐久化済みであり、応答が失われても同じ論理 signal ID から同じ receipt を確定できる。
- 良い影響: 複数 process の競合でも 1 つの論理 signal は高々 1 つの wait に届き、only-if-waiting は将来の signal を残さない。
- 悪い影響: receipt 付き送信は非同期の signal より応答が遅く、競合数に応じて複数の wait outcome を読む。
- 悪い影響: process をまたぐ再試行を必要とする利用者は、論理 signal ID を保持して再利用する必要がある。
- 中立な影響: 任意の graph signal の非同期契約と、signal のない実行の状態・実行経路は変わらない。
- 中立な影響: 保存形式は拡張しうるが、新しい実装は既存のログと snapshot を読み続ける。
- 新しい制約: 成功応答より前に、受理した event、論理 signal ID、先取り区分を同じ wait outcome として耐久化しなければならない。
- 新しい制約: 障害から空の先取り wait だけを復旧してはならず、同じ ID と異なる内容の送信を別の wait へ届けてはならない。

## 検討した代替案

- payload を再読して自分の送信か判定する: 同じ payload を別の送信が持てるため、再送の識別にならない。
- 応答を確認できなければ次の番号へ進む: 最初の送信が受理済みだった場合、同じ論理 signal を 2 つの wait へ届ける。
- 親 workflow に signal queue と通し番号を持つ: signal がない workflow にも状態を足し、wait child と別の真実を同期する必要がある。
- signal 専用の耐久 ledger を別に持つ: 不受理を含むすべての試行を保存できるが、wait outcome との原子性、保持、退役を新たに解く必要がある。受理した outcome が再送の真実になれば足りる。
- 親と child の全 shard を 1 つのトランザクションにする: shard loop の所有権と独立性を失う。親の attach guard と、子の復旧単位を分ければ必要な性質を満たせる。
- 既存の非同期 signal をすべて同期化する: receipt を必要としない graph API の互換性と費用を変える。
- receipt を無期限に保持する: 既存の実行結果と RunID の冪等期間を越える別の索引と退役規則が必要になる。

## 関連する ADR

- ADR 0023 の決定項 2（冪等性）: 同じ RunID の開始を、結果を確認できない要求の再試行に使う既存の境界。
- ADR 0041 の決定項 1（原則）: signal がない通常実行に常設の費用を足さない根拠。
- ADR 0059 の決定項 2（待ちより先に来たシグナルを、待ちの実行として先に作っておく）: 先取り signal と決定的な wait child の意味。
- ADR 0064 の決定項 1（埋め込みランタイムに、receipt を返す送信 API を足す）: 3 SDK が返す receipt の既存契約。
- ADR 0064 の決定項 2（`only if waiting` は、現在の wait がなければ先取りしない）: kairod でも保つ only-if-waiting の意味。
- ADR 0067 の決定項 2（直接の子の集合は、親を所有する shard が持つ）: 親の終端と先取り child の attach に順序を付ける仕組み。
- ADR 0069 の決定項 4（`Run`、`Signal`、`Resolve`、`Tick` を同じ ready 経路へつなぐ）: signal 後に suspend workflow を再駆動する既存の経路。

## 参照

- [Issue 19: Support durable signals-ahead and atomic receipts over kairod](https://github.com/r-hashi01/kairo/issues/19)（2026-11-11確認）: 対象範囲、再送の重複防止、互換性、検証の受け入れ条件。
- [engine の Submit と signal API](../../engine/engine.go)（2026-11-11確認）: RunID の冪等性、親 attach、既存の非同期 signal の事実。
- [engine shard の event 適用と耐久化待ち](../../engine/shard.go)（2026-11-11確認）: 無視された event を記録しないことと、耐久化後に応答できる既存機構の事実。
- [Go SDK の signal receipt と出現番号探索](../../sdk/go/kairo.go)（2026-11-11確認）: 埋め込みの receipt、only-if-waiting、先取りの既存契約。
- [core の signal 適用](../../core/apply.go)（2026-11-11確認）: 最初の signal だけが wait を終わらせ、後続を無視する事実。
