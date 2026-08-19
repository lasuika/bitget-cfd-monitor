# CFD 跟單「開倉秒級通知」可行路徑研究

日期:2026-08-19
範圍:Bitget CFD(MT5)跟單者,如何在被跟單交易員開倉、自己跟單帳戶成交後「幾秒內」收到通知。只讀研究,不動專案程式碼。
已排除(前次研究):跟單帳戶 MT5 投資者登入(官方:copier 不能登入)、Bitget 公開 API(不含 CFD)、MetaApi。

結論先講:**Bitget 沒有任何文件寫明「CFD 跟單成交會推播/寄信」**;官方 API/WebSocket 完全不含 CFD/MT5。所以秒級通知只能靠「Bitget App 或 Email 已經在發的通知 → 轉發」或「自己登入態的網頁端點輪詢」。第一步是先實測:下次開倉時,iPhone 上 Bitget App 有沒有推播、Gmail 有沒有信。這一件事決定後面 80% 的路。

---

## Q1. Bitget App/網頁通知設定:有沒有跟單成交的推播/Email/SMS?

1. 官方公告(2025-06-26 起)只寫「交易相關通知優先以 Email 發送,例如強平/預警;沒綁 Email 才用 SMS」,舉例只有風險警示,**沒提跟單、CFD、MT5、成交回報**。https://www.bitget.com/support/articles/12560603830399
2. CFD Copier Guide 全文**沒有任何通知/推播/Email 字眼**;copier 只能在 App「Copy Details → Positions」看部位,文件沒寫更新頻率。https://www.bitget.com/support/articles/12560603873726
3. 跟單者 FAQ 唯一的「提醒」功能是「名額提醒(slot reminder)」,不是成交提醒。https://www.bitget.com/support/articles/12560603801044
4. 期貨跟單相關文章(規則、限制、平倉)同樣沒有成交通知說明。https://www.bitget.com/support/articles/12560603800577 、https://www.bitget.com/support/articles/8193498034841
5. Bitget 官方 Telegram「Signal Bot」是給訊號提供者/複製訊號用的工具,不是帳戶成交通知。https://www.bitget.com/support/articles/12560603816739 、https://www.bitget.com/academy/bitget-copy-trade-telegram-signal-bot
6. 社群工具(如 GitHub `Fruchtii/bitget-notification-bot`)是用**期貨跟單 API 輪詢**後推 Telegram,權限只有 Futures/Copy trading,不含 CFD。https://github.com/Fruchtii/bitget-notification-bot

**判定**:App 內「通知設定」實際有哪些開關,官方無文件;必須自己在 App 內查(設定 → 通知)並實測。信心:高(文件缺席是確定的)。

## Q2. Bitget WebSocket 私有頻道:有跟單成交流嗎?CFD 呢?

1. 舊版(v1)跟單 API 只有 REST:`GET /api/mix/v1/trace/followerOrder`(跟單者持倉)、`followerHistoryOrders`;文件明寫無 WebSocket,範圍只有現貨/合約。https://bitgetlimited.github.io/apidoc/en/copyTrade/
2. v2 跟單 API(`/api/v2/copy/mix-follower/...`,例如 copy-settings、query-current-orders)一樣是 REST,只涵蓋 Futures/Spot copy。https://www.bitget.com/api-doc/copytrading/future-copytrade/follower/Settings 、https://www.bitget.com/api-doc/copytrading/future-copytrade/follower/Query-Current-Orders
3. UTA「Elite Trading API Guide」明寫只支援「elite trading futures pairs」下單/查詢;無 WebSocket 跟單頻道、無 CFD/TradFi/MT5。https://www.bitget.com/api-doc/uta/copy/Elite-Trading-API-Guide
4. 私有 WS 頻道(orders/positions/account)只針對 SPOT / USDT-FUTURES 等 instType,沒有 MT5 產品。https://www.bitget.com/api-doc/contract/websocket/private/Order-Channel 、https://www.bitget.com/api-doc/uta/websocket/private/Order-Channel

**判定**:此路不通。信心:高。

## Q3. 登入態的 CFD 跟單頁內部端點

1. 公開可查的只有專案已用的匿名端點 `/v1/trace/mt5/public/{currentPosition,historyPosition,details,performance,getTransferHistory}`(本 repo `cfd.js`),部位延遲約 60 分鐘。GitHub/社群**沒有**任何關於 copier 專屬(登入態)MT5 端點的公開寫法(搜尋 `trace/mt5`、`cfd copy` 皆無)。
2. 「Copy Details」頁在網頁版登入後一定會打某些需要 session 的 XHR;要靠使用者自己開 DevTools → Network 找路徑與必要 headers。這是使用者自己的帳戶,技術上可行,但**未文件化、隨時會改**。
3. Session/2FA:Bitget 官方沒有公布網頁 session 有效期;安全指南寫 App 自動鎖定可設 1 分鐘–48 小時、可用「受信任裝置管理」查看/移除登入裝置。新 IP(如 GitHub Actions)登入通常觸發裝置驗證,所以 cookie 只適合**放在自己的 Mac 上跑**,不要進雲端。https://www.bitget.com/support/articles/12560603819255 、https://www.bitget.com/academy/bitget-advanced-account-security-guide
4. 條款面:前次研究(bybit-copytrade-api-findings)已提醒自動化登入態抓取有服務條款風險;這裡同樣適用,只讀輪詢風險較低但非零。

**判定**:可行但最脆弱;只在 App/Email 完全沒通知時當備案。信心:中。

## Q4. 手機端把 App 推播轉出去

### iPhone
1. **iOS Shortcuts 沒有「收到通知」觸發器**。個人自動化觸發器只有:事件(時間/鬧鐘/睡眠/手錶運動/聲音辨識)、旅程、通訊(Email/Message)、交易、設定。https://support.apple.com/guide/shortcuts/event-triggers-apd932ff833f/ios 、https://support.apple.com/guide/shortcuts/communication-triggers-apdd711f9dff/ios
2. 通訊觸發器裡的 **Email 觸發**可用「寄件人 / 主旨包含 / 帳戶」條件,收到 Bitget 信就跑捷徑(可播聲音、發 HTTP 到 ntfy)。前提:該信箱要在 iPhone「郵件」App 且能即時推送(Gmail 在 Apple Mail 是抓取不是推送,建議在 Gmail 設過濾器轉寄到 iCloud 信箱)。https://support.apple.com/guide/shortcuts/communication-triggers-apdd711f9dff/ios
3. **macOS iPhone Mirroring**:Mac 會自動收到 iPhone 的通知,「不管你有沒有在用 iPhone Mirroring 或 iPhone 在不在旁邊,只要 iPhone 開機」。需求:macOS Sequoia 15+ 且 Apple Silicon/T2、iOS 18+、同一 Apple 帳號 2FA、藍牙+Wi-Fi、iPhone 上鎖;**歐盟不可用**。https://support.apple.com/en-us/120421
4. **Mac 通知中心資料庫**:Sequoia 起在 `~/Library/Group Containers/group.com.apple.usernoted/db2/db`(SQLite,`record` 表,`data` 欄是 plist blob;`app_id`、`delivered_date`)。讀取需要 Full Disk Access;建議先 cp 再 sqlite3 查;schema 非公開,版本可能變。https://9to5mac.com/2024/09/01/security-bite-apple-addresses-privacy-concerns-around-notification-center-database-in-macos-sequoia/ 、https://forum.latenightsw.com/t/parsing-notifications-in-macos-sequoia/5001 、https://forge-work.com/dfir/knowledge/artifacts/macos-notification-center
   → 組合:iPhone Bitget 推播 → 鏡像到 Mac → launchd 每 3–5 秒查 db 新增列(過濾 app_id 含 bitget)→ curl 到 ntfy/Telegram。

### Android
5. MacroDroid「Notification」觸發器:收到通知即觸發,可依 App 與內文(萬用字元/regex)過濾,需授予 Notification Access;Android 15+ 對通知內容存取有限制。動作可接「HTTP Request」到 ntfy。https://wiki.macrodroid.com/wiki/index.php/Trigger:_Notification
6. Tasker 需另裝 AutoNotification 外掛才有等效功能(未逐一驗證,MacroDroid 已足夠)。

## Q5. Email 路徑:Gmail 過濾器 → ntfy / Pushover

1. Gmail:「預設自動轉寄會轉所有信;要轉特定信件,關閉自動轉寄改用過濾器 → Forward it」;新增轉寄地址後 Google 會寄驗證連結/碼到該地址(ntfy 主題會直接顯示那封驗證信,照著點/輸入即可)。https://support.google.com/mail/answer/10957
2. ntfy Email 發布:寄到 `ntfy-<topic>@ntfy.sh` 即發布到 topic;ntfy.sh 前綴為 `ntfy-`;目前只支援主旨→標題;匿名可寫的公開 topic 就能用(topic 名等於密碼,取難猜的)。https://docs.ntfy.sh/publish/#e-mail-publishing
3. ntfy 價格:免費可不註冊使用;付費 Supporter $6/月(2,500 則/日)、Pro $12/月、Business $25/月;「daily emails」是**寄出**額度,與收信發布無關。https://ntfy.sh/
4. Pushover:每帳號有專屬 `@pomail.net` 位址,可另建別名並各設音效/優先級;App 每平台一次性 $4.99,30 天試用。https://support.pushover.net/i29-e-mailing-notifications-to-your-devices 、https://pushover.net/pricing
5. 前提同 Q1:**Bitget 是否真的對 CFD 跟單成交寄 Email 未文件化**,要實測。

## Q6. CFD 精英交易員「顯示部位」設定與延遲

1. 官方「Gold/Indices copy trading is now live」(2026-04-14):跟單者可看精英交易員「open positions(shown in real time or with a one-hour delay)」;績效「Data is updated hourly」;交易員可隨時調整分潤、最低跟單額、**position protection**。https://www.bitget.com/support/articles/12560603882183
2. 「Private Mode / open position protection」:交易員可對一般使用者隱藏訂單資訊;文件沒說是否也對自己的跟單者隱藏,也沒寫延遲時間。https://www.bitget.com/support/articles/12560603799856
3. 你的實測(匿名端點 ~60 分鐘)符合「一小時延遲」模式 → 星火沒開即時顯示,或即時只給登入態的跟單者頁。這是 Q3 值得試的原因。

---

## 排名:可行路徑

| # | 路徑 | 預期延遲 | 建置 | 脆弱度 | 你要做的 |
|---|------|---------|------|--------|---------|
| 0 | **先實測**:下次開倉時記錄 iPhone 是否有 Bitget 推播、Gmail 是否有信、內容長怎樣 | — | 0 | — | 開啟 App 內所有通知開關;截圖給我文字內容(不含帳號) |
| 1 | Bitget Email → Gmail 過濾器 → `ntfy-<topic>@ntfy.sh` | 10–60 秒(看 Bitget 寄信速度) | 15 分鐘 | 低(全部託管) | 自己在 Gmail 設過濾器與驗證;裝 ntfy App 訂閱 topic |
| 2 | Android 備機 + MacroDroid 通知觸發 → HTTP 到 ntfy | 1–3 秒 | 30 分鐘 | 低-中(Android 15 限制) | 需一支 Android 登入 Bitget App |
| 3 | iPhone Mirroring → Mac 通知 DB 輪詢(launchd)→ ntfy/Telegram | 3–10 秒 | 1–2 小時 | 中(Mac 要醒著、FDA、schema 會變、非歐盟) | Mac 給終端 Full Disk Access;`caffeinate` 或不睡眠 |
| 4 | iOS 捷徑 Email 觸發(Gmail→轉寄 iCloud→郵件 App) | 30–120 秒 | 20 分鐘 | 中(推送與否取決於信箱) | 純 iPhone,不需 Mac |
| 5 | 登入態 Copy Details 內部端點,本機 5–10 秒輪詢 | 5–15 秒 | 2–4 小時 + 維護 | 高(未文件化、session 過期、裝置驗證、ToS) | 自己開 DevTools 找端點;cookie 只存本機,不貼給我、不進 GitHub |
| × | 官方 API / WebSocket、公開 mt5 端點 | 無 / 60 分 | — | — | 不適用 |

執行順序:0 → 若有 Email 就 1(最穩);若只有推播沒信就 2 或 3;都沒有才 5。

## 10 行摘要

1. Bitget 沒有任何文件寫 CFD 跟單成交會推播或寄信;官方 API/WS 不含 CFD/MT5,此路確定不通。
2. 第一步必做:下次開倉實測 iPhone 推播與 Gmail 有無 Bitget 信。
3. 有 Email → Gmail 過濾器轉寄 `ntfy-<topic>@ntfy.sh`(免費、匿名 topic 即可),秒級到手機。
4. Pushover 是替代:`@pomail.net`,一次性 $4.99。
5. 只有 App 推播 → Android+MacroDroid 通知觸發最省事;iPhone 沒有通知觸發器。
6. iPhone-only 替代:macOS iPhone Mirroring 會把 iPhone 通知同步到 Mac(iOS 18/Sequoia,非歐盟),Mac 上 launchd 讀 `group.com.apple.usernoted/db2/db` 轉發。
7. iOS 捷徑「Email 觸發」可當純手機方案,但依賴信箱推送速度。
8. 登入態 Copy Details 內部端點沒有公開文件,只能自己 DevTools 找,session/2FA/ToS 風險高,cookie 一律留本機。
9. 官方文件寫精英交易員部位「即時或延遲一小時」顯示,你看到的 60 分鐘延遲符合;即時流可能只在登入態頁。
10. 秘密(cookie、token)不要貼給助理;需要雲端的只有 ntfy topic 名,其他都跑在你的 Mac/手機。
