# 自己帳戶(CFD 跟單)即時資料來源研究

日期:2026-08-18
範圍:Bitget CFD(MT5)跟單者帳戶的部位/權益/保證金,如何從外部(GitHub Actions 監控器)取得。只讀,不動任何專案程式碼。

結論先講:**Bitget 官方明文寫「跟單者不能直接登入 MT5 跟單帳戶」**,所以 MetaApi / MT5 終端機 / MetaTrader5 Python 這條路,對「跟單子帳戶」目前走不通。可行的是(a)自己的一般 CFD(MT5)主帳戶用投資者密碼 + MetaApi,(b)Email 通知 → Gmail 轉發/IMAP → 監控器。

---

## Q1. Bitget 是否提供 MT5 登入 ID / 伺服器 / 投資者密碼?跟單帳戶呢?

**答案**

1. 一般 CFD(MT5)帳戶:有。開通後彈窗顯示登入資料;之後可在「MT5 資產頁」查看帳號 ID 與初始密碼;**主密碼與投資者密碼都能在 MT5 資產頁重設**(需 2FA;8–16 字元,大小寫+數字+特殊符號)。投資者密碼 = 「唯讀存取帳戶資訊,不能交易」。
2. 第三方 MT5 終端機:官方文件寫「下載對應作業系統的 MetaTrader 5 → File → Login to Trading Account」,輸入 Bitget MT5 帳號 ID、密碼、以及「與你的 Bitget CFD(MT5)帳戶綁定的伺服器位址」。→ 允許用官方 MetaTrader 5 桌面/手機版登入。
3. 伺服器名稱:支援文章沒寫出字串。Academy 網頁版教學寫系統通常自動選,或名為 **`Bitget-CFD-Live`**;另一份 Academy 文章寫「TradFi 右上選單 → Gold/Indices info」可查伺服器資訊。搜尋摘要另出現 `Bitget-TradFi-Live`,但沒在一手頁面確認。→ 實際名稱以 App「MT5 資產頁 / Gold-Indices info」顯示為準。
4. **CFD 跟單帳戶:不能登入。** 官方 CFD Copier Guide 原句:"Copiers cannot log in directly to the MT5 copy account." 系統自動建立 MT5 跟單帳戶並綁定你的 Bitget 帳號;跟單者只能在 App「Copy Details」頁看帳戶統計(預估淨利、未實現/已實現 PnL、equity、balance)、部位、分潤紀錄。
5. CFD 不支援子帳戶(只能用 Bitget 主帳號開 CFD 帳戶)。

**信心**:高(1、2、4、5);中(3 伺服器名稱)。

**步驟(如果你也有一般 CFD 主帳戶想接出來)**
1. App:資產 → CFD / MT5 資產頁 → 查看 MT5 帳號 ID、伺服器。
2. 同頁「重設投資者密碼」→ 2FA → 產生投資者密碼(**只給唯讀**)。
3. 用 MetaTrader 5 官方 App(File → Login to Trading Account,伺服器輸入頁面顯示的名稱)確認投資者密碼可登入、看得到權益。
4. 把 login / server / investor password 直接放進 GitHub Secrets(不要貼給我)。

**來源**
- https://www.bitget.com/support/articles/12560603839750 (登入/重設密碼、主密碼 vs 投資者密碼、第三方 MT5 終端機步驟)
- https://www.bitget.com/support/articles/12560603873726 (CFD Copier Guide:跟單者不能登入 MT5 跟單帳戶;Copy Details 頁;App 路徑)
- https://www.bitget.com/support/articles/12560603847329 (CFD 不支援子帳戶)
- https://www.bitget.com/support/articles/12560603839640 (啟用後彈窗顯示登入資料)
- https://www.bitget.com/academy/bitget-tradfi-web-tutorial (`Bitget-CFD-Live`)
- https://www.bitget.com/academy/how-to-open-mt5-account (Gold/Indices info 查伺服器資訊)
- https://www.bitget.com/support/articles/12560603839625 (MT5 服務說明;無 CFD API)

---

## Q2. MetaApi(metaapi.cloud)

**答案**

1. 投資者密碼夠用:建帳號 API 的 password 欄位原句 "The password can be either investor password for read-only access or master password to enable trading features."SDK 文件也寫 "password can be investor password for read-only access"。唯讀足以拿 positions / account-information / 串流。
2. REST 端點(header `auth-token`,base 依 region,例 `https://mt-client-api-v1.new-york.agiliumtrade.ai`):
   - `GET /users/current/accounts/{accountId}/positions` → id, symbol, type, volume, openPrice, currentPrice, stopLoss, swap, profit, unrealizedProfit …
   - `GET /users/current/accounts/{accountId}/account-information` → balance, equity, margin, freeMargin, marginLevel, leverage, login, server, tradeAllowed …
   - 可選 query `refreshTerminalState=true`。
3. 新增帳號:`POST /users/current/accounts`,欄位 name / login / password / server / platform(mt5)/ magic。伺服器名會自動偵測 broker 設定;偵測失敗要建 **provisioning profile** 並上傳 MT5 終端機資料夾 `config/servers.dat`。MetaApi 說 "can be used with any broker",Bitget 沒有出現在任何一手文件中(可能自動偵測不到,要走 servers.dat)。
4. 費用:官方只寫 "MetaApi is a paid service, however we may offer a free tier access in some cases"、SDK 描述 "Free usage tier available";舊版 README 與第三方摘要說「1 個 MT 帳號免費」。帳號只在 deployed 狀態計費,最少 6 小時。**確切每帳月費沒有一手頁面可引用**(pricing 頁 404),第三方列 ~US$10/月起。
5. 速率:每帳號 5,000 CPU credits / 10 秒;每使用者 1,000/秒、6,000/分、18,000/小時 ×帳號數。輪詢一分鐘一次遠低於上限。
6. 替代:MetaTrader5 Python 套件需 Windows + 執行中的 MT5 終端機(官方文件下載連結指向 Windows Python);`login(login, password=, server=)` 可帶投資者密碼(唯讀)。MT5 web terminal(Bitget 網頁 CFD 頁)只能人工看,無 API。

**信心**:高(1、2、3 端點與欄位);中(4 價格、Bitget 是否在自動偵測清單)。

**步驟(前提:是可登入的 MT5 帳戶,不是跟單子帳戶)**
1. 註冊 metaapi.cloud → 取得 API token(放 GitHub Secrets)。
2. Web app 或 `POST /users/current/accounts`:login、investor password、server(先試自動偵測;失敗就從 MT5 桌面版 `config/servers.dat` 建 provisioning profile)。
3. Deploy 帳號 → 等 state=DEPLOYED、connectionStatus=CONNECTED。
4. 監控器每次跑:`GET .../account-information` + `GET .../positions`。
5. 不用時 undeploy(不計費)。

**來源**
- https://metaapi.cloud/docs/provisioning/api/account/createAccount/
- https://metaapi.cloud/docs/client/restApi/api/readTradingTerminalState/readPositions/
- https://metaapi.cloud/docs/client/restApi/api/readTradingTerminalState/readAccountInformation/
- https://metaapi.cloud/docs/provisioning/api/provisioningProfile/createNewProvisioningProfile/
- https://github.com/metaapi/metaapi-python-sdk/blob/main/docs/metaApi/managingAccounts.rst (servers.dat 位置、investor password)
- https://metaapi.cloud/docs/client/faq/ (投資者密碼、計費 6 小時、deployed 才計費)
- https://metaapi.cloud/docs/client/rateLimiting/
- https://github.com/metaapi/metaapi-python-sdk (free tier 措辭)
- https://www.mql5.com/en/docs/python_metatrader5 、https://www.mql5.com/en/docs/python_metatrader5/mt5login_py

---

## Q3. Bitget 對 CFD 跟單者的通知

**答案**

1. 一手文件能證實的只有:CFD 跟單上線公告寫「投資組合建立完成會收到 email 通知」;跟單者 PnL / 分潤紀錄在「Elite Trader Center > Copiers」或「Assets > Gold/Indices Copy Trading」查看,**資料每小時更新**;分潤每天 00:00(UTC+8)自動結算。
2. 沒有任何 Bitget 支援文章明列 CFD 跟單者的「開倉 / 平倉 / 止損觸發 / 分潤結算」的 Email/推播/SMS 事件清單。
3. 全站通知政策:2025-06-26 起交易相關通知(爆倉警告、預爆倉警示等)優先走 Email;沒綁 Email 才走 SMS。
4. 通知設定路徑:官方文章沒寫 App 路徑(一般是「個人 → 設定 → 通知」);Bitget 沒有官方 Telegram 帳戶事件機器人的文件(搜尋到的只有第三方 GitHub 專案)。
5. CFD 停損/停利不會被複製,跟單者需自己在 App 設。

**信心**:高(1、3、5);低(2、4——是「找不到」,不是「確定沒有」)。

**步驟(驗證你實際收到什麼)**
1. App → 個人 → 設定 → 通知,把 Email + 推播全部打開;確認帳號綁了 Gmail。
2. 記下下一次跟單開/平倉的時間,對照 Gmail 是否有 Bitget 信、主旨格式為何。
3. 若有信 → 走 Q4 的 Email 管線;若沒有 → 通知路徑不可用,退回 App 手動查看或 MetaApi(限可登入帳戶)。

**來源**
- https://www.bitget.com/support/articles/12560603882183
- https://www.bitget.com/support/articles/12560603873726
- https://www.bitget.com/support/articles/12560603830399

---

## Q4. Email → 監控器管線

**答案**

1. Gmail App Password:必須先開 2-Step Verification;在 https://myaccount.google.com/apppasswords 建立;以下情況不可用:2SV 只設安全金鑰、公司/學校帳號、Advanced Protection。
2. Gmail IMAP:2025-01 起 IMAP 永遠開啟(沒有開關);主機 `imap.gmail.com`,埠 `993`,SSL;帳號用完整 Gmail 地址,密碼填 App Password;IMAP session 約 24 小時上限。
3. Gmail 自動轉發:Settings → Forwarding → 加轉發地址 → 對方信箱點驗證連結;要只轉特定郵件:關掉全域轉發,建立 Filter → 動作選 "Forward it to";可多個 filter 對應不同地址。
4. ntfy.sh Email 發布:寄到 `ntfy-$topic@ntfy.sh`(免費公開 topic 可用);若 topic 有存取控制,用 `ntfy-$topic+$token@ntfy.sh` 或 SMTP AUTH PLAIN;目前只支援標題(主旨),不支援 tags/priority。
5. Pushover Email Gateway:每個帳號自帶一個 `@pomail.net` 地址(可加 alias、各自音效/優先級);Pushover 本身 US$4.99 每平台一次性買斷、30 天試用、每月 10,000 則免費。Gmail 轉發到 pomail 需先通過 Gmail 的驗證信(Pushover 論壇有相關議題)。

**信心**:高。

**步驟(推薦 A:Gmail Filter 直接轉發到 ntfy/Pushover,不需任何密碼進 GitHub)**
1. Gmail 網頁 → Settings → Forwarding and POP/IMAP → Add a forwarding address → 填 `ntfy-<你的topic>@ntfy.sh`(或你的 `xxx@pomail.net`)。
2. 到 ntfy 該 topic / Pushover 收 Google 驗證信,點連結。
3. Gmail → Filters → Create filter:from `bitget.com` + 主旨關鍵字(開倉/平倉/Copy)→ Forward it to 上述地址。
4. 手機裝 ntfy / Pushover App 訂閱該 topic。

**步驟(推薦 B:GitHub Actions 用 IMAP 讀信解析)**
1. Google 帳戶開 2SV → myaccount.google.com/apppasswords 建 App Password。
2. App Password 只貼進 GitHub Secrets(`GMAIL_USER`, `GMAIL_APP_PASSWORD`)。
3. Action 內用 imaplib 連 `imap.gmail.com:993` SSL,搜尋 `FROM bitget UNSEEN`,解析主旨/內文後推 ntfy/Telegram。
4. 建議建 Gmail filter 把 Bitget 信自動貼標籤,監控器只讀該 label。

**來源**
- https://support.google.com/accounts/answer/185833
- https://support.google.com/mail/answer/7126229
- https://developers.google.com/workspace/gmail/imap/imap-smtp
- https://support.google.com/mail/answer/10957
- https://github.com/binwiederhier/ntfy/blob/main/docs/publish.md (E-mail publishing 段)
- https://support.pushover.net/i29-e-mailing-notifications-to-your-devices
- https://pushover.net/pricing
- https://support.pushover.net/i194-approve-gmail-forwarding-email-address

---

## Q5. Bitget CFD 內部端點 `publicDetail.aum` 語意

**答案**

1. 沒有任何公開文件描述 CFD 跟單內部端點或 `aum` 欄位。Bitget 官方 API 文件只涵蓋 Futures/Spot copy trading。
2. Bitget 一般用語:AUM = 跟隨該交易員的資金總額(跟單者資產)。是否含未實現 PnL:**未文件化**;只能用實測(對照 App「Copy Details」的 equity vs balance,或觀察持倉中 aum 是否隨價格跳動)推斷。

**信心**:低(未文件化)。

**來源**
- https://www.bitget.com/support/articles/12560603873726 (跟單者可見的統計欄位:equity、balance、未實現/已實現 PnL)
- https://www.bitget.com/api-doc/copytrading/future-copytrade/follower/Settings (官方 API 僅 Futures 跟單)

---

## 建議排序(取得自己帳戶即時資料,安全優先)

1. **Email 通知 → Gmail Filter 轉發到 ntfy/Pushover**(零密碼外流、零程式;先驗證 Bitget 對 CFD 跟單者到底寄不寄開/平倉信——這是唯一未知)。
2. **Email → Gmail IMAP(App Password 放 GitHub Secrets)→ 監控器解析**:同 1 的前提,但能進監控器邏輯(如與帶單者部位交叉比對)。App Password 只有信箱唯讀風險,可隨時撤銷。
3. **MetaApi + 投資者密碼**:資料最完整(equity/margin/positions 秒級),但**只適用你能登入的 CFD 主帳戶,不適用跟單子帳戶**(官方明文禁止跟單者登入)。若你另有一般 CFD 帳戶或未來 Bitget 開放跟單帳戶登入,這是首選;需確認 Bitget 伺服器能被 MetaApi 偵測(否則上傳 servers.dat)。
4. **MetaTrader5 Python / 本機 MT5 終端機**:同 3 的限制,且需 Windows 常駐,不適合 GitHub Actions。
5. **App 手動查看 Copy Details**:官方唯一保證的路徑,資料每小時更新,無法自動化。

未知待驗證:Bitget 是否對 CFD 跟單者寄開/平倉 Email(Q3 步驟 2)。這一項決定 1、2 是否成立。
