// Cloud monitor for a Bitget CFD (gold) copy-trading leader.
// Runs on GitHub Actions; nothing runs on the user's machine.
//
// Why there is an internal loop: GitHub's scheduled triggers are heavily
// throttled in practice — on the sibling Bybit monitor a `*/5` cron fired every
// 67 minutes on average, with gaps up to 159. So each run holds its slot and
// polls itself for LOOP_MINUTES, and the cron only needs to restart it.
//
// Cadence is adaptive rather than fixed, driven by this trader's measured
// behaviour over 972 reconstructed trades:
//   * Saturday: he has never traded. Gold CFD is closed. Poll barely at all.
//   * 21:00 UTC: zero entries across the whole sample.
//   * 03:00-05:00 UTC: under 1% of entries each.
//   * His 16 active hours carry 87% of entries.
//   * Median hold is ~15 minutes, so once he is IN something, poll tight.
// This keeps us far away from the 429s this endpoint returns under load, and
// spends the request budget where his trades actually are.
const fs = require('fs');
const path = require('path');
const https = require('https');
const { cfd, norm, OZ_PER_LOT, sleep } = require('./cfd.js');

const DIR = __dirname;
const CFG = JSON.parse(fs.readFileSync(path.join(DIR, 'config.json'), 'utf8'));
const STATE_FILE = path.join(DIR, 'state.json');

const NTFY_TOPIC = process.env.NTFY_TOPIC || CFG.ntfyTopic;
// Pushover is the emergency channel. It holds Apple's Critical Alerts
// entitlement, so it overrides silent and Do Not Disturb, and priority=2
// re-sends every `retry` seconds until you actually acknowledge it — which is
// the part that matters when the alert lands at 3am.
const PO_TOKEN = process.env.PUSHOVER_TOKEN || '';
const PO_USER = process.env.PUSHOVER_USER || '';
// Dead man's switch. Every alert path in here assumes this process is running;
// if it stops — GitHub's scheduler skips, the workflow errors, Actions gets
// disabled after 60 days of repo inactivity — the result is silence, and
// silence is indistinguishable from a quiet market. So we ping an outside
// service on a heartbeat and let IT shout when the pings stop. It has to be
// external: anything hosted here dies in the same failure.
const HEALTHCHECK_URL = process.env.HEALTHCHECK_URL || '';
const LOOP_MINUTES = +(process.env.LOOP_MINUTES ?? 50);
const TEST_ALERT = !!process.env.TEST_ALERT;
// Your CFD equity, as of MY_EQUITY_AT. Kept in a workflow variable so it can be
// updated from the GitHub UI after a deposit without touching the repo.
//
// This is a snapshot, not a live reading: CFD/MT5 sits outside every API your
// key can reach, so nothing here can see your balance. It goes stale the moment
// he trades. We project it forward from the anchor — while your ratio is between
// 1x and 2x you hold the same 0.01 lots he does, so your PnL tracks his roughly
// 1:1 before the 20% profit share — and nag you to re-sync once the projection
// has drifted far enough that the cliff maths would be materially wrong.
const MY_EQUITY = +(process.env.MY_EQUITY || CFG.myEquity || 0);
const MY_EQUITY_AT = process.env.MY_EQUITY_AT || CFG.myEquityAt || null;

// One MT5 copy account is created per trader you follow, funded and sized
// separately, so each needs its own equity figure and its own cliff maths.
// Older single-trader configs still work.
const TRADERS = CFG.traders && CFG.traders.length ? CFG.traders : [{
  name: CFG.name, portfolioId: CFG.portfolioId,
  myEquity: MY_EQUITY, myEquityAt: MY_EQUITY_AT,
}];
// MY_EQUITY_<N> overrides the Nth trader's equity from a workflow variable.
for (let i = 0; i < TRADERS.length; i++) {
  // An explicit 0 means "not copying this one" and must override the config —
  // treating it as unset would keep reporting cliffs for an account you closed.
  const raw = process.env[`MY_EQUITY_${i + 1}`];
  if (raw != null && raw !== '' && Number.isFinite(+raw)) TRADERS[i].myEquity = +raw;
  const a = process.env[`MY_EQUITY_AT_${i + 1}`];
  if (a) TRADERS[i].myEquityAt = a;
}

const n = (v, d = 2) => (v == null || !Number.isFinite(+v) ? '—' : (+v).toFixed(d));

// Actions logs are public on a public repo, and unlimited Actions minutes are
// the only way to keep this running around the clock. So the run log must not
// carry your balance. Alert bodies still hold the real numbers — those go to
// ntfy and Pushover, which are private.
const VERBOSE = process.env.VERBOSE === '1';
const REDACT = new Set(['myEq', 'ratio', 'daysLeft']);
function log(o) {
  const out = { t: new Date().toISOString() };
  for (const [k, v] of Object.entries(o)) {
    out[k] = !VERBOSE && REDACT.has(k) && v != null ? '***' : v;
  }
  console.log(JSON.stringify(out));
}

const loadState = () => { try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch { return {}; } };
const saveState = (s) => fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 1));

// --- adaptive cadence ---------------------------------------------------
const ACTIVE_HOURS = new Set(CFG.activeHoursUtc);
const DEAD_HOURS = new Set(CFG.deadHoursUtc);

// Traders keep different hours, and pooling them wastes the request budget on
// times nobody trades. TraderEthan has never opened between 15:00 and 23:00 UTC;
// ManuGoldPrime does most of his work there. Poll fast only when the trader in
// question is actually likely to be at the desk.
function cadenceSec(hasOpen, traders = TRADERS, now = new Date()) {
  const dow = now.getUTCDay(), h = now.getUTCHours();
  // Gold CFD closes for the weekend: Friday ~21:00 UTC to Sunday ~22:00 UTC.
  if (dow === 6) return 1800;                        // Saturday: market shut
  if (dow === 0 && h < 21) return 1800;              // Sunday before reopen
  if (hasOpen) return CFG.pollOpenSec;               // someone is in a trade
  const active = traders.some((t) => {
    const hrs = t.activeHoursUtc || CFG.activeHoursUtc;
    return hrs && hrs.includes(h);
  });
  if (active) return CFG.pollActiveSec;
  const dead = traders.every((t) => {
    const hrs = t.deadHoursUtc || CFG.deadHoursUtc;
    return hrs && hrs.includes(h);
  });
  return dead ? CFG.pollDeadSec : CFG.pollIdleSec;
}

// --- notifications ------------------------------------------------------
// Two channels on purpose. ntfy carries everything and is the running log.
// Pushover carries emergencies only, because it is the one that can actually
// wake you — ntfy cannot override silent mode on iOS. Spending a Do Not Disturb
// override on routine alerts would train you to mute the app, so it is reserved
// for the cases where you must act now.
function ntfy(title, body, priority = 'default', tags = '') {
  if (!NTFY_TOPIC) { log({ ntfySkipped: title }); return Promise.resolve(false); }
  return new Promise((resolve) => {
    const data = Buffer.from(body, 'utf8');
    const req = https.request({
      hostname: 'ntfy.sh', path: `/${NTFY_TOPIC}`, method: 'POST',
      headers: {
        'Content-Length': data.length,
        // HTTP headers are latin-1; ntfy's documented way to carry UTF-8 in the
        // Title header is an RFC 2047 encoded-word. The previous attempt used a
        // made-up X-Title-Encoding header, which the server ignored — so every
        // phone notification arrived titled with raw base64.
        Title: '=?UTF-8?B?' + Buffer.from(title, 'utf8').toString('base64') + '?=',
        Priority: priority,
        ...(tags ? { Tags: tags } : {}),
      },
    }, (res) => { res.resume(); res.on('end', () => resolve(res.statusCode < 300)); });
    req.on('error', (e) => { log({ ntfyError: e.message }); resolve(false); });
    req.setTimeout(10000, () => { req.destroy(); resolve(false); });
    req.write(data); req.end();
  });
}

// priority 2 = emergency: repeats every `retry` seconds until acknowledged, or
// until `expire` elapses. Anything less can be slept through.
function pushover(title, body, { critical = false, url = null } = {}) {
  if (!PO_TOKEN || !PO_USER) return Promise.resolve(false);
  const form = new URLSearchParams({
    token: PO_TOKEN, user: PO_USER, title, message: body,
    priority: critical ? '2' : '0',
    ...(critical ? {
      retry: String(CFG.pushoverRetrySec ?? 60),
      expire: String(CFG.pushoverExpireSec ?? 1800),
      sound: 'persistent',
    } : {}),
    ...(url ? { url, url_title: '開啟他的交易頁' } : {}),
  }).toString();
  const data = Buffer.from(form, 'utf8');
  return new Promise((resolve) => {
    const req = https.request({
      hostname: 'api.pushover.net', path: '/1/messages.json', method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': data.length },
    }, (res) => {
      let b = ''; res.on('data', (c) => (b += c));
      res.on('end', () => {
        const ok = res.statusCode < 300;
        if (!ok) log({ pushoverError: `HTTP ${res.statusCode}: ${b.slice(0, 120)}` });
        resolve(ok);
      });
    });
    req.on('error', (e) => { log({ pushoverError: e.message }); resolve(false); });
    req.setTimeout(10000, () => { req.destroy(); resolve(false); });
    req.write(data); req.end();
  });
}

// Fire-and-forget, rate-limited, and never allowed to disturb the monitor.
let lastBeat = 0;
// A '/fail' suffix deliberately marks the check down, which is the only way to
// exercise the down-notification path end to end without waiting an hour for a
// real outage. The URL itself never leaves the workflow.
function heartbeat(force = false, suffix = '') {
  if (!HEALTHCHECK_URL) return Promise.resolve(false);
  const gap = (CFG.heartbeatMin ?? 10) * 60000;
  if (!suffix && !force && Date.now() - lastBeat < gap) return Promise.resolve(null);
  if (!suffix) lastBeat = Date.now();
  const url = HEALTHCHECK_URL.replace(/\/$/, '') + suffix;
  return new Promise((resolve) => {
    const req = https.request(url, { method: 'GET', timeout: 8000 }, (res) => {
      res.resume(); res.on('end', () => resolve(res.statusCode < 300));
    });
    req.on('error', (e) => { log({ heartbeatError: e.message }); resolve(false); });
    req.on('timeout', () => { req.destroy(); resolve(false); });
    req.end();
  });
}

const traderUrl = (id) => `https://www.bitget.com/copy-trading/cfd-trader/${id}`;

// Emergencies go out on both channels — if one is down you still get told.
async function notify(title, body, priority = 'default', tags = '', critical = false, url = null) {
  const full = critical ? `${body}\n\n→ 只有你能處理:開 Bitget App 停止跟單或平倉。` : body;
  // An emergency goes out on every configured channel at once. They fail in
  // different ways — one server down, one permission not granted — and this is
  // the one alert that must not be the one that got lost.
  const [n1, n2] = await Promise.all([
    ntfy(title, full, critical ? 'max' : priority, tags),
    critical ? pushover(title, full, { critical, url }) : Promise.resolve(null),
  ]);
  log({ sent: VERBOSE ? title : '(內容僅送推播)', ntfy: n1, pushover: n2, critical });
  if (critical && !n1 && !n2) log({ CRITICAL_DELIVERY_FAILED: true });
}

function pub(p) {
  return new Promise((res, rej) => {
    const r = https.request({ hostname: 'api.bitget.com', path: p, method: 'GET',
      headers: { 'Content-Type': 'application/json', locale: 'en-US' } }, (x) => {
      let b = ''; x.on('data', (c) => (b += c));
      x.on('end', () => { try { res(JSON.parse(b)); } catch { rej(new Error('bad json')); } });
    });
    r.on('error', rej);
    r.setTimeout(15000, () => r.destroy(new Error('timeout')));
    r.end();
  });
}
const goldNow = () => pub('/api/v2/mix/market/ticker?symbol=XAUUSDT&productType=USDT-FUTURES')
  .then((r) => { const t = (r.data || [])[0]; return t ? +t.lastPr : null; });

// --- one pass -----------------------------------------------------------
async function check(rootState, trader) {
  const now = Date.now();
  const alerts = [];
  const id = trader.portfolioId;
  // Per-trader state, so one trader's dedup and anchors cannot bleed into another.
  rootState.byTrader = rootState.byTrader || {};
  const state = (rootState.byTrader[id] = rootState.byTrader[id] || {});
  const MY_EQUITY = +trader.myEquity || 0;
  const MY_EQUITY_AT = trader.myEquityAt || null;
  const TAG = trader.name ? `[${trader.name}] ` : '';

  const perf = await cfd.performance(id);
  await sleep(700);
  const open = (await cfd.openPositions(id)).map(norm);

  // History is the PRIMARY detection path, polled every pass. This is now
  // proven, not precautionary: leg-1 logs caught five polls landing inside a
  // 26.8-minute position on 2026-08-14 and every one returned zero rows — the
  // anonymous currentPosition view hides live positions (standard copy-trade
  // anti-freeriding), and even lastOrderTime lags until after the close. Closed
  // trades, by contrast, appear in history within ~3 minutes of closing.
  await sleep(700);
  const closed = (await cfd.history(id, { maxPages: 1, pageSize: 20 }).catch(() => [])).map(norm);
  const gold = await goldNow().catch(() => null);

  // Canary: if the open view ever starts returning rows — a Bitget change, or
  // an authenticated future — the unrealised-risk alerts come back to life,
  // and that changes the protection posture enough to be worth a notification.
  if (open.length && !state.openViewAlive) {
    state.openViewAlive = true;
    alerts.push({ key: 'open-view-alive', p: 'high', tags: 'eyes',
      t: '👁 開倉偵測復活了',
      b: `currentPosition 開始回傳持倉了(${open.length} 筆)。\n` +
         `加碼梯/部位過久/浮虧警報從現在起真的有作用。` });
  }

  const eq = +perf.totalEquity;
  const lastOrder = +perf.lastOrderTime;

  // 1. equity cliffs. 95% of his trades are 0.01 lots — the MT5 minimum — and
  //    copier lots round DOWN, so your size for one of his 0.01-lot entries is
  //    floor(ratio)/100. That makes EVERY integer ratio a cliff, not just 1.0:
  //    slipping from 2.00x to 1.99x halves your size, and below 1.00x you stop
  //    copying his 0.01-lot trades entirely. It also means capital between two
  //    integers buys no extra size at all — at 1.5x you hold exactly what you
  //    would at 1.0x, with a third of the money idle.
  //
  //    The ratio decays on its own: the 20% profit share comes out of your side,
  //    so your equity compounds slower than his even when copying perfectly.
  // Anchor his equity the first time we see a given MY_EQUITY reading, so the
  // projection below measures from the same instant your snapshot was true.
  if (MY_EQUITY > 0 && eq > 0) {
    if (state.anchorKey !== `${MY_EQUITY}@${MY_EQUITY_AT}`) {
      state.anchorKey = `${MY_EQUITY}@${MY_EQUITY_AT}`;
      state.anchorHisEq = eq;
      state.anchorMyEq = MY_EQUITY;
      state.feeSinceAnchor = 0;
    }
    // His equity move, scaled by how many of his lots you mirror, net of the
    // 20% share taken from your gains.
    const stepAtAnchor = Math.max(1, Math.floor(state.anchorMyEq / state.anchorHisEq));
    const hisDelta = eq - state.anchorHisEq;
    const myDelta = hisDelta > 0 ? hisDelta * stepAtAnchor * 0.8 : hisDelta * stepAtAnchor;
    state.myEqEstimate = MY_EQUITY + myDelta - (state.feeSinceAnchor || 0);

    const drift = Math.abs(myDelta) / MY_EQUITY;
    if (drift > CFG.resyncDriftPct) {
      alerts.push({ key: `resync-${Math.round(state.myEqEstimate)}`, p: 'default', tags: 'arrows_counterclockwise',
        t: '🔄 該回報一次真實權益了',
        b: `你上次回報 $${n(MY_EQUITY)}(${MY_EQUITY_AT || '未記錄'})。\n` +
           `依他的權益變化推估你現在約 $${n(state.myEqEstimate)}(${myDelta >= 0 ? '+' : ''}${n(myDelta)})。\n\n` +
           `推估已偏離 ${n(drift * 100, 0)}% — 跟單比例的計算會開始失準。\n` +
           `請到 App 看實際權益,更新 GitHub 的 MY_EQUITY 變數。` });
    }
  }

  if (MY_EQUITY > 0 && eq > 0) {
    // Sample his equity hourly, not every poll: at a 45-second cadence a
    // per-poll push would put ~40,000 entries in state.json inside the 21-day
    // window, for no extra signal. Sampled here rather than inside a branch so
    // it keeps accruing even below 1.0x, which is when the rate matters most.
    const hist = (state.eqHist || []).filter((h) => now - h.t < 21 * 864e5);
    if (!hist.length || now - hist[hist.length - 1].t > 36e5) hist.push({ t: now, eq });
    state.eqHist = hist;

    // Use the projection for the cliff maths — a stale snapshot would keep
    // reporting a ratio you no longer have.
    //
    // Denominated in HIS base lot, not a hardcoded 0.01: your copy per order is
    // floor(baseLot x ratio x 100)/100, optionally pinned by your Max-lot cap.
    // With a 0.02 base (TraderEthan) the size steps at every HALF integer of
    // the ratio, and the first live trade confirmed the arithmetic: his 0.02 at
    // ratio 5.00 arrived as exactly 0.10.
    const myEqNow = state.myEqEstimate || MY_EQUITY;
    const ratio = myEqNow / eq;
    const baseLot = +trader.baseLot || 0.01;
    const lotCap = +trader.maxLotPerTrade || Infinity;
    const rawSteps = Math.floor(baseLot * ratio * 100);   // your lots in 0.01 units
    const pinned = rawSteps / 100 > lotCap;
    const myLot = Math.min(rawSteps / 100, lotCap);
    state.curMyLot = myLot;
    const floorRatio = rawSteps / (100 * baseLot);        // ratio where size drops a step
    const topUpTo = (r) => eq * r - myEqNow;

    state.daysLeft = null;
    if (rawSteps < 1) {
      alerts.push({ key: 'cliff-below', p: 'urgent', tags: 'rotating_light',
        t: '🔴 跌破跟單門檻 — 你已經跟不到單',
        b: `你 $${n(myEqNow)} ÷ 他 $${n(eq)} = ${n(ratio, 4)}x\n\n` +
           `他的單筆 ${n(baseLot)} 手乘上你的比例後無條件捨去成 0。\n` +
           `現在完全沒有在跟單。\n\n` +
           `補 $${n(topUpTo(1.05 / (100 * baseLot)))} → 恢復最小跟單` });
    } else if (pinned) {
      // Pinned at your own Max-lot cap: ratio drift cannot change your size, so
      // there is no cliff to count down to, and the capital above the cap is
      // the deliberate safety buffer — not idleness worth nagging about.
    } else {
      let daysLeft = null;
      const oldest = state.eqHist[0];
      const spanDays = oldest ? (now - oldest.t) / 864e5 : 0;
      if (spanDays >= CFG.rateMinDays && eq > oldest.eq) {
        const hisDaily = (eq - oldest.eq) / spanDays;
        // Your equity mirrors his at (myLot/baseLot) x 0.8 after the share, so
        // the gap to the next size step closes at 0.2 x floorRatio of his daily
        // gain — the algebra collapses to this for any base lot.
        const closingPerDay = 0.2 * floorRatio * hisDaily;
        if (closingPerDay > 0) daysLeft = (myEqNow - eq * floorRatio) / closingPerDay;
      }
      if (daysLeft != null && daysLeft >= 0 && daysLeft < CFG.cliffWarnDays) {
        alerts.push({ key: `cliff-warn-${rawSteps}`, p: 'high', tags: 'warning',
          t: `🟡 約 ${n(daysLeft, 0)} 天後掉一階`,
          b: `目前 ${n(ratio, 3)}x,每筆跟 ${n(myLot)} 手。\n` +
             `跌到 ${n(floorRatio, 2)}x 以下會變 ${n((rawSteps - 1) / 100)} 手` +
             (rawSteps === 1 ? '(= 完全跟不到單)' : `(減 ${n(100 / rawSteps, 0)}%)`) + `\n\n` +
             `依他近 ${n(spanDays, 0)} 天的實際速度推算。\n` +
             `補 $${n(topUpTo((rawSteps + 0.3) / (100 * baseLot)))} → 多一步緩衝。` });
      }
      state.daysLeft = daysLeft;
    }
  }

  // 1b. closed trades seen only in history — the fallback path.
  const known = state.knownClosed || {};
  const POLL_S = CFG.pollActiveSec || 120;
  if (closed.length) {
    const fresh = closed.filter((t) => t.closeTime && !known[t.id]);
    for (const t of fresh) known[t.id] = t.closeTime;
    // Prune only entries that have ALSO fallen off page 1. A fixed age alone
    // re-alerts old trades: at his ~19 trades per 3 days, a 3-day-old close can
    // still sit at rank 20 of the page — and one did, resurfacing as "fresh"
    // within 3 minutes of aging out of this map.
    const onPage = new Set(closed.map((t) => t.id));
    for (const k of Object.keys(known)) {
      if (now - known[k] > 3 * 864e5 && !onPage.has(k)) delete known[k];
    }
    state.knownClosed = known;

    // Only meaningful once we have a baseline; the first pass would otherwise
    // report his entire recent history as new.
    if (state.histSeeded) {
      const notable = fresh.filter((t) => (t.closeTime - t.openTime) / 1000 > POLL_S * 2);
      const everSeenOpen = (t) => !!(state.seen || {})[t.id];
      const blind = notable.filter((t) => !everSeenOpen(t));

      const realised = fresh.reduce((sum, t) => sum + t.profit, 0);
      if (fresh.length) {
        // He works in bursts. A close means he is at the desk — poll tightly
        // for the next while so the rest of the burst is caught near-realtime.
        state.burstUntil = now + (CFG.burstMinutes ?? 30) * 60000;
        const myEqRef = state.myEqEstimate || MY_EQUITY || 0;
        const bad = myEqRef > 0 && realised < -myEqRef * CFG.emergencyFloatPct;
        alerts.push({ key: `closed-${fresh.map((t) => t.id).join(',')}`,
          p: bad ? 'urgent' : 'default', crit: bad, tags: bad ? 'rotating_light' : 'receipt',
          t: `${bad ? '🚨' : '📄'} 他平倉 ${fresh.length} 筆 ${realised >= 0 ? '+' : ''}$${n(realised)}`,
          b: fresh.slice(0, 5).map((t) =>
            `${t.side === 'long' ? '多' : '空'} ${n(t.lots)} 手 ${n(t.openPrice)}→${n(t.closePrice)} ` +
            `${t.profit >= 0 ? '+' : ''}$${n(t.profit)} (${n((t.closeTime - t.openTime) / 60000, 1)} 分)`
          ).join('\n') + (fresh.length > 5 ? `\n…另 ${fresh.length - 5} 筆` : '') });
      }

      // Regime detectors. A 95%-win trader whose lifetime worst loss is $0.80
      // is untested by definition; what breaks first is the PATTERN, and these
      // watch for exactly that.
      const chrono = [...fresh].sort((a, b) => a.closeTime - b.closeTime);
      const bl3 = +trader.baseLot || 0.01;

      // (a) his per-order size changed — your copy scales with it 1:1
      for (const t of chrono) {
        if (Math.abs(t.lots - bl3) > 1e-9) {
          alerts.push({ key: `sizechange-${Math.round(t.lots * 100)}`, p: 'high', tags: 'triangular_ruler',
            t: `📐 他的單筆手數變了:${n(t.lots)}(原 ${n(bl3)})`,
            b: `你的跟單會等比放大 ${n(t.lots / bl3, 1)} 倍(超出部分由 Max lot 上限擋住)。\n` +
               `懸崖計算以 ${n(bl3)} 手為基礎 — 若他長期改用新手數,需要更新 baseLot。` });
        }
      }

      // (b) first real loss — his historical worst is -$0.80, so anything
      //     meaningfully deeper is the pattern breaking, not noise.
      const lossFloor = +trader.lossAlertUsd || 10;
      for (const t of chrono) {
        if (t.profit <= -lossFloor) {
          alerts.push({ key: `realloss-${t.id}`, p: 'high', tags: 'small_red_triangle_down',
            t: `🔻 出現真實虧損 -$${n(Math.abs(t.profit))}`,
            b: `${t.side === 'long' ? '多' : '空'} ${n(t.lots)} 手 ${n(t.openPrice)}→${n(t.closePrice)}\n` +
               `你的等比虧損約 -$${n(Math.abs(t.profit) * ((state.curMyLot || 0) / bl3))}。\n` +
               `這是模式改變的第一個訊號 — 留意接下來幾筆。` });
        }
      }

      // (c) rolling win rate over the last 20 closes
      const recent = state.recentResults || [];
      for (const t of chrono) recent.push(t.profit > 0 ? 1 : 0);
      state.recentResults = recent.slice(-20);
      if (state.recentResults.length >= 20) {
        const wr = state.recentResults.reduce((a, b) => a + b, 0) / state.recentResults.length;
        if (wr < 0.6 && !state.wrLow) {
          state.wrLow = true;
          alerts.push({ key: 'winrate-drift', p: 'high', tags: 'chart_with_downwards_trend',
            t: `📉 近 20 筆勝率掉到 ${n(wr * 100, 0)}%`,
            b: `他的歷史勝率 95%。連續劣化代表策略碰到了沒見過的行情。\n考慮減碼或暫停跟單。` });
        } else if (wr >= 0.75) state.wrLow = false;
      }

      // (d) fees drag on the equity projection: $6 per lot round-trip, measured
      //     from the first live fill ($0.60 at 0.10 lots).
      state.feeSinceAnchor = (state.feeSinceAnchor || 0) + fresh.length * (state.curMyLot || 0) * 6;

      // Blindness of the open view is established fact (see above), so this is
      // bookkeeping rather than news — track it, do not alarm on every trade.
      if (blind.length) state.blindCount = (state.blindCount || 0) + blind.length;
    }
    state.histSeeded = true;
  }

  // 2. new entries — report how stale our reference price already is, rather
  //    than pretending we saw the fill.
  const seen = state.seen || {};
  const fresh = open.filter((p) => !seen[p.id]);
  for (const p of fresh) seen[p.id] = { firstSeen: now, openPrice: p.openPrice, gold };
  for (const k of Object.keys(seen)) {
    if (!open.find((p) => p.id === k) && !seen[k].closedAt) seen[k].closedAt = now;
    if (seen[k].closedAt && now - seen[k].closedAt > 7 * 864e5) delete seen[k];
  }
  state.seen = seen;

  if (gold != null && open.length) {
    const avg = open.reduce((s, p) => s + p.openPrice, 0) / open.length;
    state.basis = state.basis == null ? avg - gold : state.basis * 0.8 + (avg - gold) * 0.2;
  }

  if (fresh.length) {
    const lines = fresh.map((p) => {
      const lagS = Math.round((now - p.openTime) / 1000);
      const ref = gold != null && state.basis != null ? gold + state.basis : null;
      const drift = ref == null ? null
        : (p.side === 'long' ? ref - p.openPrice : p.openPrice - ref);
      return `${p.side === 'long' ? '多' : '空'} ${n(p.lots)} 手 @ ${n(p.openPrice)}` +
        `  (延遲 ${lagS}s` + (drift != null ? `, 已走 ${drift >= 0 ? '+' : ''}$${n(drift)}` : '') + ')';
    });
    alerts.push({ key: `open-${fresh.map((p) => p.id).join(',')}`, p: 'default', tags: 'zap',
      t: `⚡ 開單 ${fresh.length} 筆`,
      b: lines.join('\n') + `\n\n他平均獲利只有 $4.73/盎司 — 滑價 $1 就吃掉 20%。` });
  }

  // 2b. Stacked exposure. The "Max lot size per copy trade" cap stops HIS
  //     withdrawals from inflating any single order, but it does nothing about
  //     him running several at once — and concurrency is where the leverage
  //     actually comes from. TraderEthan has held five at a time, which at this
  //     sizing is 108x and a liquidation only $40 of gold away.
  if (MY_EQUITY > 0 && gold != null && open.length) {
    const cap = +trader.maxLotPerTrade || Infinity;
    const ratio0 = MY_EQUITY / eq;
    const myLots = open.reduce((sum, p) =>
      sum + Math.min(Math.floor(p.lots * ratio0 * 100) / 100, cap), 0);
    const notional = myLots * OZ_PER_LOT * gold;
    const lev = notional / MY_EQUITY;
    const levCrit = trader.levCrit ?? 80;
    const levWarn = trader.levWarn ?? 55;
    if (lev >= levWarn) {
      const liqMove = MY_EQUITY / (myLots * OZ_PER_LOT);
      alerts.push({ key: `expo-${Math.round(lev / 10)}`, p: 'urgent', crit: lev >= levCrit,
        tags: 'warning',
        t: `${lev >= levCrit ? '🚨' : '⚠️'} 曝險 ${n(lev, 0)}x`,
        b: `他同時開 ${open.length} 單 → 你 ${n(myLots)} 手 = $${n(notional, 0)} 名目\n` +
           `你的權益 $${n(MY_EQUITY)}\n\n` +
           `金價再逆走 $${n(liqMove, 0)} 就會清算。\n` +
           `每單停損 $${trader.stopPerOrder || '?'} 會在 $${n((trader.stopPerOrder || 0) / (Math.min(Math.floor(0.02 * ratio0 * 100) / 100, cap) * OZ_PER_LOT), 0)} 先觸發。` });
    }
  }

  // 3. averaging-down ladder — where his losses concentrate.
  for (const side of ['long', 'short']) {
    const leg = open.filter((p) => p.side === side);
    if (leg.length < CFG.ladderMinLegs) continue;
    const px = leg.map((p) => p.openPrice).sort((a, b) => a - b);
    const spread = px[px.length - 1] - px[0];
    if (spread < CFG.ladderMinSpread) continue;
    const lots = leg.reduce((s, p) => s + p.lots, 0);
    const first = leg.slice().sort((a, b) => a.openTime - b.openTime)[0].openPrice;
    const adverse = side === 'long' ? first - px[0] : px[px.length - 1] - first;
    const ref = gold != null && state.basis != null ? gold + state.basis : null;
    const float = ref == null ? null : leg.reduce((s, p) =>
      s + (side === 'long' ? ref - p.openPrice : p.openPrice - ref) * p.lots * OZ_PER_LOT, 0);
    const myEqRef = state.myEqEstimate || MY_EQUITY || 0;
    const bad = float != null && myEqRef > 0 && float < -myEqRef * CFG.emergencyFloatPct;
    alerts.push({ key: `ladder-${side}-${leg.length}`, p: 'urgent', crit: bad,
      tags: 'chart_with_downwards_trend',
      t: `${bad ? '🚨' : '🔻'} 加碼攤平中(${side === 'long' ? '多' : '空'} ${leg.length} 單)`,
      b: `入場 ${n(px[0])} ~ ${n(px[px.length - 1])} (跨距 $${n(spread)})\n` +
         `總量 ${n(lots)} 手 = ${n(lots * OZ_PER_LOT, 0)} 盎司\n` +
         `逆勢 $${n(adverse)}` + (float != null ? ` | 估計浮動 ${float >= 0 ? '+' : ''}$${n(float)}` : '') +
         `\n\n他的加碼梯歷史上平均虧 $110,最深一次抱了 41 小時。` });
  }

  // 4. stale position — median hold is ~15 min; hours old means it went wrong.
  const oldest = open.reduce((a, p) => (a == null || p.openTime < a.openTime ? p : a), null);
  if (oldest) {
    const ageH = (now - oldest.openTime) / 3.6e6;
    // Median hold is ~15 minutes. Hours means it went wrong; many hours with no
    // stop attached means nobody is minding it, and nothing will close it but him.
    const abandoned = ageH >= CFG.emergencyStaleHours && !oldest.sl;
    if (ageH >= CFG.staleHours) {
      alerts.push({ key: `stale-${oldest.id}`, p: 'high', crit: abandoned, tags: 'hourglass',
        t: `${abandoned ? '🚨 部位無人看管' : '⏳ 部位已持有'} ${n(ageH, 1)} 小時`,
        b: `${oldest.side === 'long' ? '多' : '空'} ${n(oldest.lots)} 手 @ ${n(oldest.openPrice)}\n` +
           `中位持倉只有 15 分鐘 — 這筆走反了。\n停損 ${oldest.sl ? n(oldest.sl) : '未設'}` +
           (abandoned ? `\n\n他已 ${n(ageH, 1)} 小時沒動作,且這筆沒掛停損。` : '') });
    }
  }

  // 5. back from the announced break.
  const quietH = (now - lastOrder) / 3.6e6;
  if (state.wasQuiet && fresh.length) {
    alerts.push({ key: 'resumed', p: 'high', tags: 'bell',
      t: '🔔 他恢復交易了',
      b: `沉寂 ${n(state.quietPeakH || quietH, 1)} 小時後重新開單。` +
         (MY_EQUITY > 0 ? `\n你的比例 ${n((state.myEqEstimate || MY_EQUITY) / eq, 3)}x` : '') });
    state.quietPeakH = 0;
  }
  state.wasQuiet = quietH >= CFG.quietHours;
  if (state.wasQuiet) state.quietPeakH = Math.max(state.quietPeakH || 0, quietH);

  // 6. His equity is the DENOMINATOR of your lot size, and he controls it.
  //    Both traders sweep profits out constantly — ManuGoldPrime has moved
  //    $178,883 out since April against $1,684 in, on an account that holds
  //    under $2,000. Every sweep shrinks the denominator, which silently
  //    multiplies your position on his next trade. He is also paid 20% of what
  //    his copiers make, so bigger copier positions are directly in his
  //    interest. This is the one input to your risk that you neither set nor
  //    are told about.
  if (state.lastEquity && eq > 0) {
    const bl2 = +trader.baseLot || 0.01;
    const cp2 = +trader.maxLotPerTrade || Infinity;
    const lotAt = (hisEq) => Math.min(Math.floor(bl2 * ((MY_EQUITY || 0) / hisEq) * 100) / 100, cp2);
    const before = lotAt(state.lastEquity);
    const after = lotAt(eq);
    const moved = Math.abs(eq - state.lastEquity) / state.lastEquity;
    if (MY_EQUITY > 0 && after !== before) {
      alerts.push({ key: `lotstep-${Math.round(after * 100)}`, p: 'urgent', crit: after > before,
        tags: 'chart_with_upwards_trend',
        t: `${after > before ? '🚨 你的部位變大了' : '🔻 你的部位變小了'}`,
        b: `他的權益 $${n(state.lastEquity)} → $${n(eq)}\n` +
           `你的每筆手數 ${n(before)} → ${n(after)}` +
           (after > before ? `(放大 ${n(after / Math.max(before, 0.01), 1)} 倍)` : '') + `\n\n` +
           (after > before
             ? '你什麼都沒做,曝險就增加了 — 他的權益縮小,分母變小。\n(超出 Max lot 上限的部分已被擋住。)'
             : '他的權益變大了,你的部位被稀釋。') });
    } else if (moved > 0.25) {
      alerts.push({ key: `eq-move-${Math.round(eq / 100)}`, p: 'default', tags: 'information_source',
        t: '他的權益大幅變動',
        b: `$${n(state.lastEquity)} → $${n(eq)} (${eq > state.lastEquity ? '+' : ''}${n((eq / state.lastEquity - 1) * 100, 1)}%)\n` +
           `尚未跨過手數階梯,但接近了。` });
    }
  }
  state.lastEquity = eq;
  // Sample maturity: 44 trades was a snapshot, not a record. Nudge a re-run of
  // the full analysis as the sample grows instead of trusting day-7 statistics.
  const tot = +perf.totalTrades || 0;
  if (tot && MY_EQUITY > 0) {
    for (const m of [100, 150, 200, 300]) {
      if (tot >= m && (state.milestone || 0) < m) {
        state.milestone = m;
        alerts.push({ key: `milestone-${m}`, p: 'default', tags: 'dart',
          t: `🎯 他的樣本到 ${m} 筆了`,
          b: `統計基礎比你進場時(44 筆)厚了 ${n(m / 44, 1)} 倍。\n值得重跑一次完整分析,再決定加碼、維持或退出。` });
        break;
      }
    }
  }

  const burst = (state.burstUntil || 0) > now;
  return { alerts, eq, open, gold, quietH, burst, name: trader.name, id, daysLeft: state.daysLeft,
    ratio: MY_EQUITY > 0 ? (state.myEqEstimate || MY_EQUITY) / eq : null,
    myEq: state.myEqEstimate || MY_EQUITY };
}

// --- loop ---------------------------------------------------------------
(async () => {
  const state = loadState();

  // Drive the dead man's switch through a full down/up cycle so both
  // notifications actually land, rather than trusting that they would.
  if (process.env.TEST_HEALTHCHECK === '1') {
    if (!HEALTHCHECK_URL) { log({ healthcheckTest: 'HEALTHCHECK_URL 未設定' }); return; }
    const down = await heartbeat(true, '/fail');
    log({ step: '1/2 送出 DOWN(應觸發「監控死亡」通知)', ok: down });
    await sleep(45000);
    const up = await heartbeat(true);
    log({ step: '2/2 送出 UP(應觸發「已恢復」通知)', ok: up });
    log({ healthcheckTestDone: true, down, up });
    return;
  }

  if (TEST_ALERT) {
    const crit = process.env.TEST_CRITICAL === '1';
    await notify(crit ? '🚨 [測試] 緊急警報' : '[雲端] CFD 監控測試',
      crit
        ? `這是緊急通道測試。\n真實情況下代表加碼梯浮虧過大或部位無人看管。\n${new Date().toISOString()}`
        : `ManuGoldPrime 監控運作正常。\n${new Date().toISOString()}`,
      'default', 'white_check_mark', crit);
    const beat = await heartbeat(true);
    log({ testAlertSent: true, critical: crit,
      channels: { ntfy: !!NTFY_TOPIC, pushover: !!(PO_TOKEN && PO_USER),
        heartbeat: HEALTHCHECK_URL ? beat : false } });
    return;
  }

  const deadline = Date.now() + LOOP_MINUTES * 60000;
  const COOLDOWN = (CFG.cooldownMin || 60) * 60000;
  let pass = 0, failStreak = 0;

  do {
    pass++;
    const results = [];

    // Each trader is polled in turn. One failing — a rate limit, a trader who
    // stopped publishing — must not stop the others being watched.
    for (const trader of TRADERS) {
      try {
        results.push(await check(state, trader));
        failStreak = 0;
      } catch (e) {
        failStreak++;
        log({ pass, trader: trader.name, error: e.message, failStreak });
        if (failStreak >= 3) {
          await notify('⚠️ CFD 監控連續失敗',
            `連續 ${failStreak} 次無法取得資料:\n${e.message}`, 'high', 'warning');
          await sleep(300000);
          failStreak = 0;
        }
      }
      if (TRADERS.length > 1) await sleep(1500);
    }

    if (results.length) {
      await heartbeat();
      const sent = state.sent || {};
      for (const r of results) {
        log({ pass, trader: r.name, eq: r.eq, myEq: r.myEq ? +r.myEq.toFixed(2) : null,
          open: r.open.length, gold: r.gold,
          ratio: r.ratio ? +r.ratio.toFixed(4) : null, quietH: +r.quietH.toFixed(1),
          daysLeft: r.daysLeft != null ? +r.daysLeft.toFixed(1) : null,
          alerts: r.alerts.map((a) => a.key) });

        for (const a of r.alerts) {
          // Namespaced so the same condition on two traders alerts twice.
          const key = `${r.id}|${a.key}`;
          if (sent[key] && Date.now() - sent[key] < COOLDOWN) continue;
          const title = r.name ? `${a.t} · ${r.name}` : a.t;
          await notify(title, a.b, a.p, a.tags, !!a.crit, traderUrl(r.id));
          sent[key] = Date.now();
        }
      }
      for (const k of Object.keys(sent)) if (Date.now() - sent[k] > 7 * 864e5) delete sent[k];
      state.sent = sent;
      saveState(state);
    }

    // Tightest cadence any trader calls for. The open view is blind, so
    // "recently closed something" is the working signal that he is active.
    const anyOpen = results.some((r) => r.open.length > 0 || r.burst);
    const wait = cadenceSec(anyOpen, TRADERS) * 1000;
    if (Date.now() + wait > deadline) break;
    await sleep(wait);
  } while (Date.now() < deadline);

  saveState(state);
  log({ done: true, passes: pass });
})();
