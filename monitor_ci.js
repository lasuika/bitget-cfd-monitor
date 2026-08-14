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

function cadenceSec(hasOpen, now = new Date()) {
  const dow = now.getUTCDay(), h = now.getUTCHours();
  // Gold CFD closes for the weekend: Friday ~21:00 UTC to Sunday ~22:00 UTC.
  if (dow === 6) return 1800;                        // Saturday: market shut
  if (dow === 0 && h < 21) return 1800;              // Sunday before reopen
  if (hasOpen) return CFG.pollOpenSec;               // he is in a trade
  if (DEAD_HOURS.has(h)) return CFG.pollDeadSec;
  if (ACTIVE_HOURS.has(h)) return CFG.pollActiveSec;
  return CFG.pollIdleSec;
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
        Title: Buffer.from(title, 'utf8').toString('base64'),
        'X-Title-Encoding': 'base64',
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

const TRADER_URL = `https://www.bitget.com/copy-trading/cfd-trader/${CFG.portfolioId}`;

// Emergencies go out on both channels — if one is down you still get told.
async function notify(title, body, priority = 'default', tags = '', critical = false) {
  const full = critical ? `${body}\n\n→ 只有你能處理:開 Bitget App 停止跟單或平倉。` : body;
  // An emergency goes out on every configured channel at once. They fail in
  // different ways — one server down, one permission not granted — and this is
  // the one alert that must not be the one that got lost.
  const [n1, n2] = await Promise.all([
    ntfy(title, full, critical ? 'max' : priority, tags),
    critical ? pushover(title, full, { critical, url: TRADER_URL }) : Promise.resolve(null),
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
async function check(state) {
  const now = Date.now();
  const alerts = [];
  const id = CFG.portfolioId;

  const perf = await cfd.performance(id);
  await sleep(700);
  const open = (await cfd.openPositions(id)).map(norm);

  // Second, independent detection path. openPositions() is the endpoint every
  // alert here reads from, and it has never once been observed returning rows —
  // three of his trades went unnoticed while the monitor was demonstrably alive
  // and polling. history is the endpoint that IS proven: 700 trades came out of
  // it. Closed trades show up here whether or not the open-position view works,
  // so this both catches what the other path misses and tells us when that path
  // is blind.
  let closed = [];
  state.histPass = (state.histPass || 0) + 1;
  if (state.histPass % 2 === 1) {
    await sleep(700);
    closed = (await cfd.history(id, { maxPages: 1, pageSize: 20 }).catch(() => [])).map(norm);
  }
  const gold = await goldNow().catch(() => null);

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
    }
    // His equity move, scaled by how many of his lots you mirror, net of the
    // 20% share taken from your gains.
    const stepAtAnchor = Math.max(1, Math.floor(state.anchorMyEq / state.anchorHisEq));
    const hisDelta = eq - state.anchorHisEq;
    const myDelta = hisDelta > 0 ? hisDelta * stepAtAnchor * 0.8 : hisDelta * stepAtAnchor;
    state.myEqEstimate = MY_EQUITY + myDelta;

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
    const myEqNow = state.myEqEstimate || MY_EQUITY;
    const ratio = myEqNow / eq;
    const step = Math.floor(ratio);          // integer multiple currently held
    const myLot = step / 100;                // your size for his 0.01 lots
    const toNextFloor = ratio - step;        // headroom before size drops
    const topUpTo = (r) => eq * r - myEqNow;

    state.daysLeft = null;
    if (ratio < 1.0) {
      alerts.push({ key: 'cliff-below', p: 'urgent', tags: 'rotating_light',
        t: '🔴 跌破跟單門檻 — 你已經跟不到單',
        b: `你 $${n(myEqNow)} ÷ 他 $${n(eq)} = ${n(ratio, 4)}x\n\n` +
           `他 95% 的單是 0.01 手,乘上你的比例後無條件捨去成 0。\n` +
           `現在完全沒有在跟單。\n\n` +
           `補 $${n(topUpTo(1.05))} → 回到 1.05x(效率 95%)` });
    } else {
      // A static margin is the wrong trigger: 12% headroom is seven weeks of
      // warning at his current pace, which just teaches you to ignore the alert.
      // Warn on TIME instead, using his observed rate.
      //
      // The gap closes for a mechanical reason: you mirror his lots, so you earn
      // what he earns, but the 20% profit share is taken from your side only.
      // At step lots the gap therefore shrinks by (1 - 0.8 x step/step) of his
      // gain — 20% of it — every day he makes money.
      let daysLeft = null;
      const oldest = state.eqHist[0];
      const spanDays = oldest ? (now - oldest.t) / 864e5 : 0;
      if (spanDays >= CFG.rateMinDays && eq > oldest.eq) {
        const hisDaily = (eq - oldest.eq) / spanDays;
        const closingPerDay = 0.2 * hisDaily * step;
        if (closingPerDay > 0) daysLeft = (myEqNow - eq * step) / closingPerDay;
      }

      if (daysLeft != null && daysLeft < CFG.cliffWarnDays) {
        alerts.push({ key: `cliff-warn-${step}`, p: 'high', tags: 'warning',
          t: `🟡 約 ${n(daysLeft, 0)} 天後掉一階`,
          b: `目前 ${n(ratio, 3)}x,每筆跟 ${n(myLot)} 手。\n` +
             `跌破 ${step}.00x 會掉到 ${n((step - 1) / 100)} 手` +
             (step === 1 ? '(= 完全跟不到單)' : `(砍 ${n((1 / step) * 100, 0)}%)`) + `\n\n` +
             `依他近 ${n(spanDays, 0)} 天的實際速度推算。\n` +
             `補 $${n(topUpTo(step + 0.15))} → 回到 ${step}.15x,可再撐一段。` });
      }
      state.daysLeft = daysLeft;
    }

    // Idle capital is silent — surface it rather than letting it sit unnoticed.
    const eff = step / ratio;
    if (step >= 1 && eff < 0.8) {
      alerts.push({ key: `ineff-${step}`, p: 'low', tags: 'money_with_wings',
        t: `💤 資金效率只有 ${n(eff * 100, 0)}%`,
        b: `${n(ratio, 3)}x 和 ${step}.0x 的手數一樣(都是 ${n(myLot)} 手)。\n` +
           `目前約 $${n(myEqNow - eq * step)} 沒有在工作。\n\n` +
           `想提高倉位要補到 ${step + 1}.0x = 再加 $${n(topUpTo(step + 1))}。` });
    }
  }

  // 1b. closed trades seen only in history — the fallback path.
  const known = state.knownClosed || {};
  const POLL_S = CFG.pollActiveSec || 120;
  if (closed.length) {
    const fresh = closed.filter((t) => t.closeTime && !known[t.id]);
    for (const t of fresh) known[t.id] = t.closeTime;
    for (const k of Object.keys(known)) {
      if (now - known[k] > 3 * 864e5) delete known[k];
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

      // A trade that ran longer than two poll intervals should have been visible
      // as an open position. If it never was, the open-position view is blind and
      // every unrealised-risk alert built on it is silently dead.
      if (blind.length) {
        alerts.push({ key: `blind-${blind[0].id}`, p: 'high', tags: 'see_no_evil',
          t: '⚠️ 開倉偵測可能失效',
          b: `${blind.length} 筆持倉超過 ${n(POLL_S * 2 / 60, 1)} 分鐘的單,` +
             `從頭到尾沒被偵測為「持倉中」:\n` +
             blind.slice(0, 3).map((t) =>
               `  ${new Date(t.openTime).toISOString().slice(5, 16)} ${n((t.closeTime - t.openTime) / 60000, 1)} 分`
             ).join('\n') +
             `\n\n代表加碼梯、部位過久、浮虧這幾個警報目前看不到東西。` });
      }
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

  state.lastEquity = eq;
  return { alerts, eq, open, gold, quietH, daysLeft: state.daysLeft,
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
    let r = null;
    try {
      r = await check(state);
      failStreak = 0;
    } catch (e) {
      failStreak++;
      log({ pass, error: e.message, failStreak });
      // Repeated failures usually mean rate limiting; stand well back.
      if (failStreak >= 3) {
        await notify('⚠️ CFD 監控連續失敗',
          `連續 ${failStreak} 次無法取得資料:\n${e.message}`, 'high', 'warning');
        await sleep(300000);
        failStreak = 0;
      }
    }

    if (r) {
      await heartbeat();
      log({ pass, eq: r.eq, myEq: r.myEq ? +r.myEq.toFixed(2) : null, open: r.open.length, gold: r.gold,
        ratio: r.ratio ? +r.ratio.toFixed(4) : null, quietH: +r.quietH.toFixed(1),
        daysLeft: r.daysLeft != null ? +r.daysLeft.toFixed(1) : null,
        alerts: r.alerts.map((a) => a.key) });

      const sent = state.sent || {};
      for (const a of r.alerts) {
        if (sent[a.key] && Date.now() - sent[a.key] < COOLDOWN) continue;
        await notify(a.t, a.b, a.p, a.tags, !!a.crit);
        sent[a.key] = Date.now();
      }
      for (const k of Object.keys(sent)) if (Date.now() - sent[k] > 7 * 864e5) delete sent[k];
      state.sent = sent;
      saveState(state);
    }

    const wait = cadenceSec(r ? r.open.length > 0 : false) * 1000;
    if (Date.now() + wait > deadline) break;
    await sleep(wait);
  } while (Date.now() < deadline);

  saveState(state);
  log({ done: true, passes: pass });
})();
