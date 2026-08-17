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
// Alarm tests pin the path explicitly instead of trusting the clock: a loud
// test run in the afternoon or a vibrate test run at midnight must exercise
// the path being tested, not whatever the hour happens to select.
const FORCE_WAKE = process.env.TEST_VIBRATE === '1' ? false
  : process.env.TEST_CRITICAL === '1' ? true : null;
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
  TRADERS[i].varName = `MY_EQUITY_${i + 1}`;
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
// Gold CFD session, derived from 972 timestamps across a peer's history rather
// than assumed: zero open/close events in UTC hour 21 on ANY weekday, first
// Sunday events at 22:02 UTC, last Friday close 20:28 UTC. So the session is
// Sun 22:00 UTC → Fri 21:00 UTC with a daily 21:00–22:00 UTC break. Bitget's
// own rule during the weekend closure: "You cannot stop copying" — whatever he
// holds at 21:00 UTC Friday, you hold until Sunday 22:00 UTC, stops included.
function marketState(now = new Date()) {
  const dow = now.getUTCDay(), h = now.getUTCHours(), m = now.getUTCMinutes();
  const day = now.toISOString().slice(0, 10);
  if ((CFG.marketHolidays || []).includes(day)) return { open: false, why: 'holiday', toWeekClose: null };
  if (dow === 6 || (dow === 0 && h < 22) || (dow === 5 && h >= 21)) return { open: false, why: 'weekend', toWeekClose: null };
  if (h === 21) return { open: false, why: 'break', toWeekClose: null };
  return { open: true, why: null, toWeekClose: dow === 5 ? 21 * 60 - (h * 60 + m) : null };
}

function cadenceSec(hasOpen, traders = TRADERS, now = new Date()) {
  const h = now.getUTCHours();
  const mk = marketState(now);
  if (!mk.open) return mk.why === 'break' ? CFG.pollDeadSec : 1800; // nothing can move
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
// Every critical is emergency priority — repeating until acknowledged — at
// all hours; what varies with the clock is only the NOISE. Sleeping hours omit
// the sound parameter so the client's high-priority loud alarm applies (waking
// the user is the point); waking hours override with Pushover's official
// 'vibrate' sound, so the repeat-until-ack machinery runs silently in the
// pocket. Missing an ack just means another buzz a minute later.
function sleepingNow() {
  const h = new Date().getUTCHours();
  const hrs = CFG.sleepHoursUtc || [];
  return hrs.includes(h);
}
function pushover(title, body, { critical = false, url = null, wake = null } = {}) {
  if (!PO_TOKEN || !PO_USER) return Promise.resolve(false);
  const loud = critical && (wake != null ? wake : sleepingNow());
  const form = new URLSearchParams({
    token: PO_TOKEN, user: PO_USER, title, message: body,
    priority: critical ? '2' : '0',
    ...(critical ? {
      // Pushover's hard floor for emergency retries is 30 seconds — nothing
      // faster is possible through the API. The day path runs at the floor;
      // the night path stays at 60s.
      retry: String(loud ? (CFG.pushoverRetrySec ?? 60) : (CFG.pushoverRetryVibrateSec ?? 30)),
      expire: String(CFG.pushoverExpireSec ?? 1800),
      // Emergency-priority messages ignore the app's default-sound setting —
      // the API must name the sound. Night omits it (the client's loud
      // high-priority alarm applies); day sends the configured tone.
      ...(loud ? {} : { sound: CFG.daySound || 'pushover' }),
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

// Self-supersede: deploy hand-over was the last manual fragility — cancel
// dances left zero-coverage windows, and queued stale-code runs stole the
// slot. A leg that notices main has moved past its own commit saves state and
// steps aside, letting the queued newer run take over within seconds.
//
// The condition is "a NEWER RUN of this workflow is waiting with a different
// commit", not merely "main moved". The first version keyed on main's SHA and
// a docs-only push with no dispatch made every leg yield to a run that did not
// exist — a 2-hour hole until the next cron. Test dispatches (monitor job
// skipped) are excluded by checking the candidate's jobs.
function ghJson(p, tok) {
  return new Promise((resolve) => {
    const req = https.request({
      hostname: 'api.github.com', path: p,
      headers: { 'User-Agent': 'cfd-monitor', Accept: 'application/vnd.github+json',
        ...(tok ? { Authorization: `Bearer ${tok}` } : {}) },
    }, (res) => {
      let b = ''; res.on('data', (c) => (b += c));
      res.on('end', () => { try { resolve(res.statusCode === 200 ? JSON.parse(b) : null); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(8000, () => { req.destroy(); resolve(null); });
    req.end();
  });
}
async function newerCommitExists() {
  const repo = process.env.GITHUB_REPOSITORY, sha = process.env.GITHUB_SHA;
  const runId = +process.env.GITHUB_RUN_ID, tok = process.env.GITHUB_TOKEN;
  if (!repo || !sha || !runId) return false;          // local runs
  const wf = (process.env.GITHUB_WORKFLOW_REF || '').split('@')[0].split('/').pop() || 'monitor.yml';
  const list = await ghJson(`/repos/${repo}/actions/workflows/${wf}/runs?per_page=6`, tok);
  const cands = ((list && list.workflow_runs) || []).filter((r) =>
    r.id > runId && r.head_sha !== sha && r.status !== 'completed');
  for (const r of cands) {
    const jobs = await ghJson(`/repos/${repo}/actions/runs/${r.id}/jobs?per_page=10`, tok);
    const monitorJob = ((jobs && jobs.jobs) || []).some((j) => /^monitor/.test(j.name) && j.conclusion !== 'skipped');
    if (monitorJob) return true;
  }
  return false;
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
    critical ? pushover(title, full, { critical, url, wake: FORCE_WAKE }) : Promise.resolve(null),
  ]);
  log({ sent: VERBOSE ? title : '(內容僅送推播)', ntfy: n1, pushover: n2, critical });
  if (critical && !n1 && !n2) log({ CRITICAL_DELIVERY_FAILED: true });
  return !!(n1 || n2);
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

function getJson(hostname, p) {
  return new Promise((res, rej) => {
    const r = https.request({ hostname, path: p, method: 'GET',
      headers: { 'User-Agent': 'cfd-monitor/1.0', Accept: 'application/json' } }, (x) => {
      let b = ''; x.on('data', (c) => (b += c));
      x.on('end', () => { try { res(JSON.parse(b)); } catch { rej(new Error('bad json ' + x.statusCode)); } });
    });
    r.on('error', rej);
    r.setTimeout(15000, () => r.destroy(new Error('timeout')));
    r.end();
  });
}

// Perp 1-minute candle containing timestamp ts (ms). His CFD fill minus this
// close is the CFD-vs-perp basis: a rough, free proxy for the spread we cannot
// see. Fill-minute noise is ±$1; a persistent |basis| of several dollars means
// the CFD quote is wide or the feed drifted — either way the $30/oz stop is
// closer than the maths assumed.
async function perpCloseAt(ts) {
  const r = await pub(`/api/v2/mix/market/candles?symbol=XAUUSDT&productType=USDT-FUTURES&granularity=1m&endTime=${ts + 60000}&limit=2`);
  const rows = (r.data || []).filter((c) => +c[0] <= ts).sort((a, b) => +b[0] - +a[0]);
  return rows.length ? +rows[0][4] : null;
}

// US high-impact events for the current week, from ForexFactory's public feed.
// Gold's worst minutes cluster around these; his 30-minute-hold style has never
// been tested through one with your money on. Cached an hour; a failed fetch
// retries in five minutes.
const ecoCache = { at: 0, events: [] };
async function ecoEvents() {
  if (Date.now() - ecoCache.at < 3600e3) return ecoCache.events;
  try {
    const j = await getJson('nfs.faireconomy.media', '/ff_calendar_thisweek.json');
    ecoCache.events = (Array.isArray(j) ? j : [])
      .filter((e) => e.country === 'USD' && e.impact === 'High')
      .map((e) => ({ t: Date.parse(e.date), title: e.title }))
      .filter((e) => Number.isFinite(e.t));
    ecoCache.at = Date.now();
  } catch (e) {
    log({ ecoError: e.message });
    // The feed rate-limits per IP; a 429 earns a 30-minute back-off, anything
    // else retries in five.
    ecoCache.at = Date.now() - (/429/.test(e.message) ? 1800e3 : 3300e3);
  }
  return ecoCache.events;
}
const taipei = (ms) => new Date(ms + 8 * 3600e3).toISOString().slice(11, 16);

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
  // Resolved once per pass; every lot/scale computation below MUST use these
  // rather than re-reading config — the four independently-drifting copies of
  // this arithmetic are what produced the scale-mixing bug class.
  const baseLot = +trader.baseLot || 0.01;
  const lotCap = +trader.maxLotPerTrade || Infinity;

  const perf = await cfd.performance(id);

  // The open view is blind for anonymous callers (proven: five polls inside a
  // 26.8-minute hold, all zero rows). Probing it every pass spent a third of
  // the rate-limited budget on a canary whose only job is noticing if Bitget
  // ever unhides it — hourly is plenty for that.
  let open = [];
  if (now - (state.lastCanaryAt || 0) > 3600e3) {
    state.lastCanaryAt = now;
    await sleep(700);
    open = (await cfd.openPositions(id).catch(() => [])).map(norm);
  }

  // History is the PRIMARY detection path, polled every pass. This is now
  // proven, not precautionary: leg-1 logs caught five polls landing inside a
  // 26.8-minute position on 2026-08-14 and every one returned zero rows — the
  // anonymous currentPosition view hides live positions (standard copy-trade
  // anti-freeriding), and even lastOrderTime lags until after the close. Closed
  // trades, by contrast, appear in history within ~3 minutes of closing.
  // History failures must be LOUD: this is the primary detection path, and a
  // silent [] here previously left the heartbeat green while every detector
  // starved — total signal loss dressed as a quiet market.
  await sleep(700);
  let closed = [], histOk = true;
  try {
    closed = (await cfd.history(id, { maxPages: 1, pageSize: 20 })).map(norm);
    state.histFail = 0;
  } catch (e) {
    histOk = false;
    state.histFail = (state.histFail || 0) + 1;
    log({ trader: trader.name, histError: e.message, histFail: state.histFail });
    if (state.histFail >= 5) {
      alerts.push({ key: 'hist-dead', p: 'high', cool: 360, tags: 'no_entry',
        t: '⚠️ 主偵測路徑失效',
        b: `連續 ${state.histFail} 次撈不到他的平倉歷史:\n${e.message}\n` +
           `平倉偵測與所有模式偵測器目前全瞎。` });
    }
  }
  // Gold only matters to consumers gated on open rows; while the view is blind
  // both fetches per pass were pure waste against the ticker host.
  const gold = open.length ? await goldNow().catch(() => null) : null;

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
  let pendingCapraise = null;

  // The open view is blind, but his EQUITY is not: it carries unrealized PnL.
  // After each close we record equity as the flat baseline; any drift from it
  // means positions are running. That inference is the only live signal we
  // have while he holds — it drives the tight cadence and a floating-loss
  // alert. Learned the hard way: he sat in two shorts for hours while the
  // monitor idled at 5-minute polls because "no new close" read as "quiet".
  if (state.flatEq == null && eq > 0) state.flatEq = +trader.flatEqSeed || eq;
  const flatEq = state.flatEq;
  const eqDrift = flatEq != null && eq > 0 ? eq - flatEq : null;
  const inferOpen = eqDrift != null && Math.abs(eqDrift) >= (CFG.openInferUsd ?? 2);
  state.inferOpen = inferOpen;

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
      state.feeSinceAnchor = 0;
      state.pnlSinceAnchor = 0;
      state.resyncNagged = false;
    }
    // Your equity is projected from HIS CLOSED TRADES, not from his equity
    // curve: his equity moves mostly by withdrawals — he sweeps profit out
    // daily — and the previous delta-based projection read every sweep as a
    // trading loss, halving the phantom equity and firing false cliff alarms.
    // Trade PnL accrues in section 1b at the lot multiple in force per close.
    state.myEqEstimate = MY_EQUITY + (state.pnlSinceAnchor || 0) - (state.feeSinceAnchor || 0);

    const drift = Math.abs(state.myEqEstimate - MY_EQUITY) / MY_EQUITY;
    if (drift > CFG.resyncDriftPct && !state.resyncNagged) {
      state.resyncNagged = true;
      // Static key: the old one embedded the rounded equity estimate, which
      // both minted a fresh dedup key every pass AND printed the user's
      // balance into the public Actions log via the alert-key log line.
      alerts.push({ key: 'resync', p: 'default', cool: 1440, tags: 'arrows_counterclockwise',
        t: '🔄 該回報一次真實權益了',
        b: `你上次回報 $${n(MY_EQUITY)}(${MY_EQUITY_AT || '未記錄'})。\n` +
           `依他的成交累計推估你現在約 $${n(state.myEqEstimate)}。\n\n` +
           `推估已偏離 ${n(drift * 100, 0)}% — 跟單比例的計算會開始失準。\n` +
           `請到 App 看實際權益,更新 GitHub 的 ${trader.varName || 'MY_EQUITY_2'} 變數。` });
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
    const rawSteps = Math.floor(baseLot * ratio * 100);   // your lots in 0.01 units
    const pinned = rawSteps / 100 > lotCap;
    const myLot = Math.min(rawSteps / 100, lotCap);
    state.curMyLot = myLot;
    const floorRatio = rawSteps / (100 * baseLot);        // ratio where size drops a step
    const topUpTo = (r) => eq * r - myEqNow;

    state.daysLeft = null;
    if (rawSteps < 1) {
      alerts.push({ key: 'cliff-below', p: 'urgent', cool: 720, tags: 'rotating_light',
        t: '🔴 跌破跟單門檻 — 你已經跟不到單',
        b: `你 $${n(myEqNow)} ÷ 他 $${n(eq)} = ${n(ratio, 4)}x\n\n` +
           `他的單筆 ${n(baseLot)} 手乘上你的比例後無條件捨去成 0。\n` +
           `現在完全沒有在跟單。\n\n` +
           `補 $${n(topUpTo(1.05 / (100 * baseLot)))} → 恢復最小跟單` });
    } else if (pinned) {
      // Pinned at the cap. Below one full step of headroom that is the
      // deliberate safety buffer; at a full step and beyond, profits have
      // outgrown the cap and stopped compounding — which is exactly when the
      // user asked to be told. Two health gates guard the advice: never
      // suggest raising leverage within three days of a real loss, nor while
      // the rolling win rate is in drift. Silence is the right answer there.
      // Emission is DEFERRED to after section 1b: the health gates read the
      // regime flags (wrLow, lastRealLossAt) that 1b writes ~100 lines below,
      // so pushing here would consult last pass's flags and could advise
      // raising leverage in the same batch as the loss that forbids it.
      const stepsOver = rawSteps - Math.round(lotCap * 100);
      if (stepsOver >= 1) pendingCapraise = { stepsOver, rawSteps, myEqNow };
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
        alerts.push({ key: `cliff-warn-${rawSteps}`, p: 'high', cool: 720, tags: 'warning',
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

  // 1b. closed trades from history — the PRIMARY path.
  const known = state.knownClosed || {};
  // One conversion, one home: HIS dollars x copyMult = YOUR dollars.
  const copyMult = (state.curMyLot || 0) / baseLot;
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
      const realised = fresh.reduce((sum, t) => sum + t.profit, 0);
      if (fresh.length) {
        // He works in bursts. A close means he is at the desk — poll tightly
        // for the next while so the rest of the burst is caught near-realtime.
        state.burstUntil = now + (CFG.burstMinutes ?? 30) * 60000;
        // He just closed something; equity is at (or near) realized again.
        // Re-baseline so subsequent drift reads as new open exposure.
        state.flatEq = eq;
        state.floatWarned = false;
        state.floatStep = 0;
        const myEqRef = state.myEqEstimate || MY_EQUITY || 0;

        // Daily tally for the evening reconciliation nudge (UTC day key).
        const dayKey = new Date(now).toISOString().slice(0, 10);
        if (state.dayKey !== dayKey) { state.dayKey = dayKey; state.dayCloses = 0; state.dayPnlHis = 0; }
        state.dayCloses += fresh.length;
        state.dayPnlHis += realised;

        // CFD-vs-perp basis at his fill minutes. Best effort, at most 3 per
        // pass; a failure here must never block the close alert.
        for (const t of fresh.slice(0, 3)) {
          try {
            await sleep(300);
            const pc = await perpCloseAt(t.closeTime);
            const po = await perpCloseAt(t.openTime);
            if (pc != null && po != null) {
              const b = (state.basis = state.basis || []);
              b.push({ t: t.closeTime, open: +(t.openPrice - po).toFixed(2), close: +(t.closePrice - pc).toFixed(2) });
              while (b.length > 60) b.shift();
              // The perp carries a persistent premium over the CFD (measured on
              // 20 of his fills: median -$4.4/oz, range -0.6..-8.7), so the raw
              // basis is not the signal — its DEVIATION from the rolling median is.
              const prior = b.slice(0, -1).slice(-20).map((x) => x.close).sort((p, q) => p - q);
              const med = prior.length >= 5 ? prior[prior.length >> 1] : null;
              const dev = med == null ? 0
                : Math.max(Math.abs(t.openPrice - po - med), Math.abs(t.closePrice - pc - med));
              if (dev >= (CFG.basisWarnUsd ?? 4)) {
                alerts.push({ key: 'basis-wide', p: 'high', cool: 180, tags: 'straight_ruler',
                  t: `📏 CFD 報價異常偏離 $${n(dev)}/oz`,
                  b: `他這筆 ${n(t.openPrice)}→${n(t.closePrice)},同分鐘永續 ${n(po)}→${n(pc)};\n` +
                     `平常 CFD 比永續低約 $${n(-med)},這筆偏離 $${n(dev)}。\n` +
                     `點差放大或報價漂移;你的 $30/oz 停損實際距離可能比算的近。` });
              }
            }
          } catch (e) { log({ trader: trader.name, basisError: e.message }); }
        }
        // `realised` is in HIS dollars; the 6% line is in YOURS. Convert first —
        // the raw comparison left this trigger 5x too loose at current sizing.
        const realisedUser = realised * copyMult;
        const bad = myEqRef > 0 && copyMult > 0 && realisedUser < -myEqRef * CFG.emergencyFloatPct;
        alerts.push({ key: `closed-${fresh.map((t) => t.id).join(',')}`,
          p: bad ? 'urgent' : 'default', crit: bad, tags: bad ? 'rotating_light' : 'receipt',
          t: `${bad ? '🚨' : '📄'} 他平倉 ${fresh.length} 筆 ${realised >= 0 ? '+' : ''}$${n(realised)}` +
             (copyMult > 0 ? `(你約 ${realisedUser >= 0 ? '+' : ''}$${n(realisedUser)})` : ''),
          b: fresh.slice(0, 5).map((t) =>
            `${t.side === 'long' ? '多' : '空'} ${n(t.lots)} 手 ${n(t.openPrice)}→${n(t.closePrice)} ` +
            `${t.profit >= 0 ? '+' : ''}$${n(t.profit)} (${n((t.closeTime - t.openTime) / 60000, 1)} 分)`
          ).join('\n') + (fresh.length > 5 ? `\n…另 ${fresh.length - 5} 筆` : '') });

        // Back-from-break bell, driven by what we can actually see (closes).
        // The old version keyed on open-view rows and could never fire.
        if (state.wasQuiet) {
          alerts.push({ key: 'resumed', p: 'high', tags: 'bell',
            t: '🔔 他恢復交易了',
            b: `沉寂 ${n(state.quietPeakH || 0, 1)} 小時後重新出手。` +
               (MY_EQUITY > 0 ? `\n你的比例 ${n((state.myEqEstimate || MY_EQUITY) / eq, 3)}x` : '') });
          state.quietPeakH = 0;
          state.wasQuiet = false;
        }
      }

      // Regime detectors. A 95%-win trader whose lifetime worst loss is $0.80
      // is untested by definition; what breaks first is the PATTERN, and these
      // watch for exactly that.
      const chrono = [...fresh].sort((a, b) => a.closeTime - b.closeTime);

      // (a) his per-order size changed — your copy scales with it 1:1
      for (const t of chrono) {
        if (Math.abs(t.lots - baseLot) > 1e-9) {
          alerts.push({ key: `sizechange-${Math.round(t.lots * 100)}`, p: 'high', tags: 'triangular_ruler',
            t: `📐 他的單筆手數變了:${n(t.lots)}(原 ${n(baseLot)})`,
            b: `你的跟單會等比放大 ${n(t.lots / baseLot, 1)} 倍(超出部分由 Max lot 上限擋住)。\n` +
               `懸崖計算以 ${n(baseLot)} 手為基礎 — 若他長期改用新手數,需要更新 baseLot。` });
        }
      }

      // (b) first real loss — his historical worst is -$0.80, so anything
      //     meaningfully deeper is the pattern breaking, not noise.
      const lossFloor = +trader.lossAlertUsd || 10;
      for (const t of chrono) {
        if (t.profit <= -lossFloor) {
          state.lastRealLossAt = now;
          alerts.push({ key: `realloss-${t.id}`, p: 'high', tags: 'small_red_triangle_down',
            t: `🔻 出現真實虧損 -$${n(Math.abs(t.profit))}`,
            b: `${t.side === 'long' ? '多' : '空'} ${n(t.lots)} 手 ${n(t.openPrice)}→${n(t.closePrice)}\n` +
               `你的等比虧損約 -$${n(Math.abs(t.profit) * copyMult)}。\n` +
               `這是模式改變的第一個訊號 — 留意接下來幾筆。` });
        }
      }

      // (c) shrink-the-cap detectors — all gated on actually copying, since
      //     "調小 Max lot" is meaningless for a watch-only trader.
      if (MY_EQUITY > 0) {
      const recent = state.recentResults || [];
      for (const t of chrono) recent.push(t.profit > 0 ? 1 : 0);
      state.recentResults = recent.slice(-20);
      // Shrink-the-cap advice rides Pushover, at the user's request: the
      // upsize nudge is a nice-to-have, but the downsize signal means the
      // regime is breaking and waiting until morning has a price.
      if (state.recentResults.length >= 20) {
        const wr = state.recentResults.reduce((a, b) => a + b, 0) / state.recentResults.length;
        if (wr < 0.6 && !state.wrLow) {
          state.wrLow = true;
          alerts.push({ key: 'winrate-drift', p: 'urgent', crit: true, tags: 'chart_with_downwards_trend',
            t: `🚨 建議調小 Max lot — 勝率劣化`,
            b: `近 20 筆勝率 ${n(wr * 100, 0)}%(他的歷史是 95%)。\n` +
               `策略碰到了沒見過的行情。\n\n` +
               `App → 跟單設定 → Max lot 砍半(${n(lotCap)} → ${n(lotCap / 2)}):風險即時減半、可逆。\n` +
               `更保守:直接停止跟單(會市價平掉所有部位)。` });
        } else if (wr >= 0.75) state.wrLow = false;
      }
      // Fast break: three losses inside the last five closes. His lifetime
      // base rate is 2 losses in 44 trades — three-in-five is not noise at any
      // confidence level, and the 20-trade gauge is too slow for a fast blowup.
      const last5 = state.recentResults.slice(-5);
      const losses5 = last5.filter((x) => x === 0).length;
      if (last5.length >= 5 && losses5 >= 3 && !state.fastDrift) {
        state.fastDrift = true;
        alerts.push({ key: 'fast-drift', p: 'urgent', crit: true, tags: 'rotating_light',
          t: `🚨 建議調小 Max lot — 近 5 筆虧了 ${losses5} 筆`,
          b: `他生涯 44 筆才虧 2 筆 — 這個密度是模式斷裂,不是雜訊。\n\n` +
             `App → 跟單設定 → Max lot 砍半(${n(lotCap)} → ${n(lotCap / 2)}):風險即時減半、可逆。\n` +
             `更保守:直接停止跟單(會市價平掉所有部位)。` });
      } else if (last5.length >= 5 && losses5 <= 1) state.fastDrift = false;

      // Win rate is a proxy; dollars are the target. A trader can hold a 90%
      // win rate all the way into ruin if the occasional loss grows large
      // enough to eat the wins — these two watch the money directly.
      const pnls = state.recentPnl || [];
      for (const t of chrono) pnls.push(t.profit);
      state.recentPnl = pnls.slice(-20);
      // (d) bleed: his last 20 trades net out at zero or worse, against a
      //     baseline of roughly +$226. High win rate with no money coming in
      //     IS the broken pattern, whatever the ratio says.
      if (state.recentPnl.length >= 20) {
        const net20 = state.recentPnl.reduce((a, b) => a + b, 0);
        if (net20 <= 0 && !state.bleedLow) {
          state.bleedLow = true;
          alerts.push({ key: 'bleed-20', p: 'urgent', crit: true, tags: 'rotating_light',
            t: `🚨 建議調小 Max lot — 近 20 筆淨損益 $${n(net20)}`,
            b: `他的基準是 20 筆約 +$226(你的等比 ≈ +$900)。\n` +
               `現在是 $${n(net20)} — 勝率再高,錢沒進來就是模式壞了。\n\n` +
               `App → Max lot 砍半(${n(lotCap)} → ${n(lotCap / 2)}),或停止跟單。` });
        } else if (net20 > 50) state.bleedLow = false;
      }
      // (e) realized drawdown from the profit peak since watching began —
      //     catches the slow grind whose every single event dodges the
      //     per-trade and per-batch thresholds.
      state.cumPnl = (state.cumPnl || 0) + chrono.reduce((a, t) => a + t.profit, 0);
      state.cumPeak = Math.max(state.cumPeak || 0, state.cumPnl);
      const dd = state.cumPeak - state.cumPnl;
      const ddLine = +trader.ddAlertHisUsd || 40;
      if (dd >= ddLine && !state.ddHit) {
        state.ddHit = true;
        const mult = copyMult || 1;
        alerts.push({ key: `dd-${Math.round(state.cumPeak)}`, p: 'urgent', crit: true, tags: 'rotating_light',
          t: `🚨 建議調小 Max lot — 已實現回撤 -$${n(dd * mult)}`,
          b: `他從獲利高點回吐 $${n(dd)}(你的等比約 -$${n(dd * mult)})。\n` +
             `沒有單一事件夠大,但累積方向錯了。\n\n` +
             `App → Max lot 砍半(${n(lotCap)} → ${n(lotCap / 2)}),或停止跟單。` });
      } else if (dd < ddLine / 2) state.ddHit = false;
      }

      // (f) the projection's inputs: fees at the measured $6/lot round-trip,
      //     and trade PnL at the lot multiple in force, 20% share off wins.
      state.feeSinceAnchor = (state.feeSinceAnchor || 0) + fresh.length * (state.curMyLot || 0) * 6;
      state.pnlSinceAnchor = (state.pnlSinceAnchor || 0) + chrono.reduce((a, t) =>
        a + (t.profit > 0 ? t.profit * copyMult * 0.8 : t.profit * copyMult), 0);
    }
    state.histSeeded = true;
  }

  // Deferred cap-raise emission — the regime flags are now this pass's.
  // The user chose a fixed-cap policy until the 100-trade review: profits sit
  // out, exposure stays constant. Raising advice before that is noise by their
  // own decision, so it is muted; the milestone alert carries the reminder.
  const tradesSoFar = +perf.totalTrades || 0;
  const capMuted = trader.capPolicy === 'fixed' && tradesSoFar < (+trader.capReviewAtTrades || 0);
  if (pendingCapraise && MY_EQUITY > 0 && !capMuted) {
    const { stepsOver, rawSteps, myEqNow } = pendingCapraise;
    const recentLoss = state.lastRealLossAt && now - state.lastRealLossAt < 3 * 864e5;
    if (!state.wrLow && !recentLoss) {
      const newCap = rawSteps / 100;
      alerts.push({ key: `capraise-${rawSteps}`, p: 'default', cool: 1440, tags: 'chart_with_upwards_trend',
        t: `📈 獲利已長到可調高 Max lot`,
        b: `推估權益 $${n(myEqNow)} → 自然手數 ${n(newCap)},上限還釘在 ${n(lotCap)}。\n` +
           `App 把 Max lot 調到 ${n(newCap)} 可讓獲利投入運轉` +
           `(約 +${n(stepsOver / (lotCap * 100) * 100, 0)}% 月獲利)。\n` +
           `⚠️ 風險等比放大:壞堆疊尾部從約 -$${n(lotCap * 5 * 3000, 0)} 變 -$${n(newCap * 5 * 3000, 0)}。\n` +
           `⚠️ 每單停損也要同步調到 $${n(newCap * 3000, 0)} — 停損是美元定義的,` +
           `只調手數會讓觸發線變緊($30/oz → $${n(300 / (newCap * 100), 1)}/oz)。\n\n` +
           `調整後回報:①App 新上限+新停損 ②實際權益,我同步 config。` });
    }
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
    // Uses the drift-corrected projection like the cliff maths does — the raw
    // snapshot overstated leverage by exactly the unreported drift. Per-order
    // sizing comes from the shared ctx, not a hardcoded 0.02.
    const myEqRefX = state.myEqEstimate || MY_EQUITY;
    const ratioX = myEqRefX / eq;
    const myLots = open.reduce((sum, p) =>
      sum + Math.min(Math.floor(p.lots * ratioX * 100) / 100, lotCap), 0);
    const perOrderLot = Math.max(state.curMyLot || 0, 0.01);
    const notional = myLots * OZ_PER_LOT * gold;
    const lev = notional / myEqRefX;
    const levCrit = trader.levCrit ?? 80;
    const levWarn = trader.levWarn ?? 55;
    if (lev >= levWarn) {
      const liqMove = myEqRefX / (myLots * OZ_PER_LOT);
      alerts.push({ key: `expo-${Math.round(lev / 10)}`, p: 'urgent', crit: lev >= levCrit,
        cool: 180, tags: 'warning',
        t: `${lev >= levCrit ? '🚨' : '⚠️'} 曝險 ${n(lev, 0)}x`,
        b: `他同時開 ${open.length} 單 → 你 ${n(myLots)} 手 = $${n(notional, 0)} 名目\n` +
           `你的權益約 $${n(myEqRefX)}\n\n` +
           `金價再逆走 $${n(liqMove, 0)} 就會清算。\n` +
           `每單停損 $${trader.stopPerOrder || '?'} 會在 $${n((trader.stopPerOrder || 0) / (perOrderLot * OZ_PER_LOT), 0)} 先觸發。` });
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
    // Same scale trap as the batch emergency: `float` is HIS-scale dollars.
    const multL = (state.curMyLot || 0) / baseLot;
    const floatUser = float != null ? float * multL : null;
    const bad = floatUser != null && myEqRef > 0 && multL > 0 && floatUser < -myEqRef * CFG.emergencyFloatPct;
    alerts.push({ key: `ladder-${side}-${leg.length}`, p: 'urgent', crit: bad, cool: 120,
      tags: 'chart_with_downwards_trend',
      t: `${bad ? '🚨' : '🔻'} 加碼攤平中(${side === 'long' ? '多' : '空'} ${leg.length} 單)`,
      b: `入場 ${n(px[0])} ~ ${n(px[px.length - 1])} (跨距 $${n(spread)})\n` +
         `總量 ${n(lots)} 手 = ${n(lots * OZ_PER_LOT, 0)} 盎司\n` +
         `逆勢 $${n(adverse)}` + (floatUser != null ? ` | 估計浮動你約 ${floatUser >= 0 ? '+' : ''}$${n(floatUser)}` : '') +
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
      alerts.push({ key: `stale-${oldest.id}`, p: 'high', crit: abandoned, cool: 360, tags: 'hourglass',
        t: `${abandoned ? '🚨 部位無人看管' : '⏳ 部位已持有'} ${n(ageH, 1)} 小時`,
        b: `${oldest.side === 'long' ? '多' : '空'} ${n(oldest.lots)} 手 @ ${n(oldest.openPrice)}\n` +
           `中位持倉只有 15 分鐘 — 這筆走反了。\n停損 ${oldest.sl ? n(oldest.sl) : '未設'}` +
           (abandoned ? `\n\n他已 ${n(ageH, 1)} 小時沒動作,且這筆沒掛停損。` : '') });
    }
  }

  // 5. quiet-period tracking. The bell itself now rings from section 1b on the
  //    first CLOSE after a quiet spell — the open view that used to gate it is
  //    blind, so the old condition could never be true. lastOrderTime also lags
  //    hours behind reality, so quietH is a coarse signal, fine for a 12h bar.
  const quietH = (now - lastOrder) / 3.6e6;
  if (quietH >= CFG.quietHours) state.wasQuiet = true;
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
    const eqMine = state.myEqEstimate || MY_EQUITY || 0;
    const lotAt = (hisEq) => Math.min(Math.floor(baseLot * (eqMine / hisEq) * 100) / 100, lotCap);
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
          b: `統計基礎比你進場時(44 筆)厚了 ${n(m / 44, 1)} 倍。\n值得重跑一次完整分析,再決定加碼、維持或退出。` +
             (trader.capPolicy === 'fixed' && m >= (+trader.capReviewAtTrades || Infinity)
               ? `\n\n你之前決定 Max lot 釘在 ${n(+trader.maxLotPerTrade || 0)} 到這時候再評估 — 現在到了。` : '') });
        break;
      }
    }
  }

  // Floating-loss alert via equity drift. His drawdown x copyMult ≈ yours.
  if (MY_EQUITY > 0 && inferOpen && eqDrift < 0) {
    const mult = (state.curMyLot || 0) / baseLot;
    const mine = eqDrift * mult;
    const myEqRef = state.myEqEstimate || MY_EQUITY;
    const pct = -mine / myEqRef;
    // Escalates in steps of the emergency line (6%, 12%, 18%, …): the first
    // version was one-shot per position and stayed silent while his float
    // went from -$54 to -$135 — he had added shorts and your exposure doubled.
    const step = Math.floor(pct / CFG.emergencyFloatPct);
    if (step >= 1 && step > (state.floatStep || 0)) {
      const stops = Math.ceil(-mine / (+trader.stopPerOrder || 300));
      state.floatStep = step;
      state.floatWarned = true;
      alerts.push({ key: `float-drift-${step}`, p: 'urgent', crit: true, cool: 30, tags: 'rotating_light',
        t: `🚨 他持倉中浮虧 ${n(-eqDrift)} → 你約 -${n(-mine)}${step > 1 ? '(擴大)' : ''}`,
        b: `他的權益從平倉基準 ${n(flatEq)} 掉到 ${n(eq)}。\n` +
           `以你的手數推估浮虧 -${n(-mine)}(權益 ${n(pct * 100, 0)}%)。\n` +
           (step > 1 ? `比上次警報更深 —— 他很可能加碼了;這麼深至少對應 ${stops} 單的 ${trader.stopPerOrder || 300} 停損。\n` : '') +
           `你的每單停損 ${trader.stopPerOrder || '?'} 在交易所端等著;` +
           `要提前出場只能 App → 停止跟單。\n\n` +
           `(監控看不到持倉,這是從他的權益反推的;權益端點有延遲。開 App 看真實數字。)` });
    } else if (pct >= CFG.emergencyFloatPct * 0.5 && !state.floatWarned && !state.floatHalf) {
      state.floatHalf = true;
      alerts.push({ key: 'float-half', p: 'high', cool: 60, tags: 'warning',
        t: `⚠️ 他持倉中浮虧 $${n(-eqDrift)} → 你約 -$${n(-mine)}`,
        b: `權益從基準 $${n(flatEq)} → $${n(eq)}。還沒到緊急線,但方向不對。` });
    }
  }
  if (!inferOpen) state.floatHalf = false;

  const burst = (state.burstUntil || 0) > now || inferOpen;
  const bs = (state.basis || []).slice(-20);
  const basisMed = bs.length ? bs.map((x) => Math.abs(x.close)).sort((a, b) => a - b)[bs.length >> 1] : null;
  return { alerts, eq, open, gold, quietH, burst, histOk, copied: MY_EQUITY > 0,
    inferOpen, basisMed, dayCloses: state.dayCloses || 0, dayPnlHis: state.dayPnlHis || 0,
    dayKey: state.dayKey, copyMult, myEqRefOut: state.myEqEstimate || MY_EQUITY,
    name: trader.name, id, daysLeft: state.daysLeft,
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
    const vib = process.env.TEST_VIBRATE === '1';
    await notify(vib ? '🔔 [測試] 清醒時段緊急警報' : crit ? '🚨 [測試] 緊急警報' : '[雲端] CFD 監控測試',
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
  let pass = 0;
  // Per-trader failure accounting: a shared counter reset by ANY success let
  // one broken trader hide behind the other working one forever.
  const failStreaks = {}, rateLimitedUntil = {};
  // Copy accounts drive the cadence; a watch-only trader polled at full tempo
  // was consuming half the 429-prone budget and dragging both traders into
  // bursts nobody is copying.
  const copiedTraders = TRADERS.filter((t) => +t.myEquity > 0);
  const cadenceTraders = copiedTraders.length ? copiedTraders : TRADERS;
  // Traders removed from config leave orphaned per-trader state behind.
  if (state.byTrader) {
    const valid = new Set(TRADERS.map((t) => t.portfolioId));
    for (const k of Object.keys(state.byTrader)) if (!valid.has(k)) delete state.byTrader[k];
  }
  let lastHistOk = false;

  do {
    pass++;
    const results = [];

    for (const trader of TRADERS) {
      const tid = trader.portfolioId;
      const watchOnly = !(+trader.myEquity > 0);
      // Watch-only traders get every 4th pass — minutes of latency on a feed
      // that requires no action, in exchange for half the request volume.
      if (watchOnly && pass % 4 !== 1) continue;
      if ((rateLimitedUntil[tid] || 0) > Date.now()) {
        log({ pass, trader: trader.name, skipped: 'rate-limited backoff' });
        continue;
      }
      try {
        results.push(await check(state, trader));
        failStreaks[tid] = 0;
      } catch (e) {
        failStreaks[tid] = (failStreaks[tid] || 0) + 1;
        log({ pass, trader: trader.name, error: e.message, failStreak: failStreaks[tid] });
        // A 429 means STOP, not retry harder — cfd.js already burned its
        // per-call retries; piling more requests into a throttle window is how
        // an IP block happens.
        if (/429|rate limit/i.test(e.message)) rateLimitedUntil[tid] = Date.now() + 5 * 60000;
        if (failStreaks[tid] === 3) {
          await notify(`⚠️ 監控連續失敗 · ${trader.name}`,
            `連續 ${failStreaks[tid]} 次無法取得資料:\n${e.message}`, 'high', 'warning');
        }
      }
      if (TRADERS.length > 1) await sleep(1500);
    }

    // Friday pre-close check, once a week while copying. Gold shuts 21:00 UTC
    // and copying cannot be stopped during the closure; we are blind to open
    // positions but the user's own app is not. Measured weekend drift: median
    // $10.3/oz, max $26.3/oz over ten weekends — and stops do not protect
    // against gaps (they fill at the reopen price).
    const nw = new Date();
    // Friday 12-14 UTC = Friday 20:00-22:00 Taipei — the user's evening, before
    // their 23:00 bedtime. The original 19-21 UTC window landed at 3am Taipei:
    // a non-waking reminder delivered while its audience slept, about a market
    // close happening before they woke.
    const mk = marketState(nw);
    const held = results.filter((r) => r.copied && r.inferOpen).map((r) => r.name);
    const holdTxt = held.length ? `推斷持倉中(${held.join('、')})` : '推斷空手';
    if (nw.getUTCDay() === 5 && nw.getUTCHours() >= 12 && nw.getUTCHours() < 14 && copiedTraders.length) {
      const wk = nw.toISOString().slice(0, 10);
      if (state.friNag !== wk) {
        state.friNag = wk;
        await notify('🕘 週五晚間檢查:黃金明晨 05:00(台北)休市',
          `目前 ${holdTxt}。休市後整個週末無法停止跟單。\n` +
          `開 App 看一眼跟單帳戶:\n` +
          `• 空倉 → 忽略這則,安心過週末\n` +
          `• 有持倉 → 想想要不要在休市前手動停止跟單:\n` +
          `  週末跳空中位 ±$103、實測最大 $263(你的 0.10 手尺度),\n` +
          `  疊滿 5 單最壞約 $1,314 — 停損擋不住跳空(以開盤價成交)。\n` +
          `若他 04:30 還抱著倉,會再叫你一次(緊急,重複到確認)。`,
          'high', 'calendar');
      }
    }
    // Last call before the weekly close: he is inferred to be holding and in
    // ≤30 minutes nothing can be undone until Sunday night. This is the one
    // weekly moment where a wake-up is justified — the decision window shuts.
    if (mk.open && mk.toWeekClose != null && mk.toWeekClose <= 30 && held.length && copiedTraders.length) {
      const wk = nw.toISOString().slice(0, 10);
      if (state.wkHoldCrit !== wk) {
        state.wkHoldCrit = wk;
        const ok = await notify(`🚨 他還抱倉,黃金 ${mk.toWeekClose} 分鐘後休市到週一`,
          `${holdTxt}。休市後整個週末不能停止跟單,停損擋不住週末跳空。\n` +
          `要出場,現在是最後機會:App → 跟單 → 停止跟單(市價平)。\n` +
          `不出場也可以 —— 這是提醒你做決定,不是叫你一定要動。`,
          'urgent', 'rotating_light', true);
        if (!ok) state.wkHoldCrit = null; // both channels failed → retry next pass
      }
    }
    // Record what he carried into the close; report it when you wake Saturday.
    if (nw.getUTCDay() === 5 && nw.getUTCHours() === 20 && nw.getUTCMinutes() >= 50) state.heldIntoWeekend = held;
    if (nw.getUTCDay() === 6 && nw.getUTCHours() === 2 && (state.heldIntoWeekend || []).length) {
      const wk = nw.toISOString().slice(0, 10);
      if (state.wkHoldNote !== wk) {
        state.wkHoldNote = wk;
        await notify('🛌 他抱倉過週末了',
          `休市前推斷 ${state.heldIntoWeekend.join('、')} 仍持倉。週一 06:00(台北)開盤前你我都動不了。\n` +
          `開盤瞬間可能跳空,停損以開盤價成交。開 App 確認實際部位與浮動盈虧。`,
          'high', 'sleeping');
      }
    }

    // Economic calendar: 30-minute warning before US high-impact prints, and
    // a Monday-morning list of the week. Louder when he is inferred holding.
    let ecoHot = false;
    if (copiedTraders.length) {
      const evs = await ecoEvents();
      state.ecoSent = state.ecoSent || {};
      for (const e of evs) {
        const min = (e.t - Date.now()) / 60000;
        if (min <= (CFG.ecoHotMinAfter ?? 15) * -1) continue;
        if (min <= (CFG.ecoHotMinBefore ?? 5)) ecoHot = true;
        const k = `${e.t}|${e.title}`;
        if (min > 0 && min <= (CFG.ecoWarnMin ?? 30) && !state.ecoSent[k]) {
          state.ecoSent[k] = Date.now();
          await notify(`📅 ${Math.round(min)} 分後 ${e.title}(台北 ${taipei(e.t)})`,
            `美國高影響數據。目前 ${holdTxt}。\n` +
            `數據瞬間點差放大、價格可能一根穿過你的停損再拉回;監控在此期間改最快輪詢。`,
            held.length ? 'high' : 'default', 'calendar');
        }
      }
      for (const k of Object.keys(state.ecoSent)) if (Date.now() - state.ecoSent[k] > 8 * 864e5) delete state.ecoSent[k];
      if (nw.getUTCDay() === 1 && nw.getUTCHours() === 0) {
        const wk = nw.toISOString().slice(0, 10);
        if (state.ecoWeek !== wk) {
          state.ecoWeek = wk;
          const list = evs.filter((e) => e.t > Date.now()).sort((a, b) => a.t - b.t)
            .map((e) => `• ${new Date(e.t + 8 * 3600e3).toISOString().slice(5, 10)} ${taipei(e.t)} ${e.title}`);
          await notify('📅 本週美國高影響數據(台北時間)',
            list.length ? list.join('\n') : '本週沒有高影響美元數據。', 'default', 'calendar');
        }
      }
    }

    // Evening reconciliation nudge: the monitor cannot see YOUR account, so
    // once a day, on days with fills, ask for the one number that catches a
    // silent divergence (skipped copies, a hit stop, a paused copy).
    if (nw.getUTCHours() === 12 && copiedTraders.length) {
      const today = nw.toISOString().slice(0, 10);
      const rs = results.filter((r) => r.copied && r.dayKey === today && r.dayCloses > 0);
      if (rs.length && state.reconNag !== today) {
        state.reconNag = today;
        const lines = rs.map((r) => `${r.name}:今日 ${r.dayCloses} 筆,他 ${r.dayPnlHis >= 0 ? '+' : ''}$${n(r.dayPnlHis)} → 你估 ${r.dayPnlHis * r.copyMult >= 0 ? '+' : ''}$${n(r.dayPnlHis * r.copyMult)}(分潤前)。監控推估你的權益 ≈ $${n(r.myEqRefOut)}`);
        await notify('🧾 今日對帳(監控看不到你的帳戶)',
          lines.join('\n') + `\n\n開 App 看跟單帳戶權益;若差超過 5%,代表有單沒跟到、停損被打、或跟單被暫停 —— 回我一聲。`,
          'default', 'receipt');
      }
    }

    if (results.length) {
      // The heartbeat asserts "monitoring works", not "the process is alive" —
      // if every history fetch failed, the dead man's switch SHOULD fire.
      lastHistOk = results.some((r) => r.histOk);
      if (lastHistOk) await heartbeat();
      const sent = state.sent || {};
      for (const r of results) {
        log({ pass, trader: r.name, eq: r.eq, myEq: r.myEq ? +r.myEq.toFixed(2) : null,
          open: r.open.length, gold: r.gold,
          ratio: r.ratio ? +r.ratio.toFixed(4) : null, quietH: +r.quietH.toFixed(1),
          inferOpen: r.inferOpen, basisMed: r.basisMed, market: mk.open ? 'open' : mk.why,
          daysLeft: r.daysLeft != null ? +r.daysLeft.toFixed(1) : null,
          // Keys can embed sizing/step numbers; digits are stripped so the
          // public Actions log carries the alert TYPE without the figures.
          alerts: r.alerts.map((a) => (VERBOSE ? a.key : a.key.replace(/\d+/g, '#'))) });

        for (const a of r.alerts) {
          // Namespaced so the same condition on two traders alerts twice.
          // Standing conditions carry their own longer `cool` (minutes).
          const key = `${r.id}|${a.key}`;
          const cd = (a.cool != null ? a.cool * 60000 : COOLDOWN);
          if (sent[key] && Date.now() - sent[key] < cd) continue;
          const title = r.name ? `${a.t} · ${r.name}` : a.t;
          const delivered = await notify(title, a.b, a.p, a.tags, !!a.crit, traderUrl(r.id));
          // A critical alert that failed BOTH channels must retry next pass,
          // not sit out its cooldown while the condition worsens.
          if (delivered || !a.crit) sent[key] = Date.now();
        }
      }
      for (const k of Object.keys(sent)) if (Date.now() - sent[k] > 7 * 864e5) delete sent[k];
      state.sent = sent;
      saveState(state);
    }

    // Step aside for a newer deploy (checked at most every 5 minutes).
    if (Date.now() - (globalThis.__lastShaCheck || 0) > 300000) {
      globalThis.__lastShaCheck = Date.now();
      if (await newerCommitExists()) {
        log({ selfSupersede: true, note: 'newer commit on main; yielding to the queued run' });
        saveState(state);
        break;
      }
    }

    const anyOpen = results.some((r) => r.copied && (r.open.length > 0 || r.burst)) || ecoHot;
    const wait = cadenceSec(anyOpen, cadenceTraders) * 1000;
    if (Date.now() + wait > deadline) break;
    // Chunked sleep: long cadences (weekend 30 min) starved the heartbeat and
    // forced the dead man's grace window wide open. Beating every ≤5 min keeps
    // the external check tight; heartbeat() self-limits to every 10 min.
    let remaining = wait;
    while (remaining > 0 && Date.now() < deadline) {
      const slice = Math.min(remaining, 300000);
      await sleep(slice);
      remaining -= slice;
      if (lastHistOk) await heartbeat();
    }
  } while (Date.now() < deadline);

  saveState(state);
  log({ done: true, passes: pass });
})();
