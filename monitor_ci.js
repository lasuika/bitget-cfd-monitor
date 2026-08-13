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
// Bark is the emergency channel; without a key the monitor still runs on ntfy
// alone, it just loses the ability to break through Do Not Disturb.
const BARK_KEY = process.env.BARK_KEY || '';
const BARK_ALL = process.env.BARK_ALL === '1';
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
const log = (o) => console.log(JSON.stringify({ t: new Date().toISOString(), ...o }));

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
// Bark carries emergencies only, because it is the one that can actually wake
// you: level=critical rings through silent mode and Do Not Disturb, which ntfy
// cannot do on iOS. Spending that on routine alerts would train you to silence
// the app, so it is reserved for the cases where you must act now.
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

// POST rather than the path-based GET form so the body can contain newlines,
// slashes and Chinese without any URL-encoding surprises.
function bark(title, body, { critical = false, url = null } = {}) {
  if (!BARK_KEY) return Promise.resolve(false);
  const payload = {
    title, body,
    device_key: BARK_KEY,
    group: 'CFD跟單',
    sound: critical ? 'alarm' : 'bell',
    ...(critical ? { level: 'critical', volume: CFG.barkVolume ?? 8, call: '1' }
                 : { level: 'timeSensitive' }),
    ...(url ? { url } : {}),
  };
  const data = Buffer.from(JSON.stringify(payload), 'utf8');
  const host = (process.env.BARK_SERVER || 'api.day.app').replace(/^https?:\/\//, '').replace(/\/$/, '');
  return new Promise((resolve) => {
    const req = https.request({
      hostname: host, path: '/push', method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': data.length },
    }, (res) => {
      let b = ''; res.on('data', (c) => (b += c));
      res.on('end', () => {
        const ok = res.statusCode < 300;
        if (!ok) log({ barkError: `HTTP ${res.statusCode}: ${b.slice(0, 120)}` });
        resolve(ok);
      });
    });
    req.on('error', (e) => { log({ barkError: e.message }); resolve(false); });
    req.setTimeout(10000, () => { req.destroy(); resolve(false); });
    req.write(data); req.end();
  });
}

const TRADER_URL = `https://www.bitget.com/copy-trading/cfd-trader/${CFG.portfolioId}`;

// Emergencies go out on both channels — if one is down you still get told.
async function notify(title, body, priority = 'default', tags = '', critical = false) {
  const full = critical ? `${body}\n\n→ 只有你能處理:開 Bitget App 停止跟單或平倉。` : body;
  const [n1, n2] = await Promise.all([
    ntfy(title, full, critical ? 'max' : priority, tags),
    critical || BARK_ALL ? bark(title, full, { critical, url: TRADER_URL }) : Promise.resolve(null),
  ]);
  log({ sent: title, ntfy: n1, bark: n2, critical });
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
    // Use the projection for the cliff maths — a stale snapshot would keep
    // reporting a ratio you no longer have.
    const myEqNow = state.myEqEstimate || MY_EQUITY;
    const ratio = myEqNow / eq;
    const step = Math.floor(ratio);          // integer multiple currently held
    const myLot = step / 100;                // your size for his 0.01 lots
    const toNextFloor = ratio - step;        // headroom before size drops
    const topUpTo = (r) => eq * r - myEqNow;

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
      const hist = state.eqHist || [];
      hist.push({ t: now, eq });
      state.eqHist = hist.filter((h) => now - h.t < 21 * 864e5);

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

  if (TEST_ALERT) {
    const crit = process.env.TEST_CRITICAL === '1';
    await notify(crit ? '🚨 [測試] 緊急警報' : '[雲端] CFD 監控測試',
      crit
        ? `這是緊急通道測試。\n真實情況下代表加碼梯浮虧過大或部位無人看管。\n${new Date().toISOString()}`
        : `ManuGoldPrime 監控運作正常。\n${new Date().toISOString()}`,
      'default', 'white_check_mark', crit);
    log({ testAlertSent: true, critical: crit, barkConfigured: !!BARK_KEY });
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
