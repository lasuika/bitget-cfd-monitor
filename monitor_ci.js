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
const LOOP_MINUTES = +(process.env.LOOP_MINUTES ?? 50);
const TEST_ALERT = !!process.env.TEST_ALERT;
// Your CFD equity. Kept in a workflow variable so it can be updated from the
// GitHub UI after a deposit without editing and committing the repo.
const MY_EQUITY = +(process.env.MY_EQUITY || CFG.myEquity || 0);

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
function notify(title, body, priority = 'default', tags = '') {
  if (!NTFY_TOPIC) { log({ notifySkipped: title }); return Promise.resolve(); }
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
    }, (res) => { res.resume(); res.on('end', resolve); });
    req.on('error', (e) => { log({ notifyError: e.message }); resolve(); });
    req.setTimeout(10000, () => { req.destroy(); resolve(); });
    req.write(data); req.end();
  });
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

  // 1. equity cliff — 95% of his trades are 0.01 lots, the MT5 minimum, and
  //    copier lots round DOWN. Under 1.0x they become zero and stop copying.
  if (MY_EQUITY > 0 && eq > 0) {
    const ratio = MY_EQUITY / eq;
    if (ratio < 1.0) {
      alerts.push({ key: 'cliff-below', p: 'urgent', tags: 'rotating_light',
        t: '🔴 跌破跟單門檻 — 你已經跟不到單',
        b: `你 $${n(MY_EQUITY)} ÷ 他 $${n(eq)} = ${n(ratio, 4)}x\n\n` +
           `他 95% 的單是 0.01 手,乘上你的比例後無條件捨去成 0。\n` +
           `補足 $${n(eq - MY_EQUITY)} 才能恢復完整跟單。\n` +
           `(補到 1.3x 需要 $${n(eq * 1.3 - MY_EQUITY)})` });
    } else if (ratio < CFG.ratioWarn) {
      alerts.push({ key: 'cliff-warn', p: 'high', tags: 'warning',
        t: `🟡 跟單比例 ${n(ratio, 3)}x — 逼近門檻`,
        b: `他的權益 $${n(eq)} 還在漲,你的比例會繼續掉。\n` +
           `跌破 1.0x 就完全跟不到單。\n` +
           `補到 1.3x 需要 $${n(eq * 1.3 - MY_EQUITY)}。` });
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
    alerts.push({ key: `ladder-${side}-${leg.length}`, p: 'urgent', tags: 'chart_with_downwards_trend',
      t: `🔻 加碼攤平中(${side === 'long' ? '多' : '空'} ${leg.length} 單)`,
      b: `入場 ${n(px[0])} ~ ${n(px[px.length - 1])} (跨距 $${n(spread)})\n` +
         `總量 ${n(lots)} 手 = ${n(lots * OZ_PER_LOT, 0)} 盎司\n` +
         `逆勢 $${n(adverse)}` + (float != null ? ` | 估計浮動 ${float >= 0 ? '+' : ''}$${n(float)}` : '') +
         `\n\n他的加碼梯歷史上平均虧 $110,最深一次抱了 41 小時。` });
  }

  // 4. stale position — median hold is ~15 min; hours old means it went wrong.
  const oldest = open.reduce((a, p) => (a == null || p.openTime < a.openTime ? p : a), null);
  if (oldest) {
    const ageH = (now - oldest.openTime) / 3.6e6;
    if (ageH >= CFG.staleHours) {
      alerts.push({ key: `stale-${oldest.id}`, p: 'high', tags: 'hourglass',
        t: `⏳ 部位已持有 ${n(ageH, 1)} 小時`,
        b: `${oldest.side === 'long' ? '多' : '空'} ${n(oldest.lots)} 手 @ ${n(oldest.openPrice)}\n` +
           `中位持倉只有 15 分鐘 — 這筆走反了。\n停損 ${oldest.sl ? n(oldest.sl) : '未設'}` });
    }
  }

  // 5. back from the announced break.
  const quietH = (now - lastOrder) / 3.6e6;
  if (state.wasQuiet && fresh.length) {
    alerts.push({ key: 'resumed', p: 'high', tags: 'bell',
      t: '🔔 他恢復交易了',
      b: `沉寂 ${n(state.quietPeakH || quietH, 1)} 小時後重新開單。` +
         (MY_EQUITY > 0 ? `\n你的比例 ${n(MY_EQUITY / eq, 3)}x` : '') });
    state.quietPeakH = 0;
  }
  state.wasQuiet = quietH >= CFG.quietHours;
  if (state.wasQuiet) state.quietPeakH = Math.max(state.quietPeakH || 0, quietH);

  state.lastEquity = eq;
  return { alerts, eq, open, gold, quietH, ratio: MY_EQUITY > 0 ? MY_EQUITY / eq : null };
}

// --- loop ---------------------------------------------------------------
(async () => {
  const state = loadState();

  if (TEST_ALERT) {
    await notify('[雲端] CFD 監控測試',
      `ManuGoldPrime 監控運作正常。\n${new Date().toISOString()}`, 'default', 'white_check_mark');
    log({ testAlertSent: true });
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
      log({ pass, eq: r.eq, open: r.open.length, gold: r.gold,
        ratio: r.ratio ? +r.ratio.toFixed(4) : null, quietH: +r.quietH.toFixed(1),
        alerts: r.alerts.map((a) => a.key) });

      const sent = state.sent || {};
      for (const a of r.alerts) {
        if (sent[a.key] && Date.now() - sent[a.key] < COOLDOWN) continue;
        await notify(a.t, a.b, a.p, a.tags);
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
