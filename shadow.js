// Shadow-ladder model for a hidden-position ladder trader (星火). Estimates,
// from the perp 1m price path since his last KNOWN close, whether he is holding,
// how many legs, which side and the floating PnL. Out-of-sample (3,184 min):
// holding acc 0.85 / recall 0.96, side acc 0.90, float corr 0.81, MAE $97 and
// a systematic ~1/3 UNDER-estimate of depth. It is a risk thermometer, not a
// position feed: it may drive polling cadence and annotate alerts, never wake
// anyone on its own. Ported verbatim from bitget-monitor/shadow-ladder.js;
// refit there every ~2 weeks and copy shadow-params.json over.
const MIN = 60000;
const floorMin = (t) => Math.floor(t / MIN) * MIN;

function marketClosed(ts) {
  const d = new Date(ts), dow = d.getUTCDay(), h = d.getUTCHours();
  if (dow === 6) return true;
  if (dow === 5 && h >= 21) return true;
  if (dow === 0 && h < 22) return true;
  return h === 21;
}

function newState(ts, lastSide) {
  return { legs: [], side: null, flatSince: ts, lastSide, lastCloseAt: ts };
}

function estimate(state, cfdPrice) {
  if (!state.legs.length) return { holding: false, n: 0, side: null, avg: null, oz: 0, float: 0 };
  let oz = 0, cost = 0, fl = 0;
  for (const l of state.legs) { oz += l.oz; cost += l.oz * l.price; }
  const avg = cost / oz;
  fl = (state.side === 'long' ? 1 : -1) * (cfdPrice - avg) * oz;
  return { holding: true, n: state.legs.length, side: state.side, avg, oz, float: fl };
}

function step(state, ts, candles, P) {
  const row = candles.map.get(ts);
  if (!row) return null;
  const p = row[4] - P.offset; // cfd-equivalent price
  if (state.legs.length) {
    const sgn = state.side === 'long' ? 1 : -1;
    const last = state.legs[state.legs.length - 1];
    let oz = 0, cost = 0; for (const l of state.legs) { oz += l.oz; cost += l.oz * l.price; }
    const avg = cost / oz;
    // take profit: 'avg' = vs avg entry; 'first' = vs first (worst) leg, i.e. all legs green
    const ref = P.tpMode === 'first' ? state.legs[0].price : avg;
    if (sgn * (p - ref) >= P.tp) {
      if (P.selfClose) { // legacy: model closes itself
        state.lastSide = state.side; state.legs = []; state.side = null; state.flatSince = ts;
        return estimate(state, p);
      }
      // default: only a reported close (reset) can flatten us. TP hit without a report within lag
      // means his target is higher -> just flag it.
      state.tpHitAt = state.tpHitAt || ts;
      const e = estimate(state, p); e.tpHit = true; e.tpHitMin = (ts - state.tpHitAt) / MIN; return e;
    }
    state.tpHitAt = null;
    // add against
    if (state.legs.length < P.maxLegs && sgn * (p - last.price) <= -P.addSpacing) {
      state.legs.push({ price: p, oz: P.addLot * 100, ts });
    }
    return estimate(state, p);
  }
  // flat: entry rule
  if (marketClosed(ts)) return estimate(state, p);
  if (P.quiet && P.quiet.length) { const h = new Date(ts).getUTCHours(); if (h >= P.quiet[0] && h < P.quiet[1]) return estimate(state, p); }
  const waited = (ts - state.flatSince) / MIN;
  if (waited < P.delay) return estimate(state, p);
  const prev = candles.map.get(ts - P.lookback * MIN);
  const move = prev ? row[4] - prev[4] : 0;
  let side = null;
  if (Math.abs(move) >= P.thresh) side = move > 0 ? 'short' : 'long';
  else if (P.maxWait != null && waited >= P.maxWait) side = move > 0 ? 'short' : move < 0 ? 'long' : (state.lastSide || 'long');
  if (side) { state.side = side; state.legs = [{ price: p, oz: P.firstLot * 100, ts }]; }
  return estimate(state, p);
}

function simulate(candles, closes, P, from, to, lastSideAt) {
  const out = new Map();
  const segStarts = [from, ...closes.filter(c => c > from && c < to)];
  for (let i = 0; i < segStarts.length; i++) {
    const s = segStarts[i];
    const knownAt = i === 0 ? from : s + P.lag * MIN;
    const segEnd = i + 1 < segStarts.length ? segStarts[i + 1] + P.lag * MIN : to;
    const st = newState(floorMin(s), lastSideAt ? lastSideAt(s) : null);
    for (let ts = floorMin(s); ts < segEnd; ts += MIN) {
      const e = step(st, ts, candles, P);
      if (e && ts >= knownAt) out.set(ts, e);
    }
  }
  return out;
}

// Run from the last known close to now over an in-memory candle map.
function shadowNow(candles, lastCloseTs, nowTs, P, lastSide) {
  const from = floorMin(lastCloseTs), to = floorMin(nowTs) + MIN;
  const st = newState(from, lastSide || null);
  let e = null;
  for (let ts = from; ts < to; ts += MIN) { const r = step(st, ts, candles, P); if (r) e = r; }
  return e;
}
module.exports = { shadowNow, marketClosed, MIN };
