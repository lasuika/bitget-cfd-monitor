// Client for Bitget's CFD (gold / FX / indices) copy-trading data.
//
// Bitget's official v2 REST API has no CFD coverage at all — every /api/v2/copy/cfd-*
// path 404s and productType=CFD is rejected. The web app instead talks to an
// internal MT5 service at /v1/trace/mt5/public/*, which needs no signing and is
// reachable from plain Node (no Akamai challenge on these endpoints).
//
// Paging shape was taken from the page's own XHR:
//   {languageType, portfolioId, pageNo, pageSize, lastEndId, pre}
// lastEndId must be a NUMBER; sending it as a string is silently ignored and you
// get page 1 back forever.
const https = require('https');

const HOST = 'www.bitget.com';
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function once(path, body, portfolioId) {
  const data = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: HOST, path, method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        'User-Agent': UA,
        Accept: 'application/json',
        Origin: `https://${HOST}`,
        Referer: `https://${HOST}/copy-trading/cfd-trader/${portfolioId || ''}`,
      },
    }, (res) => {
      let b = '';
      res.on('data', (c) => (b += c));
      res.on('end', () => {
        if (res.statusCode === 429) {
          const e = new Error('rate limited (429)'); e.retryable = true; return reject(e);
        }
        try {
          const j = JSON.parse(b);
          if (j.code && String(j.code) !== '200') {
            return reject(new Error(`CFD ${j.code}: ${j.msg || 'error'}`));
          }
          resolve(j.data);
        } catch {
          const e = new Error(`bad response (HTTP ${res.statusCode}): ${b.slice(0, 160)}`);
          e.retryable = res.statusCode >= 500 || res.statusCode === 403;
          reject(e);
        }
      });
    });
    req.on('error', (e) => { e.retryable = true; reject(e); });
    req.setTimeout(20000, () => req.destroy(Object.assign(new Error('timeout'), { retryable: true })));
    req.write(data);
    req.end();
  });
}

// Bitget rate-limits this internal endpoint hard, and hammering it after a 429 is
// how you get an IP-level block rather than a throttle. Back off aggressively and
// give up rather than grind.
async function post(path, body, portfolioId, tries = 5) {
  let wait = 2000;
  for (let i = 0; i < tries; i++) {
    try {
      return await once(path, body, portfolioId);
    } catch (e) {
      if (!e.retryable || i === tries - 1) throw e;
      process.stderr.write(`  ${e.message} — ${wait / 1000}s 後重試 (${i + 1}/${tries - 1})\n`);
      await sleep(wait);
      wait *= 2;
    }
  }
}

const cfd = {
  details: (id) => post('/v1/trace/mt5/public/details', { portfolioId: id, languageType: 0 }, id),
  performance: (id) => post('/v1/trace/mt5/public/performance', { portfolioId: id, languageType: 0 }, id),
  // Money moved between his elite portfolio and spot. transferType 1 = out
  // (a sweep: your lot denominator shrinks), 0 = in.
  transfers: (id) => post('/v1/trace/mt5/public/getTransferHistory',
    { portfolioId: id, languageType: 0, pageNo: 1, pageSize: 10 }, id)
    .then((d) => (d.rows || []).map((r) => ({
      t: +r.transferTime, amount: +r.transferAmount, out: +r.transferType === 1,
      from: r.fromAccountType, to: r.toAccountType }))),

  openPositions: (id) => post('/v1/trace/mt5/public/currentPosition',
    { portfolioId: id, languageType: 0, pageNo: 1, pageSize: 50, pre: false }, id)
    .then((d) => d.rows || []),

  // Walk the full closed-trade history. Stops when a page yields nothing new,
  // which also guards against the cursor looping back to page 1.
  async history(id, { maxPages = 40, pageSize = 50, delay = 1200 } = {}) {
    const out = [];
    const seen = new Set();
    let lastEndId = null;
    for (let pageNo = 1; pageNo <= maxPages; pageNo++) {
      const body = { languageType: 0, portfolioId: id, pageNo, pageSize, pre: false };
      if (lastEndId != null) body.lastEndId = Number(lastEndId);
      const d = await post('/v1/trace/mt5/public/historyPosition', body, id);
      const rows = d.rows || [];
      if (!rows.length) break;
      let fresh = 0;
      for (const r of rows) {
        if (!seen.has(r.positionId)) { seen.add(r.positionId); out.push(r); fresh++; }
      }
      lastEndId = d.endId;
      if (!fresh || !d.nextFlag) break;
      await sleep(delay);
    }
    return out;
  },
};

// Normalise a raw MT5 row into the shape the rest of the tooling uses.
// directionType 1 = sell/short, 0 = buy/long. One XAUUSD lot is 100 oz — verified
// against the fills: 0.01 lots on a $2.40 move returns exactly $2.40.
const OZ_PER_LOT = 100;
function norm(r) {
  return {
    id: r.positionId,
    symbol: r.symbol,
    side: r.directionType === 1 ? 'short' : 'long',
    lots: +r.totalVolume,
    oz: +r.totalVolume * OZ_PER_LOT,
    openTime: +r.openTime,
    closeTime: +r.closeTime || null,
    openPrice: +r.openPrice,
    closePrice: +r.closePrice || null,
    profit: +r.totalProfit,
    commission: +r.commission || 0,
    sl: +r.sl || null,
    tp: +r.tp || null,
    comment: r.comment || '',
    closedBy: /\[sl/.test(r.comment || '') ? 'sl'
      : /\[tp/.test(r.comment || '') ? 'tp' : 'manual',
  };
}

module.exports = { cfd, norm, OZ_PER_LOT, sleep };
