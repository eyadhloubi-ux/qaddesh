/* قدّيش؟ — سيرفر قعدة رواق · صار بدها قعدة · إعداد م. إياد حلوبي
   السيرفر بس بيمرّر الرسائل بين الموبايلات، وحالة اللعبة عند الريس.
   بيحتفظ بآخر نسخة من الحالة حتى إذا الريس غاب، حدا تاني يقدر يستلم القيادة. */
'use strict';
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const STATS_KEY = process.env.STATS_KEY || 'qaddesh';
const CLAIM_MS = +(process.env.CLAIM_MS || 45000);   // بعد قديش من غياب الريس بيقدر حدا يستلم
const ROOM_TTL = 8 * 3600e3;                          // عمر القعدة الأقصى
const IDLE_TTL = 2 * 3600e3;                          // قعدة فاضية بتنمسح بعد ساعتين
const now = () => Date.now();

/* ===== الإحصائيات: ملف، أو Postgres إذا في DATABASE_URL ===== */
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const STATS_FILE = path.join(DATA_DIR, 'stats.json');
const blank = () => ({ open: 0, demo: 0, rooms: 0, players: 0, started: 0, finished: 0, questions: 0, takeovers: 0, days: {} });
let stats = blank(), pg = null, dirty = false;
async function statsInit() {
  if (process.env.DATABASE_URL) {
    try {
      const { Pool } = require('pg');
      pg = new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.PGSSL === 'off' ? false : { rejectUnauthorized: false } });
      await pg.query('create table if not exists qaddesh_stats (id int primary key, data jsonb)');
      const r = await pg.query('select data from qaddesh_stats where id=1');
      if (r.rows[0]) stats = Object.assign(blank(), r.rows[0].data);
      console.log('stats: postgres');
      return;
    } catch (e) { console.warn('postgres غير متاح، منرجع للملف:', e.message); pg = null; }
  }
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); stats = Object.assign(blank(), JSON.parse(fs.readFileSync(STATS_FILE, 'utf8'))); } catch (e) { }
}
function hit(ev, n = 1) {
  if (!(ev in stats)) return;
  stats[ev] += n; const d = new Date().toISOString().slice(0, 10);
  stats.days[d] = stats.days[d] || {}; stats.days[d][ev] = (stats.days[d][ev] || 0) + n; dirty = true;
}
setInterval(async () => {
  if (!dirty) return; dirty = false;
  try {
    if (pg) await pg.query('insert into qaddesh_stats (id,data) values (1,$1) on conflict (id) do update set data=$1', [stats]);
    else fs.writeFileSync(STATS_FILE, JSON.stringify(stats));
  } catch (e) { console.warn('stats save', e.message); }
}, 10000).unref();

/* ===== HTTP ===== */
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '4kb' }));
app.get('/healthz', (req, res) => res.send('ok'));
app.post('/api/hit', (req, res) => { const ev = String((req.body || {}).ev || ''); if (['open', 'demo'].includes(ev)) hit(ev); res.json({ ok: 1 }); });
app.get('/stats', (req, res) => {
  if (req.query.key !== STATS_KEY) return res.status(403).send('غير مسموح');
  const days = Object.entries(stats.days).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 30);
  const row = (k, v) => `<tr><td>${k}</td><td>${v}</td></tr>`;
  res.type('html').send(`<!doctype html><html lang="ar" dir="rtl"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>إحصائيات قدّيش؟</title><style>body{font-family:system-ui,sans-serif;max-width:640px;margin:0 auto;padding:24px 16px;background:#F4F1EA;color:#1A1C24}
h1{font-size:26px}table{width:100%;border-collapse:collapse;margin:12px 0 28px;background:#fff;border:2px solid #1A1C24;border-radius:10px;overflow:hidden}
td,th{padding:9px 12px;border-bottom:1px solid #DCD6C8;text-align:right}td:last-child,th:last-child{text-align:left;font-variant-numeric:tabular-nums}</style>
<h1>إحصائيات لعبة «قدّيش؟»</h1><table>
${row('مرات فتح اللعبة', stats.open)}${row('مرات تجربة الوضع التجريبي', stats.demo)}${row('القعدات المفتوحة', stats.rooms)}
${row('اللاعبون الذين دخلوا', stats.players)}${row('الألعاب التي بدأت', stats.started)}${row('الألعاب التي اكتملت', stats.finished)}
${row('الأسئلة التي لُعبت', stats.questions)}${row('مرات استلام القيادة بعد غياب الريس', stats.takeovers)}
${row('القعدات النشطة الآن', rooms.size)}</table>
<h2>آخر 30 يوماً</h2><table><tr><th>اليوم</th><th>قعدات</th><th>لاعبون</th><th>اكتملت</th></tr>
${days.map(([d, v]) => `<tr><td>${d}</td><td>${v.rooms || 0}</td><td>${v.players || 0}</td><td>${v.finished || 0}</td></tr>`).join('')}</table>
<p style="color:#6A6C75;font-size:13px">لا تُحفظ أي أسماء أو صور، أرقام فقط.</p></html>`);
});
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders(res, p) { if (p.endsWith('.html') || p.endsWith('sw.js')) res.setHeader('Cache-Control', 'no-cache'); }
}));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 6 * 1024 * 1024 });

/* ===== القعدات ===== */
// room: {code,key,hostPid,host(ws),players:Map pid→ws,snap,photos,offAt,born,touched,banned:Set,claimTimer}
const rooms = new Map();
const ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const newCode = () => { let c; do { c = Array.from(crypto.randomBytes(5), b => ALPHA[b % ALPHA.length]).join(''); } while (rooms.has(c)); return c; };
const newKey = () => crypto.randomBytes(16).toString('hex');
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const online = r => [...r.players.keys()];
function bcast(r, o, except) { for (const [pid, ws] of r.players) if (pid !== except) send(ws, o); }
function mkRoom(code, key, pid) {
  const r = { code, key, hostPid: pid, host: null, players: new Map(), snap: null, photos: {}, offAt: null, born: now(), touched: now(), banned: new Set(), claimTimer: null };
  rooms.set(code, r); return r;
}
function hostOff(r) {
  r.host = null; r.offAt = now(); bcast(r, { t: 'hostoff' });
  clearTimeout(r.claimTimer);
  r.claimTimer = setTimeout(() => { if (!r.host && r.snap) bcast(r, { t: 'canclaim' }); }, CLAIM_MS);
}
function promote(r, pid, reason) {
  const ws = r.players.get(pid); if (!ws || !r.snap) return false;
  const old = r.hostPid, oldWs = r.host;
  r.key = newKey(); r.hostPid = pid; r.host = ws; r.offAt = null; clearTimeout(r.claimTimer);
  r.players.delete(pid); ws._role = 'host'; ws._pid = pid;
  if (oldWs) { oldWs._role = null; send(oldWs, { t: 'demoted', code: r.code }); }
  send(ws, { t: 'promoted', code: r.code, key: r.key, snap: r.snap, photos: r.photos, players: online(r), old, reason });
  bcast(r, { t: 'newhost', pid });
  if (reason === 'claim') hit('takeovers');
  return true;
}

wss.on('connection', ws => {
  ws._alive = true; ws.on('pong', () => { ws._alive = true; });
  ws.on('message', buf => {
    let m; try { m = JSON.parse(buf); } catch (e) { return; }
    const r = ws._code ? rooms.get(ws._code) : null; if (r) r.touched = now();
    switch (m.t) {
      case 'create': {
        const code = newCode(), key = newKey(), nr = mkRoom(code, key, String(m.pid || ''));
        nr.host = ws; ws._code = code; ws._role = 'host'; ws._pid = nr.hostPid;
        send(ws, { t: 'created', code, key }); hit('rooms'); break;
      }
      case 'host': {
        const code = String(m.code || '').toUpperCase(); let x = rooms.get(code);
        if (!x) { x = mkRoom(code, String(m.key || ''), String(m.pid || '')); }   // السيرفر رجع اشتغل: منرجّع القعدة
        if (x.key !== m.key) { send(ws, { t: 'demoted', code }); break; }       // في حدا استلم مكانه
        if (x.host && x.host !== ws) { try { x.host._role = null; x.host.close(); } catch (e) { } }
        x.host = ws; x.offAt = null; clearTimeout(x.claimTimer); ws._code = code; ws._role = 'host'; ws._pid = x.hostPid;
        send(ws, { t: 'hosted', players: online(x) }); bcast(x, { t: 'hoston' }); break;
      }
      case 'join': {
        const code = String(m.code || '').toUpperCase(), pid = String(m.pid || ''); const x = rooms.get(code);
        if (!x || !pid) { send(ws, { t: 'err', e: 'noroom' }); break; }
        if (x.banned.has(pid)) { send(ws, { t: 'err', e: 'banned' }); break; }
        if (pid === x.hostPid) { send(ws, { t: 'err', e: 'ishost' }); break; }
        const prev = x.players.get(pid); if (prev && prev !== ws) { prev._role = null; try { prev.close(); } catch (e) { } }
        const fresh = !prev; x.players.set(pid, ws); ws._code = code; ws._role = 'player'; ws._pid = pid;
        const off = x.host ? 0 : now() - (x.offAt || x.born);
        send(ws, { t: 'joined', host: !!x.host, canClaim: !x.host && !!x.snap && off >= CLAIM_MS });
        send(x.host, { t: 'p+', pid }); if (fresh) hit('players'); break;
      }
      case 'up': if (r && ws._role === 'player') send(r.host, { t: 'up', pid: ws._pid, d: m.d }); break;
      case 'down':
        if (!r || ws._role !== 'host') break;
        if (m.to) send(r.players.get(m.to), { t: 'down', d: m.d }); else bcast(r, { t: 'down', d: m.d });
        break;
      case 'snap': if (r && ws._role === 'host' && m.s) r.snap = m.s; break;
      case 'ph':
        if (!r || ws._role !== 'host') break;
        if (m.src && String(m.src).length < 1.5e6) r.photos[m.id] = m.src; else delete r.photos[m.id];
        break;
      case 'drop': {
        if (!r || ws._role !== 'host') break;
        const pid = String(m.pid || ''); r.banned.add(pid); const pw = r.players.get(pid);
        r.players.delete(pid); if (pw) { pw._role = null; pw._code = null; } break;
      }
      case 'leave': {
        if (!r || ws._role !== 'player') break;
        r.players.delete(ws._pid); send(r.host, { t: 'up', pid: ws._pid, d: { a: 'bye' } }); ws._role = null; ws._code = null; break;
      }
      case 'claim': {
        if (!r || ws._role !== 'player') break;
        const off = now() - (r.offAt || r.born);
        if (r.host || !r.snap || off < CLAIM_MS) { send(ws, { t: 'err', e: 'noclaim' }); break; }
        promote(r, ws._pid, 'claim'); break;
      }
      case 'handover': if (r && ws._role === 'host') { if (m.s) r.snap = m.s; if (!promote(r, String(m.to || ''), 'handover')) send(ws, { t: 'err', e: 'nohandover' }); } break;
      case 'close': if (r && ws._role === 'host') { bcast(r, { t: 'closed' }); clearTimeout(r.claimTimer); rooms.delete(r.code); } break;
      case 'stat': if (r && ws._role === 'host' && ['started', 'finished'].includes(m.ev)) { hit(m.ev); if (m.n) hit('questions', Math.min(200, +m.n || 0)); } break;
    }
  });
  ws.on('close', () => {
    const r = ws._code ? rooms.get(ws._code) : null; if (!r) return;
    if (ws._role === 'host' && r.host === ws) hostOff(r);
    else if (ws._role === 'player' && r.players.get(ws._pid) === ws) { r.players.delete(ws._pid); send(r.host, { t: 'p-', pid: ws._pid }); }
  });
});

/* نبض كل 25 ثانية حتى ما تنقطع الاتصالات الساكتة، وتنضيف القعدات القديمة */
setInterval(() => {
  wss.clients.forEach(ws => { if (!ws._alive) return ws.terminate(); ws._alive = false; try { ws.ping(); } catch (e) { } });
  const t = now();
  for (const [code, r] of rooms) {
    const empty = !r.host && r.players.size === 0;
    if (t - r.born > ROOM_TTL || (empty && t - r.touched > IDLE_TTL)) { clearTimeout(r.claimTimer); rooms.delete(code); }
  }
}, 25000).unref();

statsInit().then(() => server.listen(PORT, () => console.log('قدّيش؟ شغّال على ' + PORT)));
