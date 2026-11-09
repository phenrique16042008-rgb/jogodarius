// Servidor da corrida online: serve o index.html e retransmite a "presença" dos jogadores via WebSocket.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => {
  fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('erro'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});
const wss = new WebSocketServer({ server, maxPayload: 8192 });
const rooms = new Map(); // nome da sala -> Map(id -> cliente)
let nextId = 1;

function broadcast(room) { // envia a lista de jogadores e seus estados para todos da sala
  const peers = [...room.values()].map(c => ({ id: c.id, presence: c.presence }));
  const msg = JSON.stringify({ t: 'peers', peers });
  for (const c of room.values()) if (c.ws.readyState === 1) c.ws.send(msg);
}

wss.on('connection', ws => {
  const c = { id: 'p' + nextId++, presence: Object.create(null), ws, room: null };
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'join' && !c.room && typeof m.room === 'string' && /^[a-z0-9][a-z0-9_.-]{0,47}$/.test(m.room)) {
      let r = rooms.get(m.room); if (!r) { r = new Map(); rooms.set(m.room, r); }
      if (r.size >= 2) { ws.send(JSON.stringify({ t: 'full' })); return ws.close(); } // 2 jogadores por sala
      c.room = m.room; r.set(c.id, c);
      ws.send(JSON.stringify({ t: 'welcome', id: c.id })); broadcast(r);
    } else if (m.t === 'p' && c.room && m.d && typeof m.d === 'object') {
      for (const [k, v] of Object.entries(m.d)) { if (v === null) delete c.presence[k]; else c.presence[k] = v; }
      broadcast(rooms.get(c.room));
    }
  });
  ws.on('close', () => {
    const r = rooms.get(c.room); if (!r) return;
    r.delete(c.id); if (r.size) broadcast(r); else rooms.delete(c.room);
  });
});
server.listen(process.env.PORT || 3000, () => console.log('NBA Bounce rodando'));
