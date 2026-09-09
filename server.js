// NestCall signaling server
// Job: pair two people who type the same call code, then relay their
// WebRTC connection messages to each other. The actual video/audio never
// passes through this server — once connected, the call is peer-to-peer.

const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const path = require('path');

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const rooms = {}; // code -> [ws, ws]

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      const code = String(msg.room || '').trim().toUpperCase();
      if (!code) return;

      if (!rooms[code]) rooms[code] = [];

      if (rooms[code].length >= 2) {
        ws.send(JSON.stringify({ type: 'full' }));
        return;
      }

      rooms[code].push(ws);
      ws.room = code;
      ws.send(JSON.stringify({ type: 'joined', initiator: rooms[code].length === 1 }));

      if (rooms[code].length === 2) {
        rooms[code].forEach(client => client.send(JSON.stringify({ type: 'ready' })));
      }
      return;
    }

    // Relay offer / answer / ice-candidate to the other person in the room
    const code = ws.room;
    if (!code || !rooms[code]) return;
    rooms[code].forEach(client => {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        client.send(raw.toString());
      }
    });
  });

  ws.on('close', () => {
    const code = ws.room;
    if (code && rooms[code]) {
      rooms[code] = rooms[code].filter(c => c !== ws);
      rooms[code].forEach(c => c.send(JSON.stringify({ type: 'peer-left' })));
      if (rooms[code].length === 0) delete rooms[code];
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('NestCall signaling server running on port', PORT));
