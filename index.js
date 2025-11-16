const WebSocket = require('ws');
const { encode, decode } = require('@msgpack/msgpack');
const os = require('os');

const PORT = process.env.PORT || 10000;
const isSecure = !!process.env.WSS;
const protocol = isSecure ? "wss" : "ws";
const ip = getLocalIp();
const MAX_PLAYERS_PER_ROOM = 50;
const HEARTBEAT_INTERVAL = 30 * 1000;

const HDR_CONTROL = 0x01;
const HDR_TURNCHUNK_PART = 0x02;

const BUFFERED_AMOUNT_LIMIT = 256 * 1024;
const EMPTY_ROOM_CHECK_INTERVAL = 600 * 1000;

const ALLOWED_RE = /^[\p{L}\p{N}._-]+$/u;

const rooms = new Map();
let cachedLobbySnapshot = ['RO', []];

const wss = new WebSocket.Server({
  port: PORT,
  perMessageDeflate: false,
}, () => {
  console.log(`Tracker URL: ${protocol}://${ip}:${PORT}`);
});

// ---- helpers ----
function validateName(name, type = "Name") {
  if (typeof name !== 'string') return 'E00';
  const trimmed = name.trim();
  if (trimmed.length === 0) return 'E01';

  if (type === 'Nickname') {
    if (trimmed.length < 4) return 'E02';
    if (trimmed.length > 25) return 'E03';
  } else {
    if (trimmed.length > 24) return 'E07';
  }
  if (!ALLOWED_RE.test(trimmed)) return 'E04';
  return null;
}

function validatePassword(pass) {
  if (!pass) return null;
  if (typeof pass !== 'string') return 'E08';
  if (pass.length > 24) return 'E09';
  if (!ALLOWED_RE.test(pass)) return 'E10';
  return null;
}

function validateScenario(s) {
  if (typeof s !== 'string') return 'E23';
  const trimmed = s.trim();
  if (trimmed.length === 0) return 'E24';
  if (trimmed.length > 64) return 'E25';
  if (!ALLOWED_RE.test(trimmed)) return 'E26';
  return null;
}

// ---- rooms snapshot ----
function updateRoomSnapshot() {
  const allRooms = [];
  for (const [roomName, room] of rooms) {
    const hasPass = room.password ? 1 : 0;
    const playersCount = room.players ? room.players.size : 0;
    const scenario = room.scenarioName || "Game not started";
    allRooms.push([roomName, room.host, hasPass, playersCount, scenario]);
  }
  cachedLobbySnapshot = ['RO', allRooms];
}

// ---- player leave ----
function handleLeave(ws, state = {}) {
  const roomName = state.room;
  const nick = state.player;
  if (!roomName || !nick) return;

  const room = rooms.get(roomName);
  if (!room) return;

  const wasHost = room.host === nick;
  if (room.players.delete(nick)) {
    broadcast(roomName, ['l', nick]);

    if (room.players.size === 0) {
      rooms.delete(roomName);
      updateRoomSnapshot();
      return;
    }

    if (wasHost) {
      const nextHost = room.players.keys().next().value;
      if (nextHost) {
        room.host = nextHost;
        const nextWs = room.players.get(nextHost);
        if (nextWs && nextWs.readyState === WebSocket.OPEN) {
          safeSend(nextWs, ['host']);
        }
      }
    }

    updateRoomSnapshot();
  }
}

// ---- handlers ----
const handlers = {
  hr: (ws, state, [nick, roomName, pass]) => {
    let err = validateName(nick, "Nickname");
    if (err) return safeSend(ws, ['e', err]);

    err = validateName(roomName, "Room name");
    if (err) return safeSend(ws, ['e', err]);

    err = validatePassword(pass);
    if (err) return safeSend(ws, ['e', err]);

    if (rooms.has(roomName)) return safeSend(ws, ['e', 'E11']);

    pass = pass || "";
    const players = new Map([[nick, ws]]);
    rooms.set(roomName, {
      players,
      password: pass,
      host: nick,
      scenarioName: "Game not started"
    });

    state.room = roomName;
    state.player = nick;
    ws.state = state;

    updateRoomSnapshot();
    safeSend(ws, ['roomhosted']);
  },

  j: (ws, state, [nick, roomName, pass]) => {
    let err = validateName(nick, "Nickname");
    if (err) return safeSend(ws, ['e', err]);

    err = validateName(roomName, "Room name");
    if (err) return safeSend(ws, ['e', err]);

    err = validatePassword(pass);
    if (err) return safeSend(ws, ['e', err]);

    const room = rooms.get(roomName);
    if (!room) return safeSend(ws, ['e', 'E12']);

    pass = pass || "";
    if (room.password !== pass) return safeSend(ws, ['e', 'E13']);
    if (room.players.has(nick)) return safeSend(ws, ['e', 'E14']);
    if (room.players.size >= MAX_PLAYERS_PER_ROOM) return safeSend(ws, ['e', 'E15']);

    room.players.set(nick, ws);
    state.room = roomName;
    state.player = nick;
    ws.state = state;

    updateRoomSnapshot();
    broadcast(roomName, ['j', roomName, nick]);
  },

  l: (ws, state, [nick]) => {
    handleLeave(ws, state);
  },

  cmd: (ws, state, rest) => {
    const room = rooms.get(state.room);
    if (!room) return;
    safeSend(room.players.get(room.host), ['cmd', ...rest]);
  },

  getrooms: (ws) => {
    safeSend(ws, cachedLobbySnapshot);
  },

  setscenario: (ws, state, [scenarioName]) => {
    const roomName = state.room;
    if (!roomName) return safeSend(ws, ['e', 'E16']);
    const room = rooms.get(roomName);
    if (!room) return safeSend(ws, ['e', 'E17']);
    if (room.host !== state.player) return safeSend(ws, ['e', 'E27']);

    if (typeof scenarioName !== 'string') {
      room.scenarioName = 'Unknown';
    } else {
      const trimmed = scenarioName.trim();
      const err = validateScenario(trimmed);
      room.scenarioName = err ? 'Unknown' : trimmed;
    }

    updateRoomSnapshot();
  },

  k: (ws, state, [targetNick]) => {
    const roomName = state.room;
    if(!roomName) return;
    const room = rooms.get(roomName);
    if (!room) return;
    if (room.host === state.player && state.player !== targetNick && room.players.has(targetNick)) {
      const targetWs = room.players.get(targetNick);
      broadcast(roomName, ['k', targetNick], state.player);
      room.players.delete(targetNick);
      targetWs.state.room = null;
      targetWs.state.player = null;
      updateRoomSnapshot();
    }
  }
};

// ---- sending ----
function safeSend(ws, arr) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  if (!Array.isArray(arr)) return;
  if (ws.bufferedAmount > BUFFERED_AMOUNT_LIMIT) return;
  try {
    const payload = Buffer.from(encode(arr));
    const framed = Buffer.concat([Buffer.from([HDR_CONTROL]), payload]);
    ws.send(framed, { binary: true });
  } catch (err) {}
}

function broadcast(roomName, arr) {
  if (!Array.isArray(arr)) return;
  const room = rooms.get(roomName);
  if (!room) return;
  const payload = Buffer.from(encode(arr));
  const framed = Buffer.concat([Buffer.from([HDR_CONTROL]), payload]);
  for (const client of room.players.values()) {
    if (client.readyState === WebSocket.OPEN) {
      if (client.bufferedAmount > BUFFERED_AMOUNT_LIMIT) continue;
      client.send(framed, { binary: true });
    }
  }
}

// ---- connection ----
wss.on('connection', ws => {
  const state = { room: null, player: null, countryID: null };
  ws.state = state;

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (buf.length < 1) return;
    const hdr = buf[0];

    if (hdr === HDR_CONTROL) {
      try {
        const decoded = decode(buf.slice(1));
        if (Array.isArray(decoded) && decoded.length >= 1 && typeof decoded[0] === 'string') {
          const [type, ...rest] = decoded;
          const handler = handlers[type];
          if (handler) {
            try { handler(ws, state, rest); } catch (hErr) {}
            return;
          }
          if (state.room && rooms.has(state.room)) {
            broadcast(state.room, decoded);
            return;
          }
        }
      } catch (err) {}
    } else if (hdr === HDR_TURNCHUNK_PART) {
      const room = rooms.get(state.room);
      if (!room) return;
      for (const client of room.players.values()) {
        if (client.readyState === WebSocket.OPEN) {
          if (client.bufferedAmount > BUFFERED_AMOUNT_LIMIT) continue;
          client.send(buf, { binary: true });
        }
      }
    }
  });

  ws.on('close', () => handleLeave(ws, state));
  ws.on('error', err => {
    handleLeave(ws, state);
  });
});

// ---- heartbeat ---
setInterval(() => {
    wss.clients.forEach(ws => {
        if (!ws.isAlive) {
          handleLeave(ws, ws.state || {});
          return ws.terminate(); 
        }

        ws.isAlive = false; 
        ws.ping();         
    });
}, HEARTBEAT_INTERVAL);

// ---- cleanup ---
setInterval(() => {
  try {
    let changed = false;
    for (const [roomName, room] of Array.from(rooms)) {
      let hasOpen = false;
      for (const client of room.players.values()) {
        if (client && client.readyState === WebSocket.OPEN) { hasOpen = true; break; }
      }
      if (!hasOpen) {
        rooms.delete(roomName);
        changed = true;
      }
    }
    if (changed) updateRoomSnapshot();
  } catch (err) {}
}, EMPTY_ROOM_CHECK_INTERVAL);

wss.on('close', () => clearInterval(interval));

updateRoomSnapshot();

function getLocalIp() {
  const nets = os.networkInterfaces();

  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      const family = net.family || (net.address.includes(':') ? 'IPv6' : 'IPv4');
      if (family === 'IPv4' && !net.internal) {
        return net.address;
      }
    }
  }
  return '127.0.0.1';
}
