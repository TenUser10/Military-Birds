const http = require("http");
const crypto = require("crypto");

const port = process.env.PORT || 8080;
const roomMap = new Map();

function getRoom(code) {
  if (!roomMap.has(code)) {
    roomMap.set(code, new Map());
  }
  return roomMap.get(code);
}

function sendFrame(sock, msgStr) {
  if (!sock || sock.destroyed) return;
  const body = Buffer.from(msgStr, "utf8");
  const len = body.length;
  let head;

  if (len < 126) {
    head = Buffer.alloc(2);
    head[0] = 0x81;
    head[1] = len;
  } else if (len < 65536) {
    head = Buffer.alloc(4);
    head[0] = 0x81;
    head[1] = 126;
    head.writeUInt16BE(len, 2);
  } else {
    head = Buffer.alloc(10);
    head[0] = 0x81;
    head[1] = 127;
    head.writeBigUInt64BE(BigInt(len), 2);
  }

  try {
    sock.write(Buffer.concat([head, body]));
  } catch (_) {}
}

function broadcast(code, data, skipId = null) {
  const room = roomMap.get(code);
  if (!room) return;
  const raw = JSON.stringify(data);
  for (const [id, peer] of room.entries()) {
    if (id !== skipId) {
      sendFrame(peer.sock, raw);
    }
  }
}

function dropPeer(sock) {
  if (!sock.peerInfo) return;
  const { id, code } = sock.peerInfo;
  const room = roomMap.get(code);
  if (room && room.has(id)) {
    room.delete(id);
    broadcast(code, { type: "leave", senderId: id }, id);
    if (room.size === 0) roomMap.delete(code);
  }
  sock.peerInfo = null;
}

function parseFrames(buf) {
  const out = [];
  let i = 0;

  while (i + 2 <= buf.length) {
    const b0 = buf[i];
    const b1 = buf[i + 1];
    const op = b0 & 0x0f;
    const isMasked = (b1 & 0x80) !== 0;
    let size = b1 & 0x7f;
    let p = i + 2;

    if (size === 126) {
      if (p + 2 > buf.length) break;
      size = buf.readUInt16BE(p);
      p += 2;
    } else if (size === 127) {
      if (p + 8 > buf.length) break;
      size = Number(buf.readBigUInt64BE(p));
      p += 8;
    }

    const maskBytes = isMasked ? 4 : 0;
    if (p + maskBytes + size > buf.length) break;

    let mask = null;
    if (isMasked) {
      mask = buf.subarray(p, p + 4);
      p += 4;
    }

    const payload = Buffer.alloc(size);
    for (let k = 0; k < size; k++) {
      payload[k] = isMasked ? buf[p + k] ^ mask[k % 4] : buf[p + k];
    }

    out.push({ op, payload });
    i = p + size;
  }

  return { frames: out, left: buf.subarray(i) };
}

function onPacket(sock, rawText) {
  let pkt;
  try {
    pkt = JSON.parse(rawText);
  } catch (_) {
    return;
  }
  if (!pkt || typeof pkt.type !== "string") return;

  if (pkt.type === "join") {
    const id = String(pkt.senderId || "").slice(0, 40);
    const code =
      String(pkt.roomCode || "PVP-1")
        .toUpperCase()
        .replace(/[^A-Z0-9_-]/g, "")
        .slice(0, 14) || "PVP-1";
    if (!id) return;

    if (sock.peerInfo && sock.peerInfo.code !== code) {
      dropPeer(sock);
    }

    sock.peerInfo = { id, code };
    getRoom(code).set(id, { id, sock });
    broadcast(code, { type: "ping", senderId: id }, id);
    return;
  }

  if (!sock.peerInfo) return;
  const { id, code } = sock.peerInfo;
  pkt.senderId = id;

  if (pkt.type === "hit_player" && pkt.targetId) {
    const room = roomMap.get(code);
    const target = room && room.get(String(pkt.targetId));
    if (target) sendFrame(target.sock, JSON.stringify(pkt));
    return;
  }

  if (["state", "bullet", "missile", "ping", "leave"].includes(pkt.type)) {
    broadcast(code, pkt, id);
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(426, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("WS only");
});

server.on("upgrade", (req, sock) => {
  const wsKey = req.headers["sec-websocket-key"];
  if (!wsKey) {
    sock.destroy();
    return;
  }

  const accept = crypto
    .createHash("sha1")
    .update(wsKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
    .digest("base64");

  sock.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );

  sock.peerInfo = null;
  let chunkBuf = Buffer.alloc(0);

  sock.on("data", (buf) => {
    chunkBuf = Buffer.concat([chunkBuf, buf]);
    const { frames, left } = parseFrames(chunkBuf);
    chunkBuf = left;

    for (const f of frames) {
      if (f.op === 0x8) {
        dropPeer(sock);
        sock.end();
        return;
      }
      if (f.op === 0x9) {
        sock.write(Buffer.concat([Buffer.from([0x8a, f.payload.length]), f.payload]));
      } else if (f.op === 0x1) {
        onPacket(sock, f.payload.toString("utf8"));
      }
    }
  });

  sock.on("close", () => dropPeer(sock));
  sock.on("error", () => dropPeer(sock));
});

server.listen(port, () => {
  console.log(`WS server chạy ở cổng ${port}`);
});