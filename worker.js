import { connect } from "cloudflare:sockets";
import { DurableObject } from "cloudflare:workers";

function randomBytes(n) {
  const b = new Uint8Array(n);
  crypto.getRandomValues(b);
  return b;
}

function base64Encode(bytes) {
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function buildFrame(opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) {
    header = new Uint8Array(6);
    header[1] = 0x80 | len;
  } else if (len < 65536) {
    header = new Uint8Array(8);
    header[1] = 0x80 | 126;
    header[2] = (len >> 8) & 0xff;
    header[3] = len & 0xff;
  } else {
    header = new Uint8Array(14);
    header[1] = 0x80 | 127;
    new DataView(header.buffer).setUint32(10, len, false);
  }
  header[0] = 0x80 | opcode;

  const maskKey = randomBytes(4);
  header.set(maskKey, header.length - 4);

  const out = new Uint8Array(header.length + len);
  out.set(header, 0);
  for (let i = 0; i < len; i++) out[header.length + i] = payload[i] ^ maskKey[i % 4];
  return out;
}

class FrameParser {
  constructor(onMessage, onClose) {
    this.queue = []; 
    this.queuedLen = 0;
    this.onMessage = onMessage;
    this.onClose = onClose;
    this.fragments = [];
    this.fragmentOpcode = null;
  }

  push(chunk) {
    if (chunk.length > 0) {
      this.queue.push(chunk);
      this.queuedLen += chunk.length;
    }
    this._parse();
  }

  _peek(n) {
    if (this.queuedLen < n) return null;
    if (this.queue[0].length >= n) return this.queue[0].subarray(0, n);
    const out = new Uint8Array(n);
    let filled = 0;
    for (const c of this.queue) {
      const take = Math.min(c.length, n - filled);
      out.set(c.subarray(0, take), filled);
      filled += take;
      if (filled >= n) break;
    }
    return out;
  }

  _consume(n) {
    let remaining = n;
    while (remaining > 0) {
      const head = this.queue[0];
      if (head.length <= remaining) {
        remaining -= head.length;
        this.queue.shift();
      } else {
        this.queue[0] = head.subarray(remaining);
        remaining = 0;
      }
    }
    this.queuedLen -= n;
  }

  _parse() {
    for (;;) {
      const head = this._peek(2);
      if (!head) return;
      const b0 = head[0];
      const b1 = head[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let headerLen = 2;

      if (len === 126) {
        const ext = this._peek(4);
        if (!ext) return;
        len = (ext[2] << 8) | ext[3];
        headerLen = 4;
      } else if (len === 127) {
        const ext = this._peek(10);
        if (!ext) return;
        len = Number(new DataView(ext.buffer, ext.byteOffset + 2, 8).getBigUint64(0, false));
        headerLen = 10;
      }

      if (masked) headerLen += 4;

      const total = headerLen + len;
      const full = this._peek(total);
      if (!full) return;

      let maskKey = null;
      if (masked) maskKey = full.subarray(headerLen - 4, headerLen);

      let payload = full.subarray(headerLen, headerLen + len);
      if (masked) {
        const unmasked = new Uint8Array(len);
        for (let i = 0; i < len; i++) unmasked[i] = payload[i] ^ maskKey[i % 4];
        payload = unmasked;
      } else {
        payload = payload.slice();
      }

      this._consume(total);

      if (opcode === 0x8) {
        this.onClose(payload);
        return;
      } else if (opcode === 0x9 || opcode === 0xa) {
        continue;
      } else if (opcode === 0x0) {
        this.fragments.push(payload);
        if (fin) {
          const totalLen = this.fragments.reduce((n, c) => n + c.length, 0);
          const merged = new Uint8Array(totalLen);
          let o = 0;
          for (const c of this.fragments) { merged.set(c, o); o += c.length; }
          this.onMessage(this.fragmentOpcode, merged);
          this.fragments = [];
          this.fragmentOpcode = null;
        }
      } else if (fin) {
        this.onMessage(opcode, payload);
      } else {
        this.fragments = [payload];
        this.fragmentOpcode = opcode;
      }
    }
  }
}

class WriteQueue {
  constructor(writer) {
    this.writer = writer;
    this.tail = Promise.resolve();
  }
  push(bytes) {
    this.tail = this.tail.then(() => this.writer.write(bytes)).catch((e) => {
      console.log(`Write to backend failed: ${e.message}`);
    });
    return this.tail;
  }
}

export class EaglerProxy extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.writeQueue = null;
    this.rawWriter = null;
    this.parser = null;
  }

  async fetch(request) {
    const upgrade = request.headers.get("Upgrade");
    if (!upgrade || upgrade.toLowerCase() !== "websocket") {
      return new Response("Connect from an eaglercraft client, not the browser.", { status: 426 });
    }

    const address = this.env.SERVER;
    if (!address || address === "SERVERADDRESS:25565") {
      return new Response(
        "SERVER variable missing. Go to Settings > Variables and set it to your Minecraft server's address",
        { status: 500 }
      );
    }

    const lastColon = address.lastIndexOf(":");
    const host = address.slice(0, lastColon);
    const port = parseInt(address.slice(lastColon + 1), 10);

    let socket;
    try {
      socket = connect({ hostname: host, port });
    } catch (err) {
      return new Response(`Failed to open TCP socket to backend: ${err.message}`, { status: 502 });
    }

    const rawWriter = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    this.rawWriter = rawWriter;
    this.writeQueue = new WriteQueue(rawWriter);

    const secWebSocketKey = base64Encode(randomBytes(16));
    const handshake =
      `GET / HTTP/1.1\r\n` +
      `Host: ${host}\r\n` +
      `Upgrade: websocket\r\n` +
      `Connection: Upgrade\r\n` +
      `Sec-WebSocket-Key: ${secWebSocketKey}\r\n` +
      `Sec-WebSocket-Version: 13\r\n\r\n`;

    try {
      await rawWriter.write(textEncoder.encode(handshake));
    } catch (err) {
      return new Response(`Failed to send handshake: ${err.message}`, { status: 502 });
    }

    let respBuf = new Uint8Array(0);
    let headerEnd = -1;
    while (headerEnd === -1) {
      const { value, done } = await reader.read();
      if (done) return new Response("Backend closed connection during handshake", { status: 502 });
      const merged = new Uint8Array(respBuf.length + value.length);
      merged.set(respBuf, 0);
      merged.set(value, respBuf.length);
      respBuf = merged;

      for (let i = 0; i + 3 < respBuf.length; i++) {
        if (respBuf[i] === 13 && respBuf[i + 1] === 10 && respBuf[i + 2] === 13 && respBuf[i + 3] === 10) {
          headerEnd = i + 4;
          break;
        }
      }
      if (respBuf.length > 32768) return new Response("Backend handshake response too large", { status: 502 });
    }

    const headerStr = textDecoder.decode(respBuf.slice(0, headerEnd));
    const leftover = respBuf.slice(headerEnd);

    if (!headerStr.split("\r\n")[0].includes(" 101")) {
      return new Response(`Backend rejected handshake:\n${headerStr}`, { status: 502 });
    }

    const { 0: client, 1: server } = new WebSocketPair();

    this.ctx.acceptWebSocket(server);

    this.parser = new FrameParser(
      (opcode, payload) => {
        try {
          if (opcode === 0x2) server.send(payload);
          else server.send(textDecoder.decode(payload));
        } catch (e) {}
      },
      () => {
        try { server.close(1000, "backend closed"); } catch (e) {}
      }
    );

    if (leftover.length > 0) this.parser.push(leftover);

    this.ctx.waitUntil((async () => {
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          this.parser.push(value);
        }
      } catch (e) {
        console.log(`Backend read loop error: ${e.message}`);
      } finally {
        try { server.close(1000, "backend closed"); } catch (e) {}
      }
    })());

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    let payload, opcode;
    if (typeof message === "string") {
      opcode = 0x1;
      payload = textEncoder.encode(message);
    } else {
      opcode = 0x2;
      payload = new Uint8Array(message);
    }
    this.writeQueue.push(buildFrame(opcode, payload));
  }

  async webSocketClose(ws, code, reason) {
    try {
      await this.writeQueue.push(buildFrame(0x8, new Uint8Array(0)));
      await this.rawWriter.close();
    } catch (e) {}
  }

  async webSocketError(ws, error) {
    try { await this.rawWriter.close(); } catch (e) {}
  }
}

export default {
  async fetch(request, env) {

    const id = env.EAGLER_PROXY.newUniqueId();
    const stub = env.EAGLER_PROXY.get(id);
    return stub.fetch(request);
  },
};
