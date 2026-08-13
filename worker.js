import { connect } from "cloudflare:sockets";

function encode(bytes) {
  let out = "";
  for (let i = 0; i < bytes.length; i++) out += String.fromCharCode(bytes[i]);
  return btoa(out);
}

function randombytes(n) {
  const b = new Uint8Array(n);
  crypto.getRandomValues(b);
  return b;
}

function join(...parts) {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let pos = 0;
  for (const p of parts) {
    out.set(p, pos);
    pos += p.length;
  }
  return out;
}

const enc = new TextEncoder();
const dec = new TextDecoder();

function frame(opcode, data) {
  const len = data.length;
  let head;
  if (len < 126) {
    head = new Uint8Array(6);
    head[1] = 0x80 | len;
  } else if (len < 65536) {
    head = new Uint8Array(8);
    head[1] = 0x80 | 126;
    head[2] = (len >> 8) & 0xff;
    head[3] = len & 0xff;
  } else {
    head = new Uint8Array(14);
    head[1] = 0x80 | 127;
    new DataView(head.buffer).setUint32(10, len, false);
  }
  head[0] = 0x80 | opcode;

  const key = randombytes(4);
  head.set(key, head.length - 4);
  const masked = new Uint8Array(len);
  for (let i = 0; i < len; i++) masked[i] = data[i] ^ key[i % 4];

  return join(head, masked);
}

class reader {
  constructor(onmessage, onclose) {
    this.buf = new Uint8Array(0);
    this.onmessage = onmessage;
    this.onclose = onclose;
    this.parts = [];
    this.partsop = null;
  }

  push(chunk) {
    this.buf = join(this.buf, chunk);
    this.drain();
  }

  drain() {
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0];
      const b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let pos = 2;

      if (len === 126) {
        if (this.buf.length < pos + 2) return;
        len = (this.buf[pos] << 8) | this.buf[pos + 1];
        pos += 2;
      } else if (len === 127) {
        if (this.buf.length < pos + 8) return;
        len = Number(new DataView(this.buf.buffer, this.buf.byteOffset + pos, 8).getBigUint64(0, false));
        pos += 8;
      }

      let key = null;
      if (masked) {
        if (this.buf.length < pos + 4) return;
        key = this.buf.slice(pos, pos + 4);
        pos += 4;
      }

      if (this.buf.length < pos + len) return;

      let payload = this.buf.slice(pos, pos + len);
      if (masked) {
        const unmasked = new Uint8Array(len);
        for (let i = 0; i < len; i++) unmasked[i] = payload[i] ^ key[i % 4];
        payload = unmasked;
      }

      this.buf = this.buf.slice(pos + len);

      if (opcode === 0x8) {
        this.onclose();
        return;
      } else if (opcode === 0x9 || opcode === 0xa) {
        continue;
      } else if (opcode === 0x0) {
        this.parts.push(payload);
        if (fin) {
          this.onmessage(this.partsop, join(...this.parts));
          this.parts = [];
          this.partsop = null;
        }
      } else if (fin) {
        this.onmessage(opcode, payload);
      } else {
        this.parts = [payload];
        this.partsop = opcode;
      }
    }
  }
}

export default {
  async fetch(request, env, ctx) {
    if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
      return new Response("join from an eagler client, not from the browser", { status: 426 });
    }

    const split = env.SERVER.lastIndexOf(":");
    const host = env.SERVER.slice(0, split);
    const port = parseInt(env.SERVER.slice(split + 1), 10);

    const socket = connect({ hostname: host, port });
    const writer = socket.writable.getWriter();
    const backend = socket.readable.getReader();

    const key = encode(randombytes(16));
    const handshake =
      `GET / HTTP/1.1\r\n` +
      `Host: ${host}\r\n` +
      `Upgrade: websocket\r\n` +
      `Connection: Upgrade\r\n` +
      `Sec-WebSocket-Key: ${key}\r\n` +
      `Sec-WebSocket-Version: 13\r\n\r\n`;

    await writer.write(enc.encode(handshake));

    let resp = new Uint8Array(0);
    let headerend = -1;
    while (headerend === -1) {
      const { value, done } = await backend.read();
      if (done) return new Response("backend closed during handshake", { status: 502 });
      resp = join(resp, value);
      headerend = dec.decode(resp).indexOf("\r\n\r\n");
    }

    const header = dec.decode(resp.slice(0, headerend));
    const leftover = resp.slice(headerend + 4);

    if (!header.split("\r\n")[0].includes(" 101")) {
      return new Response(`backend rejected handshake:\n${header}`, { status: 502 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();
    server.binaryType = "arraybuffer";

    const parser = new reader(
      (opcode, payload) => {
        if (opcode === 0x2) server.send(payload);
        else server.send(dec.decode(payload));
      },
      () => server.close(1000, "backend closed")
    );

    if (leftover.length > 0) parser.push(leftover);

    ctx.waitUntil((async () => {
      try {
        for (;;) {
          const { value, done } = await backend.read();
          if (done) break;
          parser.push(value);
        }
      } finally {
        server.close(1000, "backend closed");
      }
    })());

    server.addEventListener("message", (event) => {
      ctx.waitUntil((async () => {
        let opcode, payload;
        if (typeof event.data === "string") {
          opcode = 0x1;
          payload = enc.encode(event.data);
        } else {
          opcode = 0x2;
          payload = new Uint8Array(event.data);
        }
        await writer.write(frame(opcode, payload));
      })());
    });

    server.addEventListener("close", async () => {
      try {
        await writer.write(frame(0x8, new Uint8Array(0)));
        await writer.close();
      } catch (e) {}
    });

    return new Response(null, { status: 101, webSocket: client });
  },
};
