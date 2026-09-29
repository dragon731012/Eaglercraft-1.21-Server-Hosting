import { DurableObject } from "cloudflare:workers";

const MAX_PLAYERS_PER_POOL = 18;
const MAX_POOL_INDEX = 20;
const IDLE_TIMEOUT_MS = 10 * 60 * 1000;
const IDLE_CHECK_INTERVAL_MS = 60 * 1000;

export class EaglerProxy extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.sessions = new Map();
    this._idleInterval = null;
  }

  async playerCount() {
    return this.sessions.size;
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

    let backendResp;
    try {
      backendResp = await fetch(`http://${address}/`, {
        headers: {
          Upgrade: "websocket",
          Connection: "Upgrade",
        },
      });
    } catch (err) {
      return new Response(`Failed to connect to backend: ${err.message}`, { status: 502 });
    }

    const backendSocket = backendResp.webSocket;
    if (!backendSocket) {
      return new Response(
        `Backend did not upgrade to WebSocket (status ${backendResp.status})`,
        { status: 502 }
      );
    }

    backendSocket.accept();
    backendSocket.binaryType = "arraybuffer";

    const { 0: client, 1: server } = new WebSocketPair();
    this.ctx.acceptWebSocket(server);

    this.sessions.set(server, { backendSocket, lastActivity: Date.now() });
    this._scheduleIdleCheck();

    backendSocket.addEventListener("message", (event) => {
      try {
        server.send(event.data);
      } catch (e) {}
    });

    backendSocket.addEventListener("close", (event) => {
      try {
        server.close(event.code, event.reason);
      } catch (e) {}
      this.sessions.delete(server);
    });

    backendSocket.addEventListener("error", () => {
      try {
        server.close(1011, "backend error");
      } catch (e) {}
      this.sessions.delete(server);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const session = this.sessions.get(ws);
    if (!session) return;
    session.lastActivity = Date.now();
    try {
      session.backendSocket.send(message);
    } catch (e) {}
  }

  async webSocketClose(ws, code, reason) {
    const session = this.sessions.get(ws);
    if (session) {
      try {
        session.backendSocket.close(code, reason);
      } catch (e) {}
      this.sessions.delete(ws);
    }
  }

  async webSocketError(ws, error) {
    const session = this.sessions.get(ws);
    if (session) {
      try {
        session.backendSocket.close(1011, "client error");
      } catch (e) {}
      this.sessions.delete(ws);
    }
  }

  _scheduleIdleCheck() {
    if (this._idleInterval) return;
    this._idleInterval = setInterval(() => {
      const now = Date.now();
      for (const [ws, session] of this.sessions) {
        if (now - session.lastActivity > IDLE_TIMEOUT_MS) {
          try {
            ws.close(4000, "idle timeout");
          } catch (e) {}
          try {
            session.backendSocket.close();
          } catch (e) {}
          this.sessions.delete(ws);
        }
      }
      if (this.sessions.size === 0) {
        clearInterval(this._idleInterval);
        this._idleInterval = null;
      }
    }, IDLE_CHECK_INTERVAL_MS);
  }
}

export default {
  async fetch(request, env) {
    const upgrade = request.headers.get("Upgrade");
    if (!upgrade || upgrade.toLowerCase() !== "websocket") {
      return new Response("Connect from an eaglercraft client, not the browser.", { status: 426 });
    }

    for (let i = 0; i < MAX_POOL_INDEX; i++) {
      const id = env.EAGLER_PROXY.idFromName(`eagler-${i}`);
      const stub = env.EAGLER_PROXY.get(id);
      let count = 0;
      try {
        count = await stub.playerCount();
      } catch (e) {}
      if (count < MAX_PLAYERS_PER_POOL) {
        return stub.fetch(request);
      }
    }

    return new Response("Server is full, try again later.", { status: 503 });
  },
};
