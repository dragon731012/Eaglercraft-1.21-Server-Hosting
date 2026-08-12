export default {
  async fetch(request, env) {
    const upgrade = request.headers.get("Upgrade");
  
    if (upgrade != "websocket") {
      return new Response(
        "Connect from an eaglercraft client, not the browser.",
        { 
          status: 426
        }
      );
    }
  
    const address = env.SERVER;
  
    if (!address || address === "SERVERADDRESS:25565") {
      return new Response(
        "SERVER variable missing. Go to Settings > Variables and set it to your Minecraft server's address",
        { 
          status: 500
        }
      );
    }
  
    const full = "ws://"+address;
  
    let response;
    try {
      response = await fetch(full, {
        headers: request.headers 
      });
    } catch (e) {
      return new Response("Error: "+e, {
        status: 502
      });
    }
  
    const ws = response.webSocket;
    if (!ws) {
      return new Response("backend didnt accept websocket", {
        status: 502
      });
    }
  
    const [client, server] = Object.values(new WebSocketPair());
  
    server.accept();
    ws.accept();
  
    server.addEventListener("message", (event) => {
      try {
        ws.send(event.data);
      } catch (err) {
        server.close(1011, "Backend send failed");
      }
    });
  
    ws.addEventListener("message", (event) => {
      try {
        server.send(event.data);
      } catch (err) {
        ws.close(1011, "Client send failed");
      }
    });
  
    server.addEventListener("close", (event) => {
      try {
        ws.close(event.code, event.reason);
      } catch (err) {}
    });
  
    ws.addEventListener("close", (event) => {
      try {
        server.close(event.code, event.reason);
      } catch (err) {}
    });
  
    server.addEventListener("error", () => {
      try {
        ws.close();
      } catch (err) {}
    });
  
    ws.addEventListener("error", () => {
      try {
        server.close();
      } catch (err) {}
    });
  
    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};
