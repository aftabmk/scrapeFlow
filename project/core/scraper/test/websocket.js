const { WebSocketServer, WebSocket } = require('ws');

function createWebSocketServer(port = 8080) {
  const wss = new WebSocketServer({ port });

  console.log(`✅ WebSocket Server running on ws://localhost:${port}`);

  const connections = new Map();

  wss.on('connection', (ws, req) => {
    const pageId = (req.url || '/default').split('/').pop() || 'default';

    console.log(`📡 Browser connected: ${pageId}`);

    connections.set(pageId, ws);

    ws.on('message', (message) => {
      try {
        const data = JSON.parse(message);
        console.log(`📥 [${pageId}]`, data);
      } catch {
        console.log(`📥 [${pageId}]`, message.toString());
      }
    });

    ws.on('close', () => {
      console.log(`❌ Browser disconnected: ${pageId}`);
      connections.delete(pageId);
    });

    ws.on('error', (err) => {
      console.error(`❌ WS Error (${pageId}):`, err.message);
      connections.delete(pageId);
    });
  });

  const send = (pageId, data) => {
    const ws = connections.get(pageId);

    if (!ws)
      throw new Error(`No connection found for '${pageId}'`);

    if (ws.readyState !== WebSocket.OPEN)
      throw new Error(`Connection '${pageId}' is not open`);

    ws.send(
      typeof data === 'string'
        ? data
        : JSON.stringify(data)
    );
  };

  const broadcast = (data) => {
    const payload =
      typeof data === 'string'
        ? data
        : JSON.stringify(data);

    for (const ws of connections.values()) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(payload);
      }
    }
  };

  const get = (pageId) => connections.get(pageId);

  const has = (pageId) => {
    const ws = connections.get(pageId);
    return ws && ws.readyState === WebSocket.OPEN;
  };

  const list = () => [...connections.keys()];

  return {
    wss,
    connections,
    send,
    broadcast,
    get,
    has,
    list
  };
}

module.exports = { createWebSocketServer };

if (require.main === module) {
    const wsServer = createWebSocketServer(8080);

    wsServer.broadcast({
      type: 'ping'
    });

    console.log(`all connections : ${wsServer.list()}`);  
}