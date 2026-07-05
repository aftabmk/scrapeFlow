const { WebSocketServer, WebSocket } = require('ws');

class WSManager {
  static wss = null;
  static connections = new Map();

  constructor() {
    throw new Error('WSManager is a static class and cannot be instantiated');
  }

  static start(port = 8080) {
    if (this.wss) {
      console.log(`⚠️ WebSocket Server already running`);
      return this.wss;
    }

    this.wss = new WebSocketServer({ port });

    console.log(`✅ WebSocket Server running on ws://localhost:${port}`);

    this.wss.on('connection', (ws, req) => {
      const pageId =
        (req.url || '/default').split('/').pop() || 'default';

      console.log(`📡 Browser connected: ${pageId}`);

      this.connections.set(pageId, ws);

      ws.on('message', (message) => {
        try {
          const { type, endpoint , data ,timestamp, pageUrl, pageId } = JSON.parse(message);
          console.log(`📥 [${pageId}] :`, { meta : {type, timestamp} , page : {pageId, pageUrl}});
        } catch {
          console.log(`📥 [${pageId}]`, message.toString());
        }
      });

      ws.on('close', () => {
        console.log(`❌ Browser disconnected: ${pageId}`);
        this.connections.delete(pageId);
      });

      ws.on('error', (err) => {
        console.error(`❌ WS Error (${pageId}):`, err.message);
        this.connections.delete(pageId);
      });
    });

    return this.wss;
  }

  static send(pageId, data) {
    const ws = this.connections.get(pageId);

    if (!ws)
      throw new Error(`No connection found for '${pageId}'`);

    if (ws.readyState !== WebSocket.OPEN)
      throw new Error(`Connection '${pageId}' is not open`);

    ws.send(
      typeof data === 'string'
        ? data
        : JSON.stringify(data)
    );
  }

  static broadcast(data) {
    const payload =
      typeof data === 'string'
        ? data
        : JSON.stringify(data);

    for (const ws of this.connections.values()) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(payload);
      }
    }
  }

  static get(pageId) {
    return this.connections.get(pageId);
  }

  static has(pageId) {
    const ws = this.connections.get(pageId);
    return !!ws && ws.readyState === WebSocket.OPEN;
  }

  static list() {
    return [...this.connections.keys()];
  }

  static stop() {
    if (!this.wss) return;

    for (const ws of this.connections.values()) {
      ws.close();
    }

    this.connections.clear();
    this.wss.close();
    this.wss = null;

    console.log('🛑 WebSocket Server stopped');
  }
}

module.exports = WSManager;

// Direct run
if (require.main === module) {
  WSManager.start(8080);

  WSManager.broadcast({
    type: 'ping',
  });

  console.log('all connections:', WSManager.list());
}