const { WebSocketServer } = require ('ws');

function createWebSocketServer(port = 8080) {
  const wss = new WebSocketServer({ port });

  console.log(`✅ WebSocket Server running on ws://localhost:${port}`);

  const connections = new Map();

  wss.on('connection', (ws, req) => {
    const url = req.url || '/default';
    console.log(`📡 Browser connected: ${url}`);

    const pageId = url.split('/').pop() || 'default';
    connections.set(pageId, ws);

    ws.on('message', (message) => {
      try {
        const data = JSON.parse(message);
        console.log(`\n📥 [WS] Received from browser (${pageId}):`);
        console.log(JSON.stringify(data));
      } catch (e) {
        console.log('📥 [WS] Raw message:', message.toString());
      }
    });

    ws.on('close', () => {
      console.log(`❌ Browser disconnected: ${pageId}`);
      connections.delete(pageId);
    });
  });

  return { wss, connections };
}


module.exports = { createWebSocketServer };