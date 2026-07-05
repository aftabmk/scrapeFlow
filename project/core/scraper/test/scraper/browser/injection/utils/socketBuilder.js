class SocketBuilder {
  create = (port = 8080, id = 'default') => {
    const url = `ws://localhost:${port}/${id}`
    const socket = new WebSocket(url);

    socket.addEventListener('open', () => {
      console.log(`✅ Connected: ${id}`);
    });

    socket.addEventListener('message', (event) => {
      console.log('📩 Message:', event.data);
    });

    socket.addEventListener('error', (error) => {
      console.error('❌ Socket error:', error);
    });

    socket.addEventListener('close', (event) => {
      console.log(`🔒 Closed: ${event.code} ${event.reason}`);
    });

    window.socket = socket;
    return socket;
  };
}

module.exports = SocketBuilder;