const WSManager = require('../websocket');

process.on('message', (msg) => {
  if (msg.cmd === 'start') {
    try {
      WSManager.start(msg.port || 8080);
      process.send({ type: 'ready' });
    } catch (err) {
      process.send({ type: 'error', error: err.message });
    }
  }

  if (msg.cmd === 'stop') {
    WSManager.stop();
    process.exit(0);
  }
});