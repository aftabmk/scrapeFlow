const WSManager = require('../websocket');

const worker = async () => {
  process.on('message', (msg) => {
    switch (msg.cmd) {
      case 'start':
        try {
          WSManager.start(msg.port || 8080);
          process.send({ type: 'ready' });
        } catch (err) {
          process.send({ type: 'error', error: err.message });
        }
        break;

      case 'stop':
        WSManager.stop();
        process.exit(0);
    }
  });

  // Example: if the underlying WS server emits a fatal runtime error later
  // (after 'ready' was already sent), signal abort instead of just dying silently.
  WSManager.on?.('fatal', (err) => {
    process.send({ type: 'abort', error: err.message });
  });
};

worker();