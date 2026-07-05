const { scrape } = require('../scraper/browser/browser.js');

const worker = async () => {
  process.on('message', async (msg) => {
    if (!msg || typeof msg.cmd !== 'string') return;

    switch (msg.cmd) {
      case 'scrape':
        try {
          const data = await scrape();
          process.send({ type: 'done', data });
        } catch (err) {
          const message = err.message || 'Unknown error';
          console.error('Scrape error:', message);

          const isFatal =
            /disconnected|target closed|session closed|protocol error|connection closed/i.test(message);

          if (isFatal) {
            process.send({ type: 'abort', error: message });
          } else {
            process.send({ type: 'error', error: message });
          }
        }
        break;
    }
  });

  process.on('uncaughtException', (err) => {
    process.send({ type: 'abort', error: err.message || 'Uncaught exception' });
  });

  process.on('unhandledRejection', (reason) => {
    const error = reason instanceof Error ? reason.message : String(reason);
    process.send({ type: 'abort', error });
  });
};

worker();