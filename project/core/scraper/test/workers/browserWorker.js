const { scrape } = require('../scraper/browser/browser.js');

process.on('message', async (msg) => {
  if (msg.cmd === 'scrape') {
    try {
      const data = await scrape();
      process.send({ type: 'done', data });
    } catch (err) {
      process.send({ type: 'error', error: err.message });
    }
  }
});