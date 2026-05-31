class LocalEnvironment {
  async launch() {
    const puppeteerExtra = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');

    puppeteerExtra.use(StealthPlugin());

    return puppeteerExtra.launch({
      headless: false,
      devtools: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });
  }
}

module.exports = LocalEnvironment;