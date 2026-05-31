const BrowserEnvironment = require('./browserEnvironment');

class LambdaEnvironment extends BrowserEnvironment {
  async launch() {
    const chromium = require('@sparticuz/chromium');
    const puppeteerExtra = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');

    puppeteerExtra.use(StealthPlugin());

    return puppeteerExtra.launch({
      args: chromium.args,
      defaultViewport: chromium.defaultViewport,
      executablePath: await chromium.executablePath(),
      headless: chromium.headless,
    });
  }
}

module.exports = LambdaEnvironment;