const puppeteerExtra = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteerExtra.use(StealthPlugin());

class BrowserManager {
  static browser = null;

  constructor() {
    throw new Error(
      'BrowserManager is a static class and cannot be instantiated'
    );
  }

  static async launch() {
    if (this.browser) {
      console.log('♻️ Browser reused');
      return this.browser;
    }

    this.browser = await puppeteerExtra.launch({
      headless: false,
      devtools: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-web-security',
        // '--allow-insecure-localhost',
        // '--remote-allow-origins=*',
        // '--disable-features=IsolateOrigins,site-per-process,LocalNetworkAccess,ContentSecurityPolicy',
        // '--disable-site-isolation-trials',
        // '--ignore-certificate-errors',
        // '--disable-blink-features=AutomationControlled',
        // '--disable-http2',
        // '--disable-quic',
      ],
      protocolTimeout: 30_000,
    });

    console.log('✅ Browser launched with NSE + CSP fixes');

    this.browser.on('disconnected', () => {
      console.log('❌ Browser disconnected');
      this.browser = null;
    });

    return this.browser;
  }

  static async createPage() {
    const browser = await this.launch();

    const page = await browser.newPage();

    await page.setBypassCSP(true);

    await page.setViewport({
      width: 1366,
      height: 768,
    });

    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    );

    await page.setRequestInterception(true);

    page.on('request', (req) => {
      const type = req.resourceType();

      if (
        type === 'document' ||
        type === 'fetch' ||
        type === 'xhr'
      ) {
        return req.continue();
      }

      if (
        [
          'image',
          'stylesheet',
          'font',
          'media',
          'script',
          'other',
        ].includes(type)
      ) {
        return req.abort();
      }

      req.continue();
    });

    console.log(
      '✅ Page created with strict request interceptor'
    );

    return page;
  }

  static get() {
    return this.browser;
  }

  static has() {
    return !!this.browser;
  }

  static async close() {
    if (!this.browser) return;

    await this.browser.close();
    this.browser = null;

    console.log('🛑 Browser closed');
  }
}

module.exports = BrowserManager;

// Direct run
if (require.main === module) {
  (async () => {
    await BrowserManager.launch();

    const page = await BrowserManager.createPage();

    console.log(
      'Browser active:',
      BrowserManager.has()
    );
  })();
}