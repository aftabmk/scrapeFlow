const chromium = require('@sparticuz/chromium');
const { addExtra } = require('puppeteer-extra');
const puppeteerCore = require('puppeteer-core');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

const puppeteerExtra = addExtra(puppeteerCore);
puppeteerExtra.use(StealthPlugin());

// Resource types that are safe to block for a data-scraping page.
// NOTE: 'script' is intentionally NOT blocked — most exchange sites
// (NSE/BSE) are JS-rendered SPAs and blocking script will break them.
const BLOCKED_RESOURCE_TYPES = new Set([
  'image',
  'stylesheet',
  'font',
  'media',
]);

class BrowserManager {
  static browser = null;
  static launching = null; // in-flight launch promise, guards against concurrent launches

  constructor() {
    throw new Error(
      'BrowserManager is a static class and cannot be instantiated'
    );
  }

  static async launch() {
    // Verify liveness, not just presence of the reference.
    if (this.browser && this.browser.isConnected()) {
      console.log('♻️ Browser reused');
      return this.browser;
    }

    // If a launch is already in progress (e.g. concurrent calls on a cold
    // start), wait on it instead of starting a second Chromium process.
    if (this.launching) {
      return this.launching;
    }

    this.launching = (async () => {
      const browser = await puppeteerExtra.launch({
        args: chromium.args,
        defaultViewport: chromium.defaultViewport,
        executablePath: await chromium.executablePath(),
        headless: chromium.headless,
      });

      browser.on('disconnected', () => {
        console.log('❌ Browser disconnected');
        this.browser = null;
      });

      this.browser = browser;
      console.log('✅ Browser launched with NSE + CSP fixes');
      return browser;
    })();

    try {
      return await this.launching;
    } finally {
      this.launching = null;
    }
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
      if (BLOCKED_RESOURCE_TYPES.has(req.resourceType())) {
        return req.abort();
      }
      req.continue();
    });

    console.log('✅ Page created with strict request interceptor');

    return page;
  }

  static get() {
    return this.browser;
  }

  static has() {
    return !!(this.browser && this.browser.isConnected());
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

    console.log('Browser active:', BrowserManager.has());
  })();
}