const StealthPlugin = require('puppeteer-extra-plugin-stealth');

function detectIsLambda() {
  const override = process.env['BROWSER_RUNTIME'];
  if (override === 'lambda') return true;
  if (override === 'local') return false;

  return !!(
    process.env['AWS_LAMBDA_FUNCTION_NAME'] ||
    process.env['LAMBDA_TASK_ROOT'] ||
    process.env['AWS_EXECUTION_ENV'] ||
    process.env['AWS_LAMBDA_RUNTIME_API']
  );
}

const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36';

const BLOCKED_RESOURCE_TYPES_LOCAL = new Set([
  'image',
  'stylesheet',
  'font',
  'media',
  'script',
  'other',
]);

const BLOCKED_RESOURCE_TYPES_LAMBDA = new Set([
  'image',
  'stylesheet',
  'font',
  'media',
]);

function buildLambdaExtra() {
  try {
    const chromium = require('@sparticuz/chromium');
    const puppeteerCore = require('puppeteer-core');
    const { addExtra } = require('puppeteer-extra');

    const puppeteerExtra = addExtra(puppeteerCore);

    const resolveLaunchOptions = async () => ({
      args: chromium.args,
      defaultViewport: chromium.defaultViewport,
      executablePath: await chromium.executablePath(),
      headless: chromium.headless,
    });

    return { puppeteerExtra, resolveLaunchOptions };
  } catch (err) {
    throw new Error(
      `BrowserManager selected LAMBDA mode but failed to load its dependencies ` +
        `(@sparticuz/chromium / puppeteer-core / puppeteer-extra). Make sure these ` +
        `are installed and bundled into the deployment package. Original error: ${err.message}`
    );
  }
}

function buildLocalExtra() {
  try {
    const puppeteerExtra = require('puppeteer-extra');

    const resolveLaunchOptions = async () => ({
      headless: false,
      devtools: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-web-security',
      ],
      protocolTimeout: 30_000,
    });

    return { puppeteerExtra, resolveLaunchOptions };
  } catch (err) {
    throw new Error(
      `BrowserManager selected LOCAL mode but failed to load 'puppeteer-extra' ` +
        `(and its 'puppeteer' dependency). If this is actually running in Lambda, ` +
        `set process.env.BROWSER_RUNTIME = 'lambda' explicitly. Original error: ${err.message}`
    );
  }
}

let cached = null; // { isLambda, puppeteerExtra, resolveLaunchOptions } — resolved once, lazily

function resolveEnvironment() {
  if (cached) return cached;

  const isLambda = detectIsLambda();
  const { puppeteerExtra, resolveLaunchOptions } = isLambda
    ? buildLambdaExtra()
    : buildLocalExtra();

  puppeteerExtra.use(StealthPlugin());

  console.log(`🔎 BrowserManager mode: ${isLambda ? 'lambda' : 'local'}`);

  cached = { isLambda, puppeteerExtra, resolveLaunchOptions };
  return cached;
}

class BrowserManager {
  static browser = null;
  static launching = null; // in-flight launch promise, guards concurrent launches

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
      const { puppeteerExtra, resolveLaunchOptions, isLambda } = resolveEnvironment();

      const options = await resolveLaunchOptions();
      const browser = await puppeteerExtra.launch(options);

      browser.on('disconnected', () => {
        console.log('❌ Browser disconnected');
        this.browser = null;
      });

      this.browser = browser;
      console.log(
        `✅ Browser launched with NSE + CSP fixes (${isLambda ? 'lambda' : 'local'} mode)`
      );
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
    const { isLambda } = resolveEnvironment();
    const page = await browser.newPage();

    await page.setBypassCSP(true);

    await page.setViewport({
      width: 1366,
      height: 768,
    });

    await page.setUserAgent(USER_AGENT);

    await page.setRequestInterception(true);

    const blockedTypes = isLambda
      ? BLOCKED_RESOURCE_TYPES_LAMBDA
      : BLOCKED_RESOURCE_TYPES_LOCAL;

    page.on('request', (req) => {
      if (blockedTypes.has(req.resourceType())) {
        return req.abort();
      }
      return req.continue();
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
    await BrowserManager.createPage();

    console.log('Browser active:', BrowserManager.has());
  })();
}