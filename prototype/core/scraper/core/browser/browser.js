const BrowserActor = require('./core/browserActor');
const { LFUCache } = require('../../../../algorithms/LFUCache/algorithms/LFUCahche');
const Tab          = require('../tab/tab');

const MAX_TABS = 5;

let _instance = null;

class Browser extends BrowserActor {
  constructor() {
    super();
    if (_instance) return _instance;

    this.browser = null;
    this.cache   = new LFUCache(MAX_TABS);
    this.queue   = [];

    _instance = this;
  }

  static getInstance() {
    if (!_instance) new Browser();
    return _instance;
  }

  async init() {
    const isLambda = !!process.env.AWS_LAMBDA_FUNCTION_NAME;

    if (isLambda) {
      const chromium       = require('@sparticuz/chromium');
      const puppeteerExtra = require('puppeteer-extra');
      const StealthPlugin  = require('puppeteer-extra-plugin-stealth');

      puppeteerExtra.use(StealthPlugin());

      this.browser = await puppeteerExtra.launch({
        args:            chromium.args,
        defaultViewport: chromium.defaultViewport,
        executablePath:  await chromium.executablePath(),
        headless:        chromium.headless,
      });
    } else {
      const puppeteerExtra = require('puppeteer-extra');
      const StealthPlugin  = require('puppeteer-extra-plugin-stealth');

      puppeteerExtra.use(StealthPlugin());

      this.browser = await puppeteerExtra.launch({
        headless: false,
        devtools: true,
        args:     ['--no-sandbox', '--disable-setuid-sandbox'],
      });
    }

    console.log('[Browser] launched');
    this.subscribeToJobs();
    this._startHealthCheck();
  }

  async onJob(job) {
    const existing = this.cache.get(job.id);
    if (existing !== -1) {
      await existing.processJob(job);
      return;
    }

    if (this.cache.size < MAX_TABS) {
      await this._spawnTab(job);
    } else {
      console.log(`[Browser] all ${MAX_TABS} slots full — queuing job ${job.id}`);
      this.queue.push(job);
    }
  }

  async _spawnTab(job) {
    const page = await this.browser.newPage();
    const tab  = new Tab(page, () => this._onTabClosed(job.id));

    this.cache.set(job.id, tab);
    await tab.init();
    await tab.processJob(job);
  }

  async _onTabClosed(jobId) {
    console.log(`[Browser] tab closed for job ${jobId}`);
    if (this.queue.length > 0) {
      const next = this.queue.shift();
      await this._spawnTab(next);
    }
  }

  async healthCheck() {
    const pages   = await this.browser.pages();
    const results = await Promise.all(
      pages.map(async (page) => {
        try {
          return { url: page.url(), live: !page.isClosed() };
        } catch {
          return { url: null, live: false };
        }
      })
    );
    const allLive = results.every((r) => r.live);
    console.log('[Browser] health:', results);
    return allLive;
  }

  async close() {
    this._stopHealthCheck();
    if (this.browser) await this.browser.close();
    _instance = null;
    console.log('[Browser] closed');
  }
}

module.exports = Browser;