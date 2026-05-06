'use strict';

const { BrowserDLQ } = require('./browserDLQ');

class BrowserLifecycle extends BrowserDLQ {
  _detectEnv() {
    return process.env.AWS_LAMBDA_FUNCTION_NAME ? 'lambda' : 'local';
  }

  async _loadDriver() {
    const puppeteerExtra = require('puppeteer-extra');
    puppeteerExtra.use(require('puppeteer-extra-plugin-stealth')());

    if (this._env === 'lambda') {
      const chromium = require('@sparticus/chromium');
      return {
        puppeteer: puppeteerExtra,
        launchOptions: {
          args:            chromium.args,
          defaultViewport: chromium.defaultViewport,
          executablePath:  await chromium.executablePath,
          headless:        chromium.headless,
        },
      };
    }

    return {
      puppeteer: puppeteerExtra,
      launchOptions: {
        headless: false,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
      },
    };
  }

  async init() {
    if (this._isLaunched && this._browser) return this.reuse();
    const { puppeteer, launchOptions } = await this._loadDriver();
    this._browser       = await puppeteer.launch(launchOptions);
    this._isLaunched    = true;
    this._lastHealthyAt = new Date();
    this._startHealthChecks();
  }

  async reuse() {
    if (!this._browser?.isConnected()) {
      console.warn('[Browser] reuse(): browser disconnected — restarting');
      await this._restart();
    }
  }

  async close() {
    this._stopHealthChecks();
    await this._disposeAllTabs();
    try { if (this._browser) await this._browser.close(); }
    catch (err) { console.warn('[Browser] close(): error —', err.message); }
    this._browser        = null;
    this._isLaunched     = false;
    // subclass resets _instance
  }

  async _restart() {
    console.warn('[Browser] _restart(): full browser restart');
    await this._disposeAllTabs();
    try { if (this._browser) await this._browser.close(); } catch { /* already dead */ }
    this._browser    = null;
    this._isLaunched = false;
    await this.init();
  }
}

module.exports = { BrowserLifecycle };