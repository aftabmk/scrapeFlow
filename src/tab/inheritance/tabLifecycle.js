'use strict';

const { TabEvaluator } = require('./tabEvaluator');

class TabLifecycle extends TabEvaluator {
  async recreate(browser) {
    await this._closePage();
    this._page     = await browser.newPage();
    await this.interceptor();
    this.isActive  = false;
    this.lastError = null;
    this.lastUsedAt = new Date();
  }

  async dispose() {
    this.isActive = false;
    await this._closePage();
  }

  async _closePage() {
    try {
      if (!this._page.isClosed()) await this._page.close();
    } catch { /* already gone */ }
  }
}

module.exports = { TabLifecycle };