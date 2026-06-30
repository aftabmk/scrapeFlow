const { WAIT_UNTIL } = require('./constants');

const GOTO_TIMEOUT_MS = 30_000;

class Evaluator {
  constructor(page) {
    this.page = page;
  }

  async visit(job) {
    await this.page.goto(job.page_url, {
      waitUntil: WAIT_UNTIL.DOM_CONTENT_LOADED,
      timeout: GOTO_TIMEOUT_MS,
    });

    await this.page.evaluate(event => {
      window.sessionManager.initialize(event);
    }, job);
  }

  async fetch(apiUrl) {
    return this.page.evaluate(url => {
      return window.sessionManager.fetch(url);
    }, apiUrl);
  }

  async getSession() {
    return this.page.evaluate(() => {
      return window.sessionManager.getSession();
    });
  }

  async getCookies() {
    return this.page.evaluate(() => {
      return window.sessionManager.getCookies();
    });
  }

  async getUserAgent() {
    return this.page.evaluate(() => {
      return window.sessionManager.getUserAgent();
    });
  }

  async reset() {
    return this.page.evaluate(() => {
      window.sessionManager.resetSession();
    });
  }
}

module.exports = Evaluator;