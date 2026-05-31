const Tab = require('../../tab/tab');
const SessionManager = require('./sessionManager');

class TabLifecycle {
  constructor(browser, onClose) {
    this.browser = browser;
    this.onClose = onClose;
  }

  async create(job) {
    const page = await this.browser.newPage();

    await page.evaluateOnNewDocument(`
      window.sessionInjector = ${SessionManager.toString()};
      window.sessionManager = new window.sessionInjector();
    `);

    const tab = new Tab(
      page,
      () => this.onClose(job.id)
    );

    await tab.init();

    return tab;
  }
}

module.exports = TabLifecycle;