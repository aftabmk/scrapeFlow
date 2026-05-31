const Tab = require('../../tab/tab');
const FetchResponse = require('./injector/fetchResponse');
const SessionManager = require('./injector/sessionManager');
class TabLifecycle {
  constructor(browser, onClose) {
    this.browser = browser;
    this.onClose = onClose;
  }
  
  async create(job) {
    const page = await this.browser.newPage();
    
    await page.evaluateOnNewDocument(`
      // class injection
      window.sessionInjector = ${SessionManager.toString()};
      window.sessionManager = new window.sessionInjector();
      
      window.fetchResponse  = ${FetchResponse.toString()};
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