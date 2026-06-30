const Tab = require('../../tab/tab');

const FetchError = require('./injectors/fetchErrors');
const FetchResponse = require('./injectors/fetchResponse');

const HTMLRequest = require('./injectors/htmlRequest');
const SessionManager = require('./injectors/sessionManager');
class TabLifecycle {
  constructor(browser) {
    this.browser = browser;
  }
  
  async create(job) {
    const page = await this.browser.newPage();
    
    await page.evaluateOnNewDocument(`
      // class injection
      window.sessionInjector = ${SessionManager.toString()};
      window.sessionManager = new window.sessionInjector();
      
      // response and error injection
      window.fetchError  = ${FetchError.toString()};
      window.fetchResponse  = ${FetchResponse.toString()};

      // html request injection
      window.HTMLRequestInjector = ${HTMLRequest.toString()};
      window.HTMLRequest = new window.HTMLRequestInjector();
    `);

    const tab = new Tab(page);

    await tab.init();

    return tab;
  }
}

module.exports = TabLifecycle;