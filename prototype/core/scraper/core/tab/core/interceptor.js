const { /*BLOCKED_RESOURCE_TYPES,*/ ALLOWED_RESOURCE_TYPES } = require('./constants');

class Interceptor {
  constructor(page) {
    this.page = page;
  }

  async enable() {
    await this.page.setRequestInterception(true);
    
    this.page.on('request', (request) => {
      const type = request.resourceType();
      // const isAllowed = BLOCKED_RESOURCE_TYPES.has(type) ? request.abort() : request.continue();
      const isAllowed = ALLOWED_RESOURCE_TYPES.has(type) ? request.continue() : request.abort();
      return isAllowed;
    });
  }

  async disable() {
    await this.page.setRequestInterception(false);
  }
}

module.exports = Interceptor;