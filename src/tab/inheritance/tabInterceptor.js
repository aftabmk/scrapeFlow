'use strict';

const { BLOCKED_RESOURCE_TYPES } = require('./constants');

class TabInterceptor {
  async interceptor() {
    await this._page.setRequestInterception(true);

    this._page.on('request', (req) => {
      BLOCKED_RESOURCE_TYPES.has(req.resourceType())
        ? req.abort()
        : req.continue();
    });
  }
}

module.exports = { TabInterceptor };