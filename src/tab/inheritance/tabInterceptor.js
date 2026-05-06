'use strict';

const BLOCKED_RESOURCE_TYPES = new Set([
  'stylesheet', 'image', 'font', 'script',
  'xhr', 'websocket', 'media', 'other',
]);

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