'use strict';

class BrowserEvent {
  
  constructor({ pageId, pageUrl, apiUrl, evaluateTimeout = 30_000, headers = {} }) {
    if (!pageId || typeof pageId !== 'string') {
      throw new TypeError('BrowserEvent: pageId must be a non-empty string');
    }
    if (!pageUrl || typeof pageUrl !== 'string') {
      throw new TypeError('BrowserEvent: pageUrl must be a non-empty string');
    }
    if (!apiUrl || typeof apiUrl !== 'string') {
      throw new TypeError('BrowserEvent: apiUrl must be a non-empty string');
    }

    this.pageId = pageId;
    this.pageUrl = pageUrl;
    this.apiUrl = apiUrl;

    this.evaluateTimeout = evaluateTimeout;

    this.headers = headers;

    this.createdAt = new Date();
  }


  toLog() {
    return {
      pageId: this.pageId,
      pageUrl: this.pageUrl,
      apiUrl: this.apiUrl,
      evaluateTimeout: this.evaluateTimeout,
      createdAt: this.createdAt.toISOString(),
    };
  }
}

module.exports = { BrowserEvent };