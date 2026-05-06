'use strict';

const { TabLifecycle } = require('./inheritance/tabLifecycle');
const { NavigationError, NetworkError, TimeoutError }   = require('./inheritance/tabErrors');

class Tab extends TabLifecycle {
  constructor({ name, page, data, fetchTimeout = 30_000, staleAfterMs = 300_000 }) {
    super();
    this.name         = name;
    this._page        = page;
    this.data         = data;
    this.isActive     = false;
    this.fetchTimeout = fetchTimeout;
    this.staleAfterMs = staleAfterMs;
    this.lastUsedAt   = new Date();
    this.createdAt    = new Date();
    this.lastError    = null;
  }

  checkAlive() {
    if (this._page.isClosed()) { this.isActive = false; return false; }
    return true;
  }

  isStale() {
    return Date.now() - this.lastUsedAt.getTime() > this.staleAfterMs;
  }

  async ping() {
    try {
      await Promise.race([this._page.title(), this._timeout(5_000, 'ping')]);
      return true;
    } catch {
      this.isActive = false;
      return false;
    }
  }
}

// single export surface — callers only ever import from tab.js
module.exports = { Tab, NavigationError, NetworkError, TimeoutError };