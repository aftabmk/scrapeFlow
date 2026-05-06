'use strict';

const { TabInterceptor }                       = require('./tabInterceptor');
const { NavigationError, NetworkError, TimeoutError } = require('./tabErrors');

class TabEvaluator extends TabInterceptor {
  async evaluator() {
    await this._navigate();
    return this._fetchWithBoundary();
  }

  async _navigate() {
    try {
      await this._page.goto(this.data.pageUrl, {
        waitUntil: 'domcontentloaded',
        timeout:   this.data.evaluateTimeout,
      });
      this.isActive = true;
    } catch (err) {
      this.isActive  = false;
      const wrapped  = new NavigationError(err.message, err);
      this.lastError = wrapped.message;
      throw wrapped;
    }
  }

  async _fetchWithBoundary() {
    const { apiUrl, evaluateTimeout, headers: extraHeaders } = this.data;
    const { fetchTimeout } = this;

    try {
      const result = await Promise.race([
        this._page.evaluate(
          async (url, extra, timeoutMs) => {
            const headers = {
              'Content-Type': 'application/json',
              Cookie: document.cookie,
              ...extra,
            };
            const controller = new AbortController();
            const tid = setTimeout(() => controller.abort(), timeoutMs);
            try {
              const res = await fetch(url, { headers, signal: controller.signal });
              if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
              return res.json();
            } finally {
              clearTimeout(tid);
            }
          },
          apiUrl, extraHeaders, fetchTimeout,
        ),
        this._timeout(evaluateTimeout, 'evaluate()'),
      ]);

      this.lastUsedAt = new Date();
      return result;
    } catch (err) {
      this.lastError = err.message;
      if (err instanceof TimeoutError) throw err;
      throw new NetworkError(err.message, err);
    }
  }

  _timeout(ms, label) {
    return new Promise((_, reject) =>
      setTimeout(() => reject(new TimeoutError(`${label} timed out after ${ms}ms`)), ms),
    );
  }
}

module.exports = { TabEvaluator };