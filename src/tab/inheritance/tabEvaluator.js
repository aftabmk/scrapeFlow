"use strict";

const { TabInterceptor } = require("./tabInterceptor");
const { NavigationError, NetworkError, TimeoutError } = require("./tabErrors");

const { WAIT_UNTIL, CONTENT_TYPE } = require("./constants");

class TabEvaluator extends TabInterceptor {
  async evaluator() {
    await this._navigate();
    return this._fetchWithBoundary();
  }

  async _navigate() {
    try {
      await this._page.goto(this.data.pageUrl, {
        waitUntil: WAIT_UNTIL.DOM_CONTENT_LOADED,
        timeout: this.data.evaluateTimeout,
      });

      this.isActive = true;
    } 
    catch (err) {
      this.isActive = false;

      const wrapped = new NavigationError(err.message, err);

      this.lastError = wrapped.message;

      throw wrapped;
    }
  }

  async _fetchWithBoundary() {
    const { pageId, apiUrl, evaluateTimeout, refer_url } = this.data;

    const { fetchTimeout } = this;
    try {
      const getHeaders = this.getHeaders;
      const includeHeaders = /exchange_1/i.test(pageId);

      const result = await Promise.race([
        this._page.evaluate(this.browserFetch, {
          url: apiUrl,
          timeoutMs: fetchTimeout,
          contentType: CONTENT_TYPE.JSON,
          pageId,
          referUrl: refer_url,
          getHeaders,
          includeHeaders,
        }),

        this._timeout(evaluateTimeout, "evaluate()"),
      ]);

      this.lastUsedAt = new Date();

      return result;
    } catch (err) {
      this.lastError = err.message;

      if (err instanceof TimeoutError) {
        throw err;
      }

      if (err instanceof TypeError) {
        throw err;
      }

      throw new NetworkError(err.message, err);
    }
  }

  _timeout(ms, label) {
    return new Promise((_, reject) => setTimeout(() => reject(new TimeoutError(`${label} timed out after ${ms}ms`)), ms));
  }

  // ✅ browser-context-safe
  async browserFetch({ url, timeoutMs, contentType, pageId, referUrl, includeHeaders }) {
    const headers = {
      "Content-Type": contentType,

      ...(includeHeaders && {
        Accept: "application/json, text/plain, */*",

        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",

        Referer: referUrl,

        "Accept-Encoding": "gzip, deflate, br",

        Connection: "keep-alive",

        Cookie: document.cookie,
      }),
    };

    const controller = new AbortController();

    const tid = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        headers,
        signal: controller.signal,
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }

      return await res.json();
    } finally {
      clearTimeout(tid);
    }
  }
}

module.exports = { TabEvaluator };
