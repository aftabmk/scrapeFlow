'use strict';

const { LFUCache }          = require('../../algorithm/LFUCache/lfuCache');
const { Tab }               = require('../tab/tab');
const { BrowserLifecycle }  = require('./inheritance/browserLifecycle');

class Browser extends BrowserLifecycle {

  // ── factory ───────────────────────────────────────────────────────────

  static _instance = null;

  static getInstance(options = {}) {
    if (!Browser._instance) Browser._instance = new Browser(options);
    return Browser._instance;
  }

  // ── constructor ───────────────────────────────────────────────────────

  constructor(opts = {}) {
    super();
    if (Browser._instance)  throw new Error('Browser is a singleton — use Browser.getInstance()');
    if (!opts.emitter)       throw new TypeError('Browser: opts.emitter is required');

    this._emitter = opts.emitter;
    this._opts = {
      maxTabs:                 opts.maxTabs                ?? 10,
      maxMemoryMb:             opts.maxMemoryMb            ?? 400,
      browserHealthIntervalMs: opts.browserHealthIntervalMs ?? 45_000,
      tabHealthIntervalMs:     opts.tabHealthIntervalMs    ?? 12_000,
      dlqMaxRetry:             opts.dlqMaxRetry            ?? 3,
      tabFetchTimeout:         opts.tabFetchTimeout        ?? 30_000,
      tabStaleAfterMs:         opts.tabStaleAfterMs        ?? 300_000,
    };

    this._env                = opts.env ?? this._detectEnv();
    this._browser            = null;
    this._isLaunched         = false;
    this._cache              = new LFUCache(this._opts.maxTabs);
    this._lastHealthyAt      = null;
    this._browserHealthTimer = null;
    this._tabHealthTimer     = null;
  }

  // ── lifecycle override (clear singleton ref on close) ─────────────────

  async close() {
    await super.close();
    Browser._instance = null;
  }

  // ── event entry point ─────────────────────────────────────────────────

  async handleEvent(event) {
    await this.reuse();
    let tab = this._cache.get(event.pageId);
    if (!tab) tab = await this._createTab(event.pageId, event);
    else tab.data = event;
    return this._runEvaluator(tab);
  }

  // ── tab ops ───────────────────────────────────────────────────────────

  async _createTab(name, data) {
    const page = await this._browser.newPage();
    const tab  = new Tab({ name, page, data,
      fetchTimeout: this._opts.tabFetchTimeout,
      staleAfterMs: this._opts.tabStaleAfterMs,
    });
    await tab.interceptor();
    const evicted = this._cache.put(name, tab);
    if (evicted) { console.info(`[Browser] LFU evicted tab "${evicted.name}"`); await evicted.tab.dispose(); }
    return tab;
  }

  getTab(name)            { return this._cache.get(name); }

  async _deleteTab(name) {
    const tab = this._cache.delete(name);
    if (tab) await tab.dispose();
  }

  async _disposeAllTabs() {
    const entries = this._cache.entries();
    this._cache.clear();
    await Promise.allSettled(entries.map(({ tab }) => tab.dispose()));
  }
}

module.exports = { Browser };