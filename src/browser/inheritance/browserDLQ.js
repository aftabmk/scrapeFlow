'use strict';

const { NavigationError, TimeoutError } = require('../../tab/tab');
const { DLQEvent } = require('../../dlq/dlqEvent');
const { BrowserHealth } = require('./browserHealth');

class BrowserDLQ extends BrowserHealth {
  async _runEvaluator(tab) {
    try {
      return await tab.evaluator();
    } catch (err) {
      if (err instanceof NavigationError) {
        console.warn(`[Browser] NavigationError on tab "${tab.name}" — recreating once`);
        try {
          await tab.recreate(this._browser);
          return await tab.evaluator();
        } catch (retryErr) {
          this._enqueueDLQ(tab, retryErr);
          throw retryErr;
        }
      }

      if (err instanceof TimeoutError) {
        console.warn(`[Browser] TimeoutError on tab "${tab.name}" — disposing and enqueuing DLQ`);
        await this._deleteTab(tab.name);
        this._enqueueDLQ(null, err, tab.data, 'TimeoutError', this._snapshotTab(tab));
        throw err;
      }

      // NetworkError or unknown
      this._enqueueDLQ(tab, err);
      throw err;
    }
  }

  _enqueueDLQ(tab, err, originalEvent, errorTypeOverride, snapshotOverride) {
    const event = new DLQEvent({
      originalEvent:  tab?.data ?? originalEvent,
      errorType:      errorTypeOverride ?? err.name ?? 'UnknownError',
      errorMessage:   err.message,
      tabSnapshot:    snapshotOverride ?? (tab ? this._snapshotTab(tab) : null),
      maxRetry:       this._opts.dlqMaxRetry,
    });

    console.warn(`[Browser] DLQ enqueue — pageId="${event.originalEvent.pageId}" errorType="${event.errorType}"`);
    this._emitter.emit('browser:dlq', event);
  }

  _snapshotTab(tab) {
    return {
      name:        tab.name,
      isActive:    tab.isActive,
      lastUsedAt:  tab.lastUsedAt,
      createdAt:   tab.createdAt,
      lastError:   tab.lastError,
    };
  }
}

module.exports = { BrowserDLQ };