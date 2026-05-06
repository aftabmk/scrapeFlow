'use strict';

class DLQEvent {

  constructor({
    originalEvent,
    errorType,
    errorMessage,
    tabSnapshot = null,
    maxRetry = 3,
    retryCount = 0,
    backoffBaseMs = 200,
    maxBackoffMs = 10_000,
  }) {
    if (!originalEvent) {
      throw new TypeError('DLQEvent: originalEvent is required');
    }
    if (!errorType || !errorMessage) {
      throw new TypeError('DLQEvent: errorType and errorMessage are required');
    }

    this.originalEvent = originalEvent;
    this.errorType = errorType;
    this.errorMessage = errorMessage;
    this.tabSnapshot = tabSnapshot;
    this.maxRetry = maxRetry;
    this.retryCount = retryCount;
    this.backoffBaseMs = backoffBaseMs;
    this.maxBackoffMs = maxBackoffMs;
    this.enqueuedAt = new Date();
    this.lastAttemptAt = null;
    this.errorHistory = [errorMessage];
  }

  canRetry() {
    return this.retryCount < this.maxRetry;
  }


  recordAttempt(newErrorMessage) {
    this.retryCount++;
    this.lastAttemptAt = new Date();
    if (newErrorMessage) this.errorHistory.push(newErrorMessage);
  }

  nextBackoffMs() {
    return Math.min(
      Math.pow(2, this.retryCount) * this.backoffBaseMs,
      this.maxBackoffMs,
    );
  }

  wait() {
    return new Promise((r) => setTimeout(r, this.nextBackoffMs()));
  }

  recoveryAction() {
    if (!this.canRetry()) return 'discard';

    switch (this.errorType) {
      case 'NavigationError': return 'recreate-tab';
      case 'NetworkError':    return 'replay-only';
      case 'TimeoutError':    return 'dispose-and-replay';
      default:                return this.retryCount === 0 ? 'replay-only' : 'discard';
    }
  }


  toLog() {
    return {
      pageId: this.originalEvent.pageId,
      pageUrl: this.originalEvent.pageUrl,
      errorType: this.errorType,
      errorMessage: this.errorMessage,
      errorHistory: this.errorHistory,
      retryCount: this.retryCount,
      maxRetry: this.maxRetry,
      enqueuedAt: this.enqueuedAt.toISOString(),
      lastAttemptAt: this.lastAttemptAt?.toISOString() ?? null,
      tabSnapshot: this.tabSnapshot
        ? {
            name: this.tabSnapshot.name,
            isActive: this.tabSnapshot.isActive,
            lastUsedAt: this.tabSnapshot.lastUsedAt?.toISOString(),
            lastError: this.tabSnapshot.lastError,
          }
        : null,
    };
  }
}

module.exports = { DLQEvent };