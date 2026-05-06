'use strict';

class NavigationError extends Error {
  constructor(message, cause) {
    super(message);
    this.name  = 'NavigationError';
    this.cause = cause;
  }
}

class NetworkError extends Error {
  constructor(message, cause) {
    super(message);
    this.name  = 'NetworkError';
    this.cause = cause;
  }
}

class TimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'TimeoutError';
  }
}

module.exports = { NavigationError, NetworkError, TimeoutError };