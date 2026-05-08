'use strict';

const { ERROR } = require('./constants');

class NavigationError extends Error {
  constructor(message, cause) {
    super(message);
    this.name  = ERROR.NAVIGATION;
    this.cause = cause;
  }
}

class NetworkError extends Error {
  constructor(message, cause) {
    super(message);
    this.name  = ERROR.NETWORK;
    this.cause = cause;
  }
}

class TimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = ERROR.TIMEOUT;
  }
}

module.exports = { NavigationError, NetworkError, TimeoutError };