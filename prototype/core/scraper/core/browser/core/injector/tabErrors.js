const ERROR = Object.freeze({
  NETWORK: 'NetworkError',
  TIMEOUT: 'TimeoutError',
  NAVIGATION: 'NavigationError',
});

class BaseError extends Error {
  constructor(type, error) {
    super(error.message);

    this.name = type;
    this.cause = error;
  }

  static from(error) {
    if (
      error.name === 'TimeoutError' ||
      error.code === 'ETIMEDOUT'
    ) {
      return new TimeoutError(error);
    }

    if (
      error.message?.includes('Navigation') ||
      error.message?.includes('net::ERR_ABORTED')
    ) {
      return new NavigationError(error);
    }

    return new NetworkError(error);
  }
}

class NetworkError extends BaseError {
  constructor(error) {
    super(ERROR.NETWORK, error);
  }
}

class TimeoutError extends BaseError {
  constructor(error) {
    super(ERROR.TIMEOUT, error);
  }
}

class NavigationError extends BaseError {
  constructor(error) {
    super(ERROR.NAVIGATION, error);
  }
}

module.exports = {
  BaseError,
  NetworkError,
  TimeoutError,
  NavigationError,
};