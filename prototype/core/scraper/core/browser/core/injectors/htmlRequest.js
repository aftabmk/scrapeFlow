class HTMLRequest {
  constructor(defaultTimeout = 30000) {
    this.defaultTimeout = defaultTimeout;
  }

  async fetch(url, options = {}) {
    const { timeout = this.defaultTimeout, ...fetchOptions } = options;

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
      return await fetch(url, {
        ...fetchOptions,
        signal: controller.signal,
      });
    }
    finally {
      clearTimeout(timeoutId);
    }
  }
}

module.exports = HTMLRequest;