class BrowserEnvironment {
  async launch() {
    throw new Error('launch() must be implemented');
  }
}

module.exports = BrowserEnvironment;