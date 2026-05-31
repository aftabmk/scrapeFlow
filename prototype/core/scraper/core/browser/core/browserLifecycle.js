class BrowserLifecycle {
  constructor(environment) {
    this.environment = environment;
    this.browser = null;
  }

  async start() {
    this.browser = await this.environment.launch();

    // close blank tab at start saves approx 2.6 mb
    // const [blankTab] = await this.browser.pages();
    // if (blankTab) await blankTab.close();

    console.log('[Browser] launched');

    return this.browser;
  }

  async stop() {
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
    }

    console.log('[Browser] closed');
  }

  async healthCheck() {
    const pages = await this.browser.pages();

    const results = await Promise.all(
      pages.map(async (page) => {
        try {
          return {
            url: page.url(),
            live: !page.isClosed(),
          };
        } catch {
          return {
            url: null,
            live: false,
          };
        }
      })
    );

    console.log('[Browser] health:', results);

    return results.every((r) => r.live);
  }
}

module.exports = BrowserLifecycle;