class BrowserLifecycle {
  constructor(environment) {
    this.environment = environment;
    this.browser = null;
  }

  async start() {
    this.browser = await this.environment.launch();
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

  async ensureBrowser() {
    try {
      if (!this.browser ||!this.browser.isConnected()) {
        console.warn(
          '[Browser] disconnected, launching new browser'
        );

        this.browser = await this.environment.launch();
      }

      return this.browser;
    } 
    catch (err) {
      console.error(
        '[Browser] health check failed, relaunching',
        err
      );

      this.browser = await this.environment.launch();

      return this.browser;
    }
  }

  /**
   * Remove frozen/unresponsive tabs.
   */
  async cleanupDeadPages(timeout = 5_000) {
    const browser = await this.ensureBrowser();

    const pages = await browser.pages();

    for (const page of pages) {
      try {
        // while page is loading (page.goto()) or page.evaluate skip the cleanup
        if (page.isClosed() || page._loading || page._processing || page.url == 'about:blank') {
          continue;
        }

        await Promise.race([
          page.evaluate(() => true),
          new Promise((_, reject) =>
            setTimeout(
              () => reject(new Error('Page timeout')),
              timeout
            )
          ),
        ]);
      } catch (err) {
        console.warn(
          `[Browser] closing unresponsive page: ${page.url()}`
        );

        try {
          await page.close();
        } 
        catch {
          console.warn("[Browser] error in page.close() method, cant close the page");
        }
      }
    }
  }

  async healthCheck() {
    const browser = await this.ensureBrowser();
    
    await this.cleanupDeadPages();

    const pages = await browser.pages();

    const results = await Promise.all(
      pages.map(async (page) => {
        try {
          await page.evaluate(() => true);

          return {
            url: page.url(),
            live: !page.isClosed(),
          };
        } catch {
          return {
            url: page.url(),
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