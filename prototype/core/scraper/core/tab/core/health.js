class Health {
  constructor(page, onClose) {
    this.page = page;
    this.onClose = onClose;
    this.timer = null;
  }

  start(intervalMs = 30_000) {
    this.timer = setInterval(async () => {
      await this.check();
    }, intervalMs);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async check() {
    try {
      if (this.page.isClosed()) {
        console.warn('[Health] page closed');
        await this.onClose();
        return;
      }

      await this.page.evaluate(() => true);
    } catch {
      console.warn('[Health] page unresponsive');
      await this.onClose();
    }
  }
}

module.exports = Health;