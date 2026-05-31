const Health = require('./core/health');
const Evaluator = require('./core/evaluator');
const Interceptor = require('./core/interceptor');

class Tab {
  constructor(page, onClose) {
    this.page = page;
    this.onClose = onClose;

    this.fetcher = new Evaluator(page);
    this.interceptor = new Interceptor(page);
    this.health = new Health(
      page,
      this.close.bind(this)
    );
  }

  async init() {
    await this.interceptor.enable();
    this.health.start();
  }

  async processJob(job) {
    await this.fetcher.visit(job);

    return this.fetcher.fetch();
  }

  async close() {
    this.health.stop();

    await this.fetcher.reset();

    try {
      if (!this.page.isClosed()) {
        await this.page.close();
      }
    } catch {}

    this.onClose?.();
  }
}

module.exports = Tab;