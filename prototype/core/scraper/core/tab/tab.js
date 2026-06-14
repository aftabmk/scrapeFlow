const Evaluator = require('./core/evaluator');
const Interceptor = require('./core/interceptor');

class Tab {
  constructor(page) {
    this.page = page;
    this.page._loading = false;
    this.page._processing = false;

    this.evaluator = new Evaluator(page);
    this.interceptor = new Interceptor(page);
  }

  async init() {
    this.page._loading = true;
    await this.interceptor.enable();
    this.page._loading = false;
  }
  
  async processJob(job) {
    this.page._processing = true;
    await this.evaluator.visit(job);
    this.page._processing = false;
    return this.evaluator.fetch();
  }

}

module.exports = Tab;