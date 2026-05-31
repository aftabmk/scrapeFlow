const TabLifecycle = require('./core/tabLifecycle');
const QueueManager = require('./core/queueManager');
const HealthMonitor = require('./core/healthMonitor');
const JobSubscriber = require('./core/jobSubscriber');
const BrowserLifecycle = require('./core/browserLifecycle');
const BrowserEnvironment = require('./core/environment/browserEnvironment');

const MAX_TABS = 5;

let _instance = null;

class Browser {
  constructor() {
    if (_instance) return _instance;

    this.lifecycle = null;
    this.tabs = null;

    this.queueManager = new QueueManager(MAX_TABS);

    this.healthMonitor = new HealthMonitor(
      () => this.healthCheck()
    );

    this.jobSubscriber = new JobSubscriber(
      (job) => this.onJob(job)
    );

    _instance = this;
  }

  static getInstance() {
    if (!_instance) {
      new Browser();
    }

    return _instance;
  }

  async init() {
    const environment = new BrowserEnvironment();

    this.lifecycle = new BrowserLifecycle(environment);

    const browser = await this.lifecycle.start();

    this.tabs =
      new TabLifecycle(
        browser,
        this._onTabClosed.bind(this)
      );

    this.jobSubscriber.subscribe();

    this.healthMonitor.start();

    return this;
  }

  async onJob(job) {
    const existing =
      this.queueManager.get(job.id);

    if (existing !== -1) {
      const data = await existing.processJob(job);
      return;
    }

    if (!this.queueManager.hasCapacity()) {
      console.log(
        `[Browser] all ${MAX_TABS} slots full — queuing job ${job.id}`
      );

      this.queueManager.enqueue(job);
      return;
    }

    await this._spawnTab(job);
  }

  async _spawnTab(job) {
    const tab =
      await this.tabs.create(job);

    this.queueManager.add(
      job.id,
      tab
    );

    const data = await tab.processJob(job);
    console.log({ data });
    // await tab.processJob(job);
  }

  async _onTabClosed(jobId) {
    this.queueManager.remove(jobId);

    console.log(
      `[Browser] tab closed for job ${jobId}`
    );

    if (this.queueManager.hasQueuedJobs) {
      const next =
        this.queueManager.dequeue();

      await this._spawnTab(next);
    }
  }

  async healthCheck() {
    return this.lifecycle.healthCheck();
  }

  async close() {
    this.healthMonitor.stop();

    await this.lifecycle.stop();

    _instance = null;

    console.log('[Browser] closed');
  }
}

module.exports = Browser;