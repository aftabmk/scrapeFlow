'use strict';

const eventBus = require('../eventBus/eventBus');
const { BrowserEvent } = require('../browser/browserEvent');
const { JOB_PRODUCER_EVENTS } = require('./jobProducer');


class JobConsumer {
  constructor(opts = {}) {
    this._evaluateTimeout = opts.evaluateTimeout ?? 30_000;
    this._started = false;

    // Bound handler references — stored so they can be removed on stop()
    this._onJobCreated = this._handleJobCreated.bind(this);
    this._onJobFailed  = this._handleJobFailed.bind(this);
  }


  start() {
    if (this._started) return;
    eventBus.subscribe(JOB_PRODUCER_EVENTS.JOB_CREATED, this._onJobCreated);
    eventBus.subscribe(JOB_PRODUCER_EVENTS.JOB_FAILED,  this._onJobFailed);
    this._started = true;
    console.info('[JobConsumer] started — listening for jobs');
  }


  stop() {
    if (!this._started) return;
    eventBus.unsubscribe(JOB_PRODUCER_EVENTS.JOB_CREATED, this._onJobCreated);
    eventBus.unsubscribe(JOB_PRODUCER_EVENTS.JOB_FAILED,  this._onJobFailed);
    this._started = false;
    console.info('[JobConsumer] stopped');
  }

  _handleJobCreated({ data }) {
    const { job } = data;

    try {

      const resolvedApiUrl = job.url_builder || job.api_url;

      if (!resolvedApiUrl) {
        throw new TypeError(`job "${job.id}" has neither api_url nor url_builder`);
      }

      const browserEvent = new BrowserEvent({
        pageId:          job.id,
        pageUrl:         job.page_url,
        apiUrl:          resolvedApiUrl,
        evaluateTimeout: this._evaluateTimeout,
        headers: job.exchange ? { 'x-exchange': job.exchange } : {},
      });

      console.info(`[JobConsumer] job "${job.id}" (${job.exchange ?? 'no-exchange'}) → browser:request`);
      eventBus.publish('browser:request', browserEvent);
    } catch (err) {
      console.error(`[JobConsumer] failed to build BrowserEvent for job "${job.id}":`, err.message);
      eventBus.publish('consumer:error', { job, error: err.message });
    }
  }


  _handleJobFailed({ data }) {
    console.error(`[JobConsumer] producer reported failure for job "${data.job?.id}":`, data.error);
  }
}

module.exports = { JobConsumer };