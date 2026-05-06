'use strict';

const eventBus = require('../eventBus/eventBus');


const JOB_PRODUCER_EVENTS = {
  JOB_CREATED: 'jobproducer:job:created',
  JOB_FAILED:  'jobproducer:job:failed',
};


class JobProducer {

  constructor(producerId) {
    this.producerId = producerId;
    this.name = `JobProducer#${producerId}`;
  }


  createJob({ id, exchange, page_url, api_url, url_builder }) {
    // retry intentionally not accepted — retry budget lives on DLQEvent
    const job = { id, exchange, page_url, api_url, url_builder };

    try {
      if (!id || !page_url || !api_url) {
        throw new TypeError(
          `Job missing required fields (id, page_url, api_url) — got: ${JSON.stringify(job)}`
        );
      }
      if (!exchange) {
        console.warn(`[${this.name}] job "${id}" has no exchange — will publish without exchange metadata`);
      }

      eventBus.publish(JOB_PRODUCER_EVENTS.JOB_CREATED, {
        producerId: this.producerId,
        job,
      });
    } catch (err) {
      console.error(`[${this.name}] createJob failed for id="${id}":`, err.message);
      eventBus.publish(JOB_PRODUCER_EVENTS.JOB_FAILED, {
        producerId: this.producerId,
        job,
        error: err.message,
      });
    }
  }

  createJobs(jobs) {
    jobs.forEach((job) => this.createJob(job));
    return jobs.length;
  }
}

module.exports = { JobProducer, JOB_PRODUCER_EVENTS };