// JobBuilder.js

const { Job } = require('./Job');

class JobBuilder {
  constructor(events) {
    this.events = events;
  }

  buildAll() {
    const jobs = [];

    for (const event of this.events) {
      try {
        jobs.push(new Job(event));
      } 
      catch (err) {
        console.warn(`[JobBuilder] Skipping exchange=${event.EXCHANGE} & contract=${event.CONTRACT}: ${err.message}`);
      }
    }

    return jobs;  // Job[]
  }
}

module.exports = { JobBuilder };