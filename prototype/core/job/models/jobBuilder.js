// JobBuilder.js

const { Job } = require('./Job');

class JobBuilder {
  _discoverIds() {
    return Object.keys(process.env)
      .filter(k => /^EXCHANGE_\d+$/.test(k))
      .map(k => parseInt(k.replace('EXCHANGE_', ''), 10))
      .sort((a, b) => a - b);
  }

  buildAll() {
    const ids  = this._discoverIds();
    const jobs = [];

    for (const id of ids) {
      try {
        jobs.push(new Job(id));
      } catch (err) {
        console.warn(`[JobBuilder] Skipping id=${id}: ${err.message}`);
      }
    }

    return jobs;  // Job[]
  }
}

module.exports = { JobBuilder };