// JobBuilder.js

const { Job } = require('./Job');

class JobBuilder {
  constructor(events) {
    this.events = events;
  }

  buildAll() {
    // Strict validation for events
    if (!this.events) {
      throw new Error('JobBuilder: events parameter is required');
    }

    const jobs = [];

    for (const event of this.events) {
      try {
        if (!event) {
          console.warn('[JobBuilder] Skipping null/undefined event');
          continue;
        }

        const job = new Job(event);
        jobs.push(job);

      } catch (err) {
        const exchange = event?.EXCHANGE || 'unknown';
        const contract = event?.CONTRACT || 'unknown';

        console.warn(
          `[JobBuilder] Skipping exchange=${exchange} & contract=${contract}: ${err.message}`
        );
      }
    }

    // Optional: Safety check
    if (jobs.length === 0) {
      console.warn('[JobBuilder] Warning: No jobs were built from the available events.');
    }

    return jobs; // Job[]
  }
}

module.exports = { JobBuilder };