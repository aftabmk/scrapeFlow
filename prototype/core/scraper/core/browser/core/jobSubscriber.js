const JobEvent = require('../../../../../events/jobEvent');

class JobSubscriber {
  constructor(handler) {
    this.handler = handler;
  }

  subscribe() {
    JobEvent.subscribe(this.handler);
  }
}

module.exports = JobSubscriber;