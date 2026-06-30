const eventBus = require('./eventBus');

const JobEvent = {
  JOB_CREATED: 'job:created',

  emit(jobPayload) {
    eventBus.emit(JobEvent.JOB_CREATED, jobPayload);
  },

  subscribe(handler) {
    eventBus.on(JobEvent.JOB_CREATED, handler);
  },
};

module.exports = JobEvent;