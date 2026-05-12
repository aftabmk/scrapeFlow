const JobEvent      = require('../events/jobEvent');
const WorkflowEvent = require('../events/workflowEvent');
const TracerEvent   = require('../events/tracerEvent');

class WorkflowClass {
  constructor() {
    JobEvent.subscribe((jobPayload) => {
      this.assignWorkflow(jobPayload);
    });
  }

  assignWorkflow(jobPayload) {
    const { type, key } = jobPayload;

    switch (type) {
      case 'equity': return this.equityWorkflow(jobPayload);
      case 'future': return this.futureWorkflow(jobPayload);
      case 'option': return this.optionWorkflow(jobPayload);
      default:
        TracerEvent.warn(key, ['JobEvent', 'WorkflowClass', 'assignWorkflow'], `Unknown job type: ${type}`);
    }
  }

  equityWorkflow(payload) {
    TracerEvent.trace(payload.key, ['JobEvent', 'WorkflowClass', 'equityWorkflow', 'WorkflowEvent']);
    WorkflowEvent.emit({ ...payload, workflow: 'equity' });
  }

  futureWorkflow(payload) {
    TracerEvent.trace(payload.key, ['JobEvent', 'WorkflowClass', 'futureWorkflow', 'WorkflowEvent']);
    WorkflowEvent.emit({ ...payload, workflow: 'future' });
  }

  optionWorkflow(payload) {
    TracerEvent.trace(payload.key, ['JobEvent', 'WorkflowClass', 'optionWorkflow', 'WorkflowEvent']);
    WorkflowEvent.emit({ ...payload, workflow: 'option' });
  }

  run(jobPayload) {
    this.assignWorkflow(jobPayload);
  }
}

module.exports = WorkflowClass;