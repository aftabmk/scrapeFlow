const eventBus = require('./eventBus');

const WorkflowEvent = {
  WORKFLOW_ASSIGNED: 'workflow:assigned',

  emit(workflowPayload) {
    eventBus.emit(WorkflowEvent.WORKFLOW_ASSIGNED, workflowPayload);
  },

  subscribe(handler) {
    eventBus.on(WorkflowEvent.WORKFLOW_ASSIGNED, handler);
  },
};

module.exports = WorkflowEvent;