const eventBus = require('./eventBus');

const EvaluatorEvent = {
  EVALUATION_COMPLETE: 'evaluator:complete',

  emit(evaluationPayload) {
    eventBus.emit(EvaluatorEvent.EVALUATION_COMPLETE, evaluationPayload);
  },

  subscribe(handler) {
    eventBus.on(EvaluatorEvent.EVALUATION_COMPLETE, handler);
  },
};

module.exports = EvaluatorEvent;