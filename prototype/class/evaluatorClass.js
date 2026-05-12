const ScraperEvent   = require('../events/scraperEvent');
const EvaluatorEvent = require('../events/evaluatorEvent');
const TracerEvent    = require('../events/tracerEvent');

class EvaluatorClass {
  constructor() {
    ScraperEvent.subscribe((scrapePayload) => {
      this.evaluateData(scrapePayload);
    });
  }

  evaluateData(scrapePayload) {
    TracerEvent.trace(scrapePayload.key, ['ScraperEvent', 'EvaluatorClass', 'evaluateData']);
    const result = {
      key:         scrapePayload.key,
      symbol:      scrapePayload.symbol,
      workflow:    scrapePayload.workflow,
      score:       Math.random(),
      evaluatedAt: Date.now(),
    };
    this.sendData(result);
  }

  sendData(result) {
    TracerEvent.trace(result.key, ['EvaluatorClass', 'evaluateData', 'sendData', 'EvaluatorEvent']);
    EvaluatorEvent.emit(result);
    // terminal node — plug in DB write / webhook here
  }

  run(scrapePayload) {
    this.evaluateData(scrapePayload);
  }
}

module.exports = EvaluatorClass;