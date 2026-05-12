const WorkflowEvent = require('../events/workflowEvent');
const ScraperEvent  = require('../events/scraperEvent');
const TracerEvent   = require('../events/tracerEvent');

class ScraperClass {
  constructor() {
    WorkflowEvent.subscribe((workflowPayload) => {
      this.gotoPage(workflowPayload);
    });
  }

  gotoPage(workflowPayload) {
    TracerEvent.trace(workflowPayload.key, ['WorkflowEvent', 'ScraperClass', 'gotoPage']);
    const page = {
      ...workflowPayload,
      page: `/${workflowPayload.workflow}/${workflowPayload.symbol}`,
    };
    this.fetchApi(page);
  }

  fetchApi(pagePayload) {
    TracerEvent.trace(pagePayload.key, ['ScraperClass', 'gotoPage', 'fetchApi']);
    const data = {
      ...pagePayload,
      raw: `<html>mock data for ${pagePayload.symbol}</html>`,
    };
    this.sendData(data);
  }

  sendData(data) {
    TracerEvent.trace(data.key, ['ScraperClass', 'fetchApi', 'sendData', 'ScraperEvent']);
    ScraperEvent.emit(data);
  }

  run(workflowPayload) {
    this.gotoPage(workflowPayload);
  }
}

module.exports = ScraperClass;