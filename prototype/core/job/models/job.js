// Job.js

const { JobSchema } = require('./JobSchema');
const TradeIdBuilder = require('./tradeBuilder');
const { Validator } = require('../../../algorithms/jsonValidator/algorithms/Validator');

const TracerEvent   = require('../../../events/TracerEvent');

const schema    = new JobSchema();
const validator = new Validator(schema);

class Job {
  // private
  #traceOnFail(id,result) {
    TracerEvent.trace({ jobId: this.id, class: 'Job', function: 'validate', status: 'failure', message: result.errors.join(', ') });
    throw new Error(`[Job] Invalid job: ${result.errors.join(', ')}`);
  }

  #traceOnSunccess(id) {
    TracerEvent.trace({ jobId: this.id, class: 'Job', function: 'validate', status: 'success' });
  }

  // public
  constructor(job) {
    this.build(job);
    this.validate();
  }

  build(job) {
    // this.id              = buildId({exchange : job.EXCHANGE, contract : job.CONTRACT, createdAt : Date.now()});
    // this.createdAt       = Date.now();
    // this.exchange        = job.EXCHANGE;
    // this.contract        = job.CONTRACT;
    // | elapsedMinutes | status | contract | exchange |
    //                    2 bits    2 bits     2 bits
    
    this.id              = TradeIdBuilder.build(job);
    this.page_url        = job.PAGE_URL;
    this.api_url         = job.API_URL;
  
    let apiUrlBuilder = job.API_URL_BUILDER;
    if(apiUrlBuilder)
      this.api_url_builder = apiUrlBuilder;
  }
  validate() {
    const data = {
      id       : this.id,
      exchange : this.exchange,
      page_url : this.page_url,
      api_url  : this.api_url,
      contract : this.contract
    };

    if (this.api_url_builder) data.api_url_builder = this.api_url_builder;

    const result = validator.validate(data);

    if (!result.valid) {
      this.#traceOnFail(this.id,result);
    }
    this.#traceOnSunccess(this.id);
  }

}

module.exports = { Job };