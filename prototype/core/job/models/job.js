// Job.js

const { JobSchema } = require('./JobSchema');
const { Validator } = require('../../../algorithms/jsonValidator/algorithms/Validator');
const TracerEvent   = require('../../../events/TracerEvent');

const schema    = new JobSchema();
const validator = new Validator(schema);

class Job {
  // private
  #traceOnFail(id,result) {
    TracerEvent.trace({ jobId: this.id, class: 'Job', function: '_validate', status: 'failure', message: result.errors.join(', ') });
    throw new Error(`[Job] Invalid job: ${result.errors.join(', ')}`);
  }

  #traceOnSunccess(id) {
    TracerEvent.trace({ jobId: this.id, class: 'Job', function: '_validate', status: 'success' });
  }

  // public
  constructor(id) {
    this.id              = id;
    this.createdAt       = Date.now();
    this.exchange        = process.env[`EXCHANGE_${id}`];
    this.page_url        = process.env[`PAGE_URL_${id}`];
    this.api_url         = process.env[`API_URL_${id}`];
    this.referer         = process.env[`REFERER_${id}`];
    this.contract        = process.env[`CONTRACT_${id}`];
    this.api_url_builder = process.env[`API_URL_BUILDER_${id}`] ?? null;
    
    this._validate();
  }
  
  _validate() {
    const data = {
      exchange : this.exchange,
      page_url : this.page_url,
      api_url  : this.api_url,
      referer  : this.referer,
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