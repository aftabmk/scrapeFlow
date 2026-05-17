// Job.js

const { JobSchema } = require('./JobSchema');
const { Validator } = require('../../../algorithms/jsonValidator/algorithms/Validator');

const schema    = new JobSchema();
const validator = new Validator(schema);  // compiled once at module load

class Job {
  constructor(id) {
    this.exchange        = process.env[`EXCHANGE_${id}`];
    this.page_url        = process.env[`PAGE_URL_${id}`];
    this.api_url         = process.env[`API_URL_${id}`];
    this.api_url_builder = process.env[`API_URL_BUILDER_${id}`] ?? null;
    this.referer         = process.env[`REFERER_${id}`];
    this.createdAt       = Date.now();

    this._validate();
  }

  _validate() {
    const data = {
      exchange : this.exchange,
      page_url : this.page_url,
      api_url  : this.api_url,
      referer  : this.referer,
    };

    if (this.api_url_builder) data.api_url_builder = this.api_url_builder;

    const result = validator.validate(data);
    if (!result.valid) {
      throw new Error(`[Job] Invalid job: ${result.errors.join(', ')}`);
    }
  }
}

module.exports = { Job };