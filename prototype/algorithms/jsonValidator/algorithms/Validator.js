// Uses Ajv to compile the schema once and validate repeatedly.

const Ajv = require('ajv');
const { Schema } = require('../models/Schema.js');

class Validator {
  constructor(schema) {
    const ajv = new Ajv({ allErrors: true });
    this._validate = ajv.compile(schema.getJsonSchema());
  }

  validate(data) {
    const valid = this._validate(data);
    const errors = valid ? [] : this._validate.errors.map(e => `${e.instancePath || '(root)'} ${e.message}`);

    return { valid, errors };
  }
}

module.exports = { Validator };