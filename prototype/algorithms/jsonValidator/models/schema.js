// Schema.js
// Holds the JSON Schema definition compatible with Ajv.

class Schema {
  constructor(definition) {
    this.jsonSchema = this._build(definition);
  }

  _build(definition) {
    const properties = {};
    const required   = [];

    for (const [key, type] of Object.entries(definition)) {
      properties[key] = { type };
      required.push(key);
    }

    return {
      type: 'object',
      properties,
      required,
      additionalProperties: false,  // reject unknown keys
    };
  }

  getJsonSchema() {
    return this.jsonSchema;
  }
}

module.exports = { Schema };