// JobSchema.js
// Defines the expected shape of a job payload.
// All fields are required strings.

class JobSchema {
  getJsonSchema() {
    return {
      type      : 'object',
      properties: {
        exchange        : { type: 'string' },
        page_url        : { type: 'string' },
        api_url         : { type: 'string' },
        api_url_builder : { type: 'string' },
        referer         : { type: 'string' },
      },
      required            : ['exchange', 'page_url', 'api_url', 'referer'],
      additionalProperties: false,
    };
  }
}

module.exports = { JobSchema };