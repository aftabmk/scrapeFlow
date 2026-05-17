class JobSchema {
  getJsonSchema() {
    return {
      type      : 'object',
      properties: {
        exchange        : { type: 'string' },
        contract        : { type: 'string' },
        page_url        : { type: 'string' },
        api_url         : { type: 'string' },
        api_url_builder : { type: 'string' },
        referer         : { type: 'string' },
      },
      required            : ['exchange', 'page_url', 'api_url', 'referer', 'contract'],
      additionalProperties: false,
    };
  }
}

module.exports = { JobSchema };