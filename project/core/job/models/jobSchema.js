class JobSchema {
  getJsonSchema() {
    return {
      type      : 'object',
      properties: {
        // exchange        : { type: 'string' },
        // contract        : { type: 'string' },
        id              : {type : 'number' },
        page_url        : { type: 'string' },
        api_url         : { type: 'string' },
        api_url_builder : { type: 'string' },
      },
      // required            : ['exchange', 'page_url', 'api_url', 'contract'],
      required            : ['id', 'page_url', 'api_url'],
      additionalProperties: true,
    };
  }
}

module.exports = { JobSchema };