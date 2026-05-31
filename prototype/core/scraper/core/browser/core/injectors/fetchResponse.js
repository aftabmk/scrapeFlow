class FetchResponse {
  constructor(session,response, body) {
    this.status = response.status;
    this.id = session.id,
    this.exchange = session.exchange,
    this.contract = session.contract,
    this.body = body;
  }
}

module.exports = FetchResponse;