class FetchError {
  constructor(session, status, error) {
    this.id       = session.id;
    this.exchange = session.exchange;
    this.contract = session.contract;
    this.status   = status;
    this.error    = error;
    this.body     = null;
  }
}

module.exports = FetchError;