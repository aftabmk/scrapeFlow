class SessionManager {
  constructor() {
    this.session = null;
  }

  initialize(event) {
    this.session = {
      id      : event.id,
      exchange: event.exchange,
      contract: event.contract,

      pageUrl: event.page_url,
      apiUrl: event.api_url,
      apiUrlBuilder: event.api_url_builder,

      referer: window.location.href,
      origin: window.location.origin,

      userAgent: navigator.userAgent,
      cookieString: document.cookie,
    };

    return this.session;
  }

  async fetch(url = this.session.apiUrl) {
    try {
      const response = await fetch(url, {
        headers: {
          Origin: this.session?.origin,
          Cookie: this.session?.cookieString,
          Referer: this.session?.referer,
          'User-Agent': this.session?.userAgent,
        },
      });

      const body = await response.json();
      return new window.fetchResponse(this.session,response,body);
    } 
    catch (error) {
      console.log(error);
      return new window.fetchError(this.session,error?.status ?? 0,error.message);
    }
  }

  resetSession() {
    this.session = null;
  }
}

module.exports = SessionManager;