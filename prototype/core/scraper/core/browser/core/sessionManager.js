class SessionManager {
  constructor() {
    this.session = null;
  }

  initialize(event) {
    this.session = {
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

  getCookies() {
    return this.session?.cookieString;
  }

  getUserAgent() {
    return this.session?.userAgent;
  }

  getReferer() {
    return this.session?.referer;
  }

  getOrigin() {
    return this.session?.origin;
  }

  async fetch(url = this.session.apiUrl) {
    try {
      const response = await fetch(url, {
        headers: {
          Origin: this.getOrigin(),
          Cookie: this.getCookies(),
          Referer: this.getReferer(),
          'User-Agent': this.getUserAgent(),
        },
      });

      return {
        ok: response.ok,
        status: response.status,
        body : response.json(),
      };
    } catch (error) {
      return {
        ok: false,
        status: 0,
        error: error.message,
        body: null,
      };
    }
  }

  resetSession() {
    this.session = null;
  }
}

module.exports = SessionManager;