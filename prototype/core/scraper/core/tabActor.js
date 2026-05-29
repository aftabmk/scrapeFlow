const GOTO_TIMEOUT_MS = 30_000;

class TabActor {
  constructor() {
    this._healthTimer = null;
    this.page         = null;
    this.job          = null;
    this.session      = null;
  }

  async init() {
    throw new Error(`[${this.constructor.name}] init() must be implemented`);
  }

  async close() {
    throw new Error(`[${this.constructor.name}] close() must be implemented`);
  }

  async processJob(job) {
    throw new Error(`[${this.constructor.name}] processJob() must be implemented`);
  }

  async pageVisit(url) {
    try {
      console.log(`[${this.constructor.name}] navigating to ${url}`);
      await this.page.goto(url, {
        waitUntil: 'networkidle2',
        timeout:   GOTO_TIMEOUT_MS,
      });

      const cookies   = await this.page.cookies();
      const userAgent = await this.page.evaluate(() => navigator.userAgent);

      this.session = {
        cookieString: cookies.map(c => `${c.name}=${c.value}`).join('; '),
        userAgent,
        referer:      this.page.url(),
        origin:       new URL(this.page.url()).origin,
      };

      console.log(`[${this.constructor.name}] session captured`);
    } catch (err) {
      console.error(`[${this.constructor.name}] pageVisit failed: ${err.message}`);
      await this.close();
    }
  }

  async fetch(apiUrl) {
    if (!this.session) {
      console.error(`[${this.constructor.name}] no session — call pageVisit() first`);
      return;
    }

    try {
      console.log(`[${this.constructor.name}] fetching ${apiUrl}`);

      const result = await this.page.evaluate(async (url, session) => {
        const res = await fetch(url, {
          method:  'GET',
          headers: {
            'Cookie':          session.cookieString,
            'User-Agent':      session.userAgent,
            'Referer':         session.referer,
            'Origin':          session.origin,
            'Accept':          'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Fetch-Site':  'same-origin',
            'Sec-Fetch-Mode':  'cors',
            'Sec-Fetch-Dest':  'empty',
          },
        });

        if (!res.ok) {
          return { ok: false, status: res.status, body: await res.text() };
        }

        const data = await res.json();
        return { ok: true, status: res.status, data };
      }, apiUrl, this.session);

      if (result.ok) {
        console.log(`[${this.constructor.name}] fetch result:`, result.data);
        return result.data;
      } else {
        console.error(`[${this.constructor.name}] fetch blocked — status: ${result.status}`);
        console.error(result.body);
      }
    } catch (err) {
      console.error(`[${this.constructor.name}] fetch failed: ${err.message}`);
      await this.close();
    }
  }

  async healthCheck() {
    throw new Error(`[${this.constructor.name}] healthCheck() must be implemented`);
  }

  _startHealthCheck(intervalMs = 30_000) {
    this._healthTimer = setInterval(async () => {
      await this.healthCheck();
    }, intervalMs);
  }

  _stopHealthCheck() {
    if (this._healthTimer) {
      clearInterval(this._healthTimer);
      this._healthTimer = null;
    }
  }
}

module.exports = TabActor;