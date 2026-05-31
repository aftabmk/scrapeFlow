const GOTO_TIMEOUT_MS = 30_000;

class Evaluator {
  constructor(page) {
    this.page = page;
    this.session = null;
  }

  async visit(url) {
    console.log(`[Evaluator] navigating to ${url}`);

    await this.page.goto(url, {
      waitUntil: 'networkidle2',
      timeout: GOTO_TIMEOUT_MS,
    });

    const cookies = await this.page.cookies();
    const userAgent = await this.page.evaluate(() => navigator.userAgent);

    this.session = {
      cookieString: cookies.map(c => `${c.name}=${c.value}`).join('; '),
      userAgent,
      referer: this.page.url(),
      origin: new URL(this.page.url()).origin,
    };
  }

  async fetch(apiUrl) {
    if (!this.session) {
      throw new Error('visit() must be called first');
    }

    return this.page.evaluate(
      async (url, session) => {
        const res = await fetch(url, {
          method: 'GET',
          headers: {
            Cookie: session.cookieString,
            Referer: session.referer,
            Origin: session.origin,
            'User-Agent': session.userAgent,
          },
        });

        const text = await res.text();

        return {
          ok: res.ok,
          status: res.status,
          body: text,
        };
      },
      apiUrl,
      this.session
    );
  }

  reset() {
    this.session = null;
  }
}

module.exports = Evaluator;