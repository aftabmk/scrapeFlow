const eventBus = require('./eventBus');

const ScraperEvent = {
  SCRAPE_TRIGGERED: 'scraper:triggered',

  emit(scrapePayload) {
    eventBus.emit(ScraperEvent.SCRAPE_TRIGGERED, scrapePayload);
  },

  subscribe(handler) {
    eventBus.on(ScraperEvent.SCRAPE_TRIGGERED, handler);
  },
};

module.exports = ScraperEvent;