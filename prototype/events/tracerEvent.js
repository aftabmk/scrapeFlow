const eventBus = require('../eventBus');

const TracerEvent = {
  TRACE: 'tracer:trace',
  WARN:  'tracer:warn',

  trace(key, chain) {
    eventBus.emit(TracerEvent.TRACE, { key, chain });
  },

  warn(key, chain, message) {
    eventBus.emit(TracerEvent.WARN, { key, chain, message });
  },

  subscribe(onTrace, onWarn) {
    eventBus.on(TracerEvent.TRACE, onTrace);
    eventBus.on(TracerEvent.WARN,  onWarn);
  },
};

module.exports = TracerEvent;