const TracerEvent = require('../../events/tracerEvent');

class TracerClass {
  constructor() {
    TracerEvent.subscribe(
      (payload) => this._onTrace(payload),
      (payload) => this._onWarn(payload),
    );
  }

  _format(parts) {
    return parts.join(' -> ');
  }

  _onTrace({ key, chain }) {
    console.log(`[TRACE] [${key}]`, this._format(chain));
  }

  _onWarn({ key, chain, message }) {
    console.warn(`[WARN]  [${key}]`, this._format(chain), '|', message);
  }
}

module.exports = TracerClass;