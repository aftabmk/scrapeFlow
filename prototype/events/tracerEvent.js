// TracerEvent.js

const eventBus   = require('../eventBus');
const { TracerStore } = require('../core/tracer/tracerStore');

class TracerEvent {
  constructor() {
    this.TRACE = 'tracer:trace';
  }
 
  trace(payload) {
    const store = new TracerStore(payload);
    eventBus.emit(this.TRACE, TracerStore.traceAll());
  }
 
  subscribe(onTrace) {
    eventBus.on(this.TRACE, onTrace);
  }
}
 
module.exports = new TracerEvent();
 