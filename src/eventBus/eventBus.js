'use strict';

const EventEmitter = require('events');

class EventBus extends EventEmitter {
  constructor() {
    super();

    this.setMaxListeners(10);
  }


  publish(event, data) {
    this.emit(event, { event, data, timestamp: new Date().toISOString() });
  }


  subscribe(event, handler) {
    this.on(event, handler);
  }


  unsubscribe(event, handler) {
    this.off(event, handler);
  }
}

// Singleton — one bus for the entire process
const eventBus = new EventBus();

module.exports = eventBus;