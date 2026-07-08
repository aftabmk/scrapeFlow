'use strict';

const Tracer = require('./Tracer');

class ProcessBase {
  constructor({ name } = {}) {
    this.name = name || `proc-${process.pid}`;
    this.tracer = new Tracer();
    this.ready = false;

    this._bindIpc();
  }

  _bindIpc() {
    process.on('message', (msg) => {
      this.onMessage(msg);
    });

    process.on('SIGTERM', () => this.shutdown('SIGTERM'));
    process.on('SIGINT', () => this.shutdown('SIGINT'));
  }

  onMessage(_msg) {
    throw new Error('onMessage must be implemented by subclass');
  }

  signalReady() {
    this.ready = true;
    if (process.send) {
      process.send({ type: 'ready', from: this.name });
    }
  }

  send(msg) {
    if (process.send) {
      process.send(msg);
    }
  }

  async shutdown(signal) {
    process.exit(0);
  }
}

module.exports = ProcessBase;