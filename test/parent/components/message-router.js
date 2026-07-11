// parent/components/message-router.js
const { EventEmitter } = require('events');

class MessageRouter extends EventEmitter {
  constructor() {
    super();
  }

  route(message, processInfo, handlers = {}) {
    if (!message || !message.type) return;

    const handler = handlers[message.type];
    if (handler) {
      handler(message, processInfo);
    }

    this.emit('message', processInfo, message);
  }
}

module.exports = MessageRouter;