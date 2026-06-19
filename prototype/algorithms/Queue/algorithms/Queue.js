const BaseQueue = require("./BaseQueue");

class Queue extends BaseQueue {
  async receiveMessages() {
    const deadline = Date.now() + this.waitTimeMs;

    while (this.pending.length === 0 && Date.now() < deadline) {
      await this.sleep(100);
    }

    const messages = [];

    while (messages.length < this.maxMessages && this.pending.length > 0) {
      const id = this.pending.shift();
      const event = this.store.get(id);

      if (!event) continue;

      const timer = setTimeout(() => this.timeout(id), this.visibilityTimeout);

      this.inFlight.set(id, timer);

      messages.push({
        ReceiptHandle: id,
        Body: JSON.stringify(event),
      });
    }

    return messages;
  }

  async deleteMessage(receiptHandle) {
    this.ack(receiptHandle);
  }

  size() {
    return this.pending.length + this.inFlight.size;
  }
}

module.exports = Queue;