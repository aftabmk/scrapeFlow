// ProcessSupervisor.js
const Logger = require('./supervisor/Logger');
const MessageRouter = require('./supervisor/MessageRouter');
const StartupManager = require('./supervisor/StartupManager');

class ProcessSupervisor {
  constructor(specs) {
    this.specs = specs;
    this.procs = {};

    this.messageRouter = new MessageRouter();
    this.startupManager = new StartupManager(specs);
  }

  async startInOrder() {
    this.procs = await this.startupManager.startAll();
  }

  wireProcesses() {
    // persistent log/error forwarding, after startup's own listener is torn down
    Object.entries(this.procs).forEach(([name, proc]) => Logger.attach(proc, name));

    this.messageRouter.wire(this.procs);
  }

  startPolling(intervalMs = 5000) {
    setInterval(() => {
      this.procs.browser.send({ type: 'trigger-poll' });
    }, intervalMs);
  }

  async start(eventPayload) {
    await this.startInOrder();
    this.wireProcesses();

    this.procs.jobs.send({ type: 'start', payload: eventPayload });
    this.startPolling();
  }
}

module.exports = ProcessSupervisor;