// ProcessSupervisor.js  (only the `start` method changed — rest stays identical)
const { fork } = require('child_process');
const TopoSort = require('./TopoSort');

class ProcessSupervisor {
  constructor(specs) {
    this.specs = specs;
    this.procs = {};
  }

  resolveStartOrder() {
    const sorter = new TopoSort();
    this.specs.forEach(spec => sorter.addNode(spec.name, spec.dependsOn));
    return sorter.sort();
  }

  async startInOrder(sortedNames) {
    for (const name of sortedNames) {
      const spec = this.specs.find(s => s.name === name);
      const proc = fork(spec.file);

      await new Promise((resolve) => {
        proc.once('message', (msg) => {
          if (msg.type === 'ready') resolve();
        });
      });

      this.procs[name] = proc;
      console.log(`[Supervisor] ${name} ready`);
    }
  }

  wireProcesses() {
    const { jobs, queue, browser } = this.procs;

    jobs.on('message', (msg) => {
      if (msg.type === 'enqueue') queue.send(msg);
    });

    browser.on('message', (msg) => {
      if (['dequeue-request', 'ack', 'nack'].includes(msg.type)) {
        queue.send(msg);
      }
    });

    queue.on('message', (msg) => {
      if (msg.type === 'dequeue-response') browser.send(msg);
      if (msg.type === 'ack-confirm') jobs.send(msg);
    });
  }

  startPolling(intervalMs = 5000) {
    setInterval(() => {
      this.procs.browser.send({ type: 'trigger-poll' });
    }, intervalMs);
  }

  async start(eventPayload) {
    const sortedNames = this.resolveStartOrder();
    console.log('[Supervisor] start order:', sortedNames);

    await this.startInOrder(sortedNames);
    this.wireProcesses();

    // pass the lambda-style payload into the jobs process
    this.procs.jobs.send({ type: 'start', payload: eventPayload });
    this.startPolling();
  }
}

module.exports = ProcessSupervisor;