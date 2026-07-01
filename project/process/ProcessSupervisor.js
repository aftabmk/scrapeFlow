// ProcessSupervisor.js
const { fork } = require('child_process');
const { Graph } = require('../algorithms/TopologicalSort/models/graph.js');
const { TopologicalSort } = require('../algorithms/TopologicalSort/algorithms/topologicalSort.js');

class ProcessSupervisor {
  constructor(specs) {
    this.specs = specs;
    this.procs = {};
  }

  resolveStartOrder() {
    const graph = new Graph();

    // make sure every process appears as a node, even with no deps
    this.specs.forEach(spec => {
      graph.addNode?.(spec.name); // safe no-op if Graph has no addNode method
    });

    this.specs.forEach(spec => {
      const deps = Array.isArray(spec.dependsOn)
        ? spec.dependsOn
        : (spec.dependsOn ? [spec.dependsOn] : []);

      deps.forEach(dep => {
        // dep must start before spec.name -> edge dep -> spec.name
        graph.addEdge(dep, spec.name);
      });
    });

    return TopologicalSort.kahn(graph);
  }

  async startInOrder(sortedNames) {
    for (const name of sortedNames) {
      const spec = this.specs.find(s => s.name === name);
      if (!spec) continue; // guards against nodes with no matching spec, if any

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

    this.procs.jobs.send({ type: 'start', payload: eventPayload });
    this.startPolling();
  }
}

module.exports = ProcessSupervisor;