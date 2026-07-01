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

  waitForReady(proc, name) {
    return new Promise((resolve, reject) => {
      const onMessage = (msg) => {
        if (msg.type === 'log') {
          console.log(`[${msg.source ?? name}]`, msg.message, msg.port ?? '');
          return;
        }

        if (msg.type === 'error') {
          console.error(`[${msg.source ?? name}]`, msg.message, msg.error ?? '');
          return;
        }

        if (msg.type === 'ready') {
          cleanup();
          resolve();
        }
      };

      const onExit = (code) => {
        cleanup();
        reject(new Error(`[Supervisor] ${name} exited with code ${code} before signaling ready`));
      };

      const onError = (err) => {
        cleanup();
        reject(new Error(`[Supervisor] ${name} failed to start: ${err.message}`));
      };

      const cleanup = () => {
        proc.off('message', onMessage);
        proc.off('exit', onExit);
        proc.off('error', onError);
      };

      proc.on('message', onMessage);
      proc.once('exit', onExit);
      proc.once('error', onError);
    });
  }

  async startInOrder(sortedNames) {
    for (const name of sortedNames) {
      const spec = this.specs.find(s => s.name === name);
      if (!spec) continue; // guards against nodes with no matching spec, if any

      const proc = fork(spec.file);
      // Each runs on distinct pid, 
      // browser @4180, walServer @4792,durabelQueue @11364,jobs @3696
      // four distinct PIDs, confirming four separate OS processes genuinely running concurrently, 
      // each capable of being scheduled on its own core.
      console.log(`[Supervisor] forked ${name} as PID ${proc.pid}`);

      await this.waitForReady(proc, name);

      this.procs[name] = proc;
      console.log(`[Supervisor] ${name} ready`);
    }
  }

  attachLogging(proc, name) {
    proc.on('message', (msg) => {
      if (msg.type === 'log') {
        console.log(`[${msg.source ?? name}]`, msg.message, msg.port ?? '');
      }
      if (msg.type === 'error') {
        console.error(`[${msg.source ?? name}]`, msg.message, msg.error ?? '');
      }
    });
  }

  wireProcesses() {
    const { jobs, queue, browser, walServer } = this.procs;

    // keep forwarding log/error messages after startup too,
    // since waitForReady's listener is removed once 'ready' fires
    Object.entries(this.procs).forEach(([name, proc]) => this.attachLogging(proc, name));

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

      // relay WAL-bound writes/recovery requests from queue -> walServer
      if (walServer && ['wal-write', 'recover'].includes(msg.type)) {
        walServer.send(msg);
      }
    });

    if (walServer) {
      walServer.on('message', (msg) => {
        // relay WAL responses back to whichever process is waiting on them
        if (['recover-response', 'write-ack'].includes(msg.type)) {
          queue.send(msg);
        }
      });
    }
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