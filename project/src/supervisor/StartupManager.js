// supervisor/StartupManager.js
const { fork } = require('child_process');
const Logger = require('./Logger.js');
const { TopologicalSort } = require('../../algorithms/TopologicalSort/algorithms/topologicalSort.js');

class StartupManager {
  constructor(specs) {
    this.specs = specs;
  }

  resolveStartOrder() {
    const graph = new TopologicalSort();

    this.specs.forEach(spec => {
      graph.addNode?.(spec.name);
    });

    this.specs.forEach(spec => {
      const deps = Array.isArray(spec.dependsOn)
        ? spec.dependsOn
        : (spec.dependsOn ? [spec.dependsOn] : []);

      deps.forEach(dep => {
        graph.addEdge(dep, spec.name);
      });
    });

    return graph.kahn();
  }

  waitForReady(proc, name) {
    return new Promise((resolve, reject) => {
      const onMessage = (msg) => {
        if (msg.type === 'log') return Logger.log(name, msg);
        if (msg.type === 'error') return Logger.error(name, msg);

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

  // Forks and starts every process in dependency order.
  // Returns a { name: ChildProcess } map once everything is ready.
  async startAll() {
    const sortedNames = this.resolveStartOrder();
    console.log('[Supervisor] start order:', sortedNames);

    const procs = {};

    for (const name of sortedNames) {
      const spec = this.specs.find(s => s.name === name);
      if (!spec) continue;

      const proc = fork(spec.file);
      console.log(`[Supervisor] forked ${name} as PID ${proc.pid}`);

      await this.waitForReady(proc, name);

      procs[name] = proc;
      console.log(`[Supervisor] ${name} ready`);
    }

    return procs;
  }
}

module.exports = StartupManager;