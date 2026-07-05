const { WSWorkerManager } = require('./WSWorkerManager');
const { BrowserWorkerManager } = require('./BrowserWorkerManager');

const { TopologicalSort } = require('../../../../algorithms/TopologicalSort/algorithms/topologicalSort');

class PipelineRunner {
  static registry = {
    ensureWSChild: WSWorkerManager,
    ensureBrowserScrape: BrowserWorkerManager,
  };

  static registerManager(node, ManagerClass) {
    this.registry[node] = ManagerClass;
  }

  static buildOrder(definitions) {
    const byName = new Map(definitions.map((d) => [d.node, d]));
    const graph = new TopologicalSort();

    for (const def of definitions) {
      const deps = def.dependency || [];

      if (deps.length === 0) {
        graph.addNode ? graph.addNode(def.node) : graph.addEdge(def.node, def.node);
      }

      for (const dep of deps) {
        if (!byName.has(dep)) {
          throw new Error(`Unknown dependency '${dep}' referenced by '${def.node}'`);
        }
        graph.addEdge(dep, def.node);
      }
    }

    const order = graph.kahn();
    return order.map((name) => byName.get(name)).filter(Boolean);
  }

  static async run(definitions) {
    const sorted = this.buildOrder(definitions);
    const results = {};

    for (const { node, path: workerPath } of sorted) {
      const ManagerClass = this.registry[node];
      if (!ManagerClass) {
        throw new Error(`No worker manager registered for node '${node}'`);
      }
      // Monitoring is already wired up inside BaseWorkerManager.ensure() —
      // no need to call WorkerMonitor.watch() again here.
      results[node] = await ManagerClass.run(workerPath);
    }

    return results;
  }
}

module.exports = { PipelineRunner };