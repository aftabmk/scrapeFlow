const { Graph } = require ('../models/graph.js');

class TopologicalSort {
  static kahn(graph) {
    const inDegree = new Map(graph.inDegree);  // local copy
    const queue    = [];
    const result   = [];

    // Seed queue with all zero-in-degree nodes
    for (const [node, deg] of inDegree) {
      if (deg === 0) queue.push(node);
    }

    while (queue.length) {
      const node = queue.shift();
      result.push(node);

      for (const neighbor of graph.getNeighbors(node)) {
        inDegree.set(neighbor, inDegree.get(neighbor) - 1);
        if (inDegree.get(neighbor) === 0) queue.push(neighbor);
      }
    }

    if (result.length !== graph.getNodes().length) {
      throw new Error('Cycle detected — topological sort not possible');
    }

    return result;
  }

  static dfs(graph) {
    const visited  = new Set();
    const onStack  = new Set();  // cycle detection
    const result   = [];

    const visit = (node) => {
      if (onStack.has(node)) throw new Error('Cycle detected');
      if (visited.has(node)) return;

      onStack.add(node);
      for (const neighbor of graph.getNeighbors(node)) {
        visit(neighbor);
      }
      onStack.delete(node);
      visited.add(node);
      result.push(node);   // postorder: push after all descendants
    };

    for (const node of graph.getNodes()) {
      visit(node);
    }

    return result.reverse();  // reverse postorder = topological order
  }
}

module.exports = { TopologicalSort };