// TopologicalSort.js
const { Graph } = require('../models/graph');

class TopologicalSort extends Graph {
  constructor() { super(); }

  kahn() {
    const inDegree = this.inDegree; 
    const queue    = [];
    const result   = [];

    for (const [node, deg] of inDegree) {
      if (deg === 0) queue.push(node);
    }

    while (queue.length) {
      const node = queue.shift();
      result.push(node);

      for (const neighbor of this.getNeighbors(node)) {
        inDegree.set(neighbor, inDegree.get(neighbor) - 1);
        if (inDegree.get(neighbor) === 0) queue.push(neighbor);
      }
    }

    if (result.length !== this.getNodes().length) {
      throw new Error('Cycle detected — topological sort not possible');
    }

    return result;
  }
}

module.exports = { TopologicalSort };