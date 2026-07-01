// TopologicalSort.js 

class TopologicalSort {

  static kahn(graph) {
    const inDegree = new Map(graph.inDegree);  
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
}

module.exports = { TopologicalSort };