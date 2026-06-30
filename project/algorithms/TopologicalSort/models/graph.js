class Graph {
  constructor() {
    this.adjacency = new Map();  // node → [neighbors]
    this.inDegree  = new Map();  // node → count of incoming edges
  }

  addNode(node) {
    if (!this.adjacency.has(node)) {
      this.adjacency.set(node, []);
      this.inDegree.set(node, 0);
    }
  }

  addEdge(from, to) {
    this.addNode(from);
    this.addNode(to);
    this.adjacency.get(from).push(to);
    this.inDegree.set(to, this.inDegree.get(to) + 1);
  }

  getNeighbors(node) {
    return this.adjacency.get(node) ?? [];
  }

  getNodes() {
    return [...this.adjacency.keys()];
  }
}

module.exports = { Graph };