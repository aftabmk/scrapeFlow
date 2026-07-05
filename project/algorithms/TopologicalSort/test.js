const { TopologicalSort } = require ('./algorithms/TopologicalSort.js');

const Graph = new TopologicalSort();

// Example: build pipeline dependency graph
Graph.addEdge('env',    'config');
Graph.addEdge('config', 'db');
Graph.addEdge('config', 'cache');
Graph.addEdge('db',     'server');
Graph.addEdge('cache',  'server');
Graph.addEdge('server', 'routes');

const order = Graph.kahn();
console.log('Topological order:', order);