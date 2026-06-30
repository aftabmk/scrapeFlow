// index.js

const { Graph }           = require ('./models/Graph.js');
const { TopologicalSort } = require ('./algorithms/TopologicalSort.js');

const graph = new Graph();

// Example: build pipeline dependency graph
graph.addEdge('env',    'config');
graph.addEdge('config', 'db');
graph.addEdge('config', 'cache');
graph.addEdge('db',     'server');
graph.addEdge('cache',  'server');
graph.addEdge('server', 'routes');

const order = TopologicalSort.kahn(graph);
console.log('Topological order:', order);