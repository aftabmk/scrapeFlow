const { Graph }           = require  ('./models/graph');
const { TopologicalSort } = require  ('./algorithms/topologicalSort');

const graph = new Graph();

// uild pipeline dependency graph
graph.addEdge('env',    'config');
graph.addEdge('config', 'db');
graph.addEdge('config', 'cache');
graph.addEdge('db',     'server');
graph.addEdge('cache',  'server');
graph.addEdge('server', 'routes');

console.log('Kahn:', TopologicalSort.kahn(graph));
console.log('DFS: ', TopologicalSort.dfs(graph));