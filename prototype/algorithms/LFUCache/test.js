const { LFUCache } = require('./algorithms/LFUCahche');

const cache = new LFUCache(3);

cache.set('A', 1);
cache.set('B', 1);
cache.set('C', 1);
cache.set('A', 2);
console.log(cache.get('A'));
cache.set('D', 5);
cache.set('E', 5);
console.log(cache.get('B'));
console.log(cache.get('C'));
console.log(cache.get('D'));