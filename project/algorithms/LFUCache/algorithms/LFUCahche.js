// Data structures:
//   map     : Map<key, ListNode>     — direct node lookup
//   freqMap : Map<freq, FreqList>    — one doubly linked list per frequency
//   minFreq : number                 — current minimum frequency (for O(1) eviction)

const { ListNode }  = require ('../models/ListNode.js');
const { FreqList  } = require ('../models/FreqList.js');

class LFUCache {
  constructor(capacity) {
    if (capacity <= 0) throw new RangeError('capacity must be > 0');
    this.cap     = capacity;
    this.map     = new Map();   // key → ListNode
    this.freqMap = new Map();   // freq → FreqList
    this.minFreq = 0;
  }

  _getList(f) {
    if (!this.freqMap.has(f)) this.freqMap.set(f, new FreqList());
    return this.freqMap.get(f);
  }

  /**
   * Increment node.freq, move it to the next FreqList,
   * and update minFreq if the old bucket became empty.
   */
  _touch(node) {
    const oldFreq = node.freq;
    const oldList = this._getList(oldFreq);
    oldList.remove(node);

    // If the min-freq bucket is now empty, its minimum rises
    if (oldList.isEmpty() && oldFreq === this.minFreq) {
      this.minFreq++;
    }

    node.freq++;
    this._getList(node.freq).addFront(node);
  }

  get(key) {
    if (!this.map.has(key)) return -1;
    const node = this.map.get(key);
    this._touch(node);
    return node.val;
  }

  set(key, val) {
    if (this.map.has(key)) {
      // Update existing entry and refresh its frequency
      const node = this.map.get(key);
      node.val = val;
      this._touch(node);
      return;
    }

    // Evict if full
    if (this.map.size >= this.cap) {
      const minList = this._getList(this.minFreq);
      const evicted = minList.removeLast();   // LRU of the lowest-freq bucket
      if (evicted) this.map.delete(evicted.key);
    }

    // Insert new node at freq = 1
    const node = new ListNode(key, val, 1);
    this.map.set(key, node);
    this._getList(1).addFront(node);
    this.minFreq = 1;   // a fresh node is always the new minimum
  }

  get size() {
    return this.map.size;
  }

  clear() {
    this.map.clear();
    this.freqMap.clear();
    this.minFreq = 0;
  }
}

module.exports = { LFUCache };