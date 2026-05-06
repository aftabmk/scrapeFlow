'use strict';

const { LFUMaintenance } = require('./inheritance/lfuMaintainance');

// Factory

class LFUCache extends LFUMaintenance {
  constructor(capacity) {
    super(capacity);
  }

  _incrementFreq(name) {
    const entry = this._keyMap.get(name);
    const oldFreq = entry.freq;
    const newFreq = oldFreq + 1;
    entry.freq = newFreq;

    const oldBucket = this._freqMap.get(oldFreq);
    oldBucket.delete(name);

    if (oldBucket.size === 0) {
      this._freqMap.delete(oldFreq);
      if (this._minFreq === oldFreq) this._minFreq = newFreq;
    }

    if (!this._freqMap.has(newFreq)) this._freqMap.set(newFreq, new Set());
    this._freqMap.get(newFreq).add(name);
  }

  _evictLFU() {
    const bucket = this._freqMap.get(this._minFreq);
    const name = bucket.values().next().value;
    
    bucket.delete(name);
    if (bucket.size === 0) this._freqMap.delete(this._minFreq);

    const entry = this._keyMap.get(name);
    this._keyMap.delete(name);

    return { name, tab: entry.value };
  }

  get(name) {
    if (!this._keyMap.has(name)) return null;
    this._incrementFreq(name);
    return this._keyMap.get(name).value;
  }

  put(name, value) {
    if (this._keyMap.has(name)) {
      this._keyMap.get(name).value = value;
      this._incrementFreq(name);
      return null;
    }

    let evicted = null;
    if (this._keyMap.size >= this.capacity) {
      evicted = this._evictLFU();
    }

    this._keyMap.set(name, { value, freq: 1 });
    if (!this._freqMap.has(1)) this._freqMap.set(1, new Set());
    this._freqMap.get(1).add(name);
    this._minFreq = 1;

    return evicted;
  }
}

module.exports = { LFUCache };