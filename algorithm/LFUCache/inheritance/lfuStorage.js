class LFUStorage {
  constructor(capacity) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError('LFUCache: capacity must be a positive integer');
    }
    this.capacity = capacity;
    this._keyMap = new Map();
    this._freqMap = new Map();
    this._minFreq = 0;
  }

  get size() {
    return this._keyMap.size;
  }

  has(name) {
    return this._keyMap.has(name);
  }
}

module.exports = { LFUStorage };