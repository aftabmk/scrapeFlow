const { LFUStorage } = require('./lfuStorage');


class LFUMaintenance extends LFUStorage {
  delete(name) {
    const entry = this._keyMap.get(name);
    if (!entry) return null;

    const bucket = this._freqMap.get(entry.freq);
    if (bucket) {
      bucket.delete(name);
      if (bucket.size === 0) this._freqMap.delete(entry.freq);
    }
    this._keyMap.delete(name);

    // Recalculate minFreq if the deleted item was the last of the min frequency
    if (this._keyMap.size > 0 && !this._freqMap.has(this._minFreq)) {
      this._minFreq = Math.min(...this._freqMap.keys());
    }

    return entry.value;
  }

  clear() {
    this._keyMap.clear();
    this._freqMap.clear();
    this._minFreq = 0;
  }

  entries() {
    return Array.from(this._keyMap.entries()).map(([name, entry]) => ({
      name,
      tab: entry.value,
      freq: entry.freq,
    }));
  }
}

module.exports = { LFUMaintenance };