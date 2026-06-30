// A doubly linked list node that holds a single cache entry.

class ListNode {
  constructor(key, val, freq = 1) {
    this.key  = key;
    this.val  = val;
    this.freq = freq;
	// pointer to previous node in FreqList
    this.prev = null;  
	// pointer to next node in FreqList
    this.next = null;  
  }
}

module.exports = { ListNode };