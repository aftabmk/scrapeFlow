// A doubly linked list that holds all ListNodes sharing the same access frequency.
const { ListNode } = require( './ListNode.js' );

class FreqList {
  constructor() {
    // Sentinels — never evicted, never returned
    this.head = new ListNode(null, null, -1);
    this.tail = new ListNode(null, null, -1);
    this.head.next = this.tail;
    this.tail.prev = this.head;
    this.size = 0;
  }

  /**
   * Insert node at the front (MRU position).
   */
  addFront(node) {
    node.next          = this.head.next;
    node.prev          = this.head;
    this.head.next.prev = node;
    this.head.next     = node;
    this.size++;
  }

  /**
   * Unlink an arbitrary node from this list.
   */
  remove(node) {
    node.prev.next = node.next;
    node.next.prev = node.prev;
    node.prev = null;
    node.next = null;
    this.size--;
  }

  /**
   * Remove and return the LRU node (tail.prev).
   */
  removeLast() {
    if (this.size === 0) return null;
    const node = this.tail.prev;
    this.remove(node);
    return node;
  }

  /**
   * Snapshot: returns all nodes MRU → LRU (excludes sentinels).
   */
  toArray() {
    const arr = [];
    let cur = this.head.next;
    while (cur !== this.tail) {
      arr.push(cur);
      cur = cur.next;
    }
    return arr;
  }

  isEmpty() {
    return this.size === 0;
  }
}

module.exports = { FreqList };