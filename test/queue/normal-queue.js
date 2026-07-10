// queue/normal-queue.js

/**
 * Node class for linked list
 */
class QueueNode {
  constructor(value) {
    this.value = value;
    this.next = null;
  }
}

/**
 * Normal Queue implementation using Linked List
 * O(1) enqueue and dequeue operations
 */
class NormalQueue {
  constructor(options = {}) {
    this.head = null;
    this.tail = null;
    this.length = 0;
    this.maxSize = options.maxSize || 10000;
    this.name = options.name || 'default';
    this.stats = {
      enqueued: 0,
      dequeued: 0,
      dropped: 0
    };
  }

  /**
   * Add item to the end of the queue - O(1)
   */
  enqueue(item) {
    if (this.length >= this.maxSize) {
      this.stats.dropped++;
      return false;
    }

    const node = new QueueNode(item);

    if (this.tail === null) {
      // Queue is empty
      this.head = node;
      this.tail = node;
    } else {
      // Add to the end
      this.tail.next = node;
      this.tail = node;
    }

    this.length++;
    this.stats.enqueued++;
    return true;
  }

  /**
   * Remove and return item from the front of the queue - O(1)
   */
  dequeue() {
    if (this.head === null) {
      return null;
    }

    const node = this.head;
    this.head = node.next;

    // If queue becomes empty, update tail
    if (this.head === null) {
      this.tail = null;
    }

    this.length--;
    this.stats.dequeued++;
    return node.value;
  }

  /**
   * Remove and return multiple items - O(n)
   */
  dequeueMultiple(count) {
    const items = [];
    const batchSize = Math.min(count, this.length);

    for (let i = 0; i < batchSize; i++) {
      const item = this.dequeue();
      if (item !== null) {
        items.push(item);
      } else {
        break;
      }
    }

    return items;
  }

  /**
   * Peek at the front item without removing - O(1)
   */
  peek() {
    return this.head ? this.head.value : null;
  }

  /**
   * Get current size - O(1)
   */
  size() {
    return this.length;
  }

  /**
   * Check if queue is empty - O(1)
   */
  isEmpty() {
    return this.length === 0;
  }

  /**
   * Clear all items - O(n)
   */
  clear() {
    this.head = null;
    this.tail = null;
    this.length = 0;
  }

  /**
   * Get all items as array (for debugging) - O(n)
   */
  toArray() {
    const result = [];
    let current = this.head;
    while (current !== null) {
      result.push(current.value);
      current = current.next;
    }
    return result;
  }

  /**
   * Get queue statistics
   */
  getStats() {
    return {
      ...this.stats,
      currentSize: this.length,
      name: this.name,
      maxSize: this.maxSize
    };
  }

  /**
   * Iterate over queue items (read-only)
   */
  [Symbol.iterator]() {
    let current = this.head;
    return {
      next: () => {
        if (current === null) {
          return { done: true };
        }
        const value = current.value;
        current = current.next;
        return { value, done: false };
      }
    };
  }
}

module.exports = NormalQueue;