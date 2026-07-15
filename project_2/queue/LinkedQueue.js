// queue/LinkedQueue.js
const Node = require('./Node');

class LinkedQueue {
    constructor(name) {
        this.name = name;
        this.head = null;
        this.tail = null;
        this.pending = new Map();
        this.size = 0;
    }

    _pushBack(job) {
        const node = new Node(job);

        if (!this.head) {
            this.head = this.tail = node;
        } else {
            this.tail.next = node;
            node.prev = this.tail;
            this.tail = node;
        }

        this.pending.set(job.id, node);
        this.size++;
    }

    popFront() {
        if (!this.head) return null;

        const node = this.head;
        this.head = node.next;

        if (this.head) {
            this.head.prev = null;
        } else {
            this.tail = null;
        }

        this.pending.delete(node.job.id);
        this.size--;

        return node.job;
    }

    peek() {
        return this.head ? this.head.job : null;
    }

    getSize() {
        return this.size;
    }

    isEmpty() {
        return this.size === 0;
    }

    clear() {
        this.head = null;
        this.tail = null;
        this.pending.clear();
        this.size = 0;
    }

    toArray() {
        const result = [];
        let current = this.head;
        while (current) {
            result.push(current.job);
            current = current.next;
        }
        return result;
    }
}

module.exports = LinkedQueue;