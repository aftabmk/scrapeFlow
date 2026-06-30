const Node = require("./Node");

class LinkedQueue {

    constructor(name) {
        this.name = name;

        this.head = null;
        this.tail = null;

        this.pending = new Map();
    }

    _pushBack(job) {
        const node = new Node(job);

        if (!this.head)
            this.head = this.tail = node;
        else {
            this.tail.next = node;
            node.prev = this.tail;
            this.tail = node;
        }

        this.pending.set(job.id, node);
    }

    popFront() {
        if (!this.head)
            return null;

        const node = this.head;

        this.head = node.next;

        if (this.head)
            this.head.prev = null;
        else
            this.tail = null;

        this.pending.delete(node.job.id);

        return node.job;
    }

    size() {
        return this.pending.size;
    }

    empty() {
        return this.pending.size === 0;
    }
}

module.exports = LinkedQueue;