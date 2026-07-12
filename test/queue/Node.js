// queue/Node.js
class Node {
    constructor(job) {
        this.job = job;
        this.prev = null;
        this.next = null;
    }
}

module.exports = Node;