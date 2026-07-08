'use strict';

const { fork } = require('child_process');
const ProcessBase = require('./ProcessBase');

class ParentProcess extends ProcessBase {
  constructor(opts = {}) {
    super({ name: opts.name || 'parent' });

    this.children = new Map();
  }

  registerChild(name, scriptPath, forkArgs = []) {
    const childHandle = fork(scriptPath, forkArgs);

    childHandle.on('message', (msg) => this.onMessage(msg, name));
    this.children.set(name, childHandle);

    return childHandle;
  }

  routeJobTo(childName, traceId, job) {
    const childHandle = this.children.get(childName);
    if (!childHandle) {
      throw new Error(`No such child: ${childName}`);
    }
    childHandle.send({ traceId, job });
  }

  onMessage(msg, fromChildName) {
    if (msg.trace) {
      this.tracer.append(fromChildName || msg.from, msg.trace);
    }

    if (msg.type === 'result') {
      return;
    }
  }

  async shutdown(signal) {
    for (const [, childHandle] of this.children) {
      childHandle.kill();
    }
    process.exit(0);
  }
}

module.exports = ParentProcess;