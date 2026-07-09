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

  routeJobTo(childName, job) {
    const childHandle = this.children.get(childName);
    if (!childHandle) {
      throw new Error(`No such child: ${childName}`);
    }
    if (!job || !job.id) {
      throw new Error('routeJobTo: job must have an id');
    }
    childHandle.send({ type: 'job:process', job });
  }

  onMessage(msg, fromChildName) {
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'job:forward') {
      // same job.id preserved across the hop, new payload as args
      const nextJob = { ...msg.job, args: [msg.data] };
      this.routeJobTo(msg.to, nextJob);
      return;
    }

    if (msg.type === 'job:result') {
      return; // subclass handles final results
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