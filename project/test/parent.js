const ParentProcess = require('./core/ParentProcess');

class Parent extends ParentProcess {
  onMessage(msg, fromChildName) {
    super.onMessage(msg, fromChildName);

    if (msg.type === 'result') {
      console.log(`[${this.name}] result from "${fromChildName}":`, msg.data);
      console.log(`[${this.name}] trace for "${fromChildName}":`, this.tracer.getByChild(fromChildName));
    }
  }
}

module.exports = Parent;