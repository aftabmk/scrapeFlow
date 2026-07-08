'use strict';

class Calculator {
  constructor(_options = {}) {}

  async run(payload) {
    const { op, args } = payload;

    switch (op) {
      case 'add':
        return this.add(...args);
      default:
        throw new Error(`Calculator: unknown op "${op}"`);
    }
  }

  add(a, b) {
    return a + b;
  }
}

module.exports = Calculator;