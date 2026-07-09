'use strict';

const path = require('path');
const ChildProcess = require('../core/ChildProcess');

new ChildProcess({
  name: 'calculator',
  handlerPath: path.join(__dirname,'..','class', 'Calculator.js'),
  concurrency: 2,
  forwardTo: ['tracer'], // e.g. ['formatter'] once a downstream child exists
});