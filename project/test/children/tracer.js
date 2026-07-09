'use strict';

const path = require('path');
const ChildProcess = require('../core/ChildProcess');

new ChildProcess({
  name: 'tracer',
  handlerPath: path.join(__dirname,'..','class', 'Tracer.js'),
  concurrency: 1, // sequential writes keep trace ordering sane
  forwardTo: [], // tracer is terminal, no further hops
});