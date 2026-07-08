'use strict';

const path = require('path');
const crypto = require('crypto');
const Parent = require('./parent');

const app = new Parent({ name: 'parent' });

app.registerChild('calculator', path.join(__dirname, 'child.js'));

const traceId = crypto.randomUUID();

setTimeout(() => {
  app.routeJobTo('calculator', traceId, { op: 'add', args: [10, 3] });
}, 300);