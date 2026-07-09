'use strict';

const path = require('path');
const crypto = require('crypto');
const Parent = require('./parent');

const app = new Parent({ name: 'parent' });

app.registerChild('tracer', path.join(__dirname,'children','tracer.js'));
app.registerChild('calculator', path.join(__dirname,'children','calculator.js'));

const job = { id: crypto.randomUUID(), op: 'add', args: [2, 3] };

setTimeout(() => {
  app.routeJobTo('calculator', job);
}, 300);