// main.js
const ProcessSupervisor = require('./core/ProcessSupervisor');

const processSpecs = [
  { name: 'browser', file: './browserProcess.js', dependsOn: [] },
  { name: 'queue',   file: './queueProcess.js',   dependsOn: ['browser'] },
  { name: 'jobs',    file: './jobsProcess.js',    dependsOn: ['queue'] },
];

// lambda-style event payload, passed in at invocation time
const eventPayload = [
  { url: 'https://nseindia.com/page1', api: 'https://api.nse.com/data1' },
  { url: 'https://nseindia.com/page2', api: 'https://api.nse.com/data2' },
];

const supervisor = new ProcessSupervisor(processSpecs);

supervisor.start(eventPayload).catch(err => {
  console.error('[main] failed to start:', err);
  process.exit(1);
});