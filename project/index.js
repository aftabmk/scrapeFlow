// main.js
const eventPayload = require('./event.json');
const ProcessSupervisor = require('./process/ProcessSupervisor');

const processSpecs = [
  { name: 'browser',  file: './process/browserProcess.js', dependsOn: [] },
  // { name: 'walServer',file: './algorithms/sqlite/server/WALServer.js', dependsOn: [] },
  { name: 'jobs',     file: './process/jobsProcess.js',    dependsOn: ['queue'] },
  { name: 'queue',    file: './process/queueProcess.js',   dependsOn: ['browser'/*,'walServer'*/] },
];

const supervisor = new ProcessSupervisor(processSpecs);

supervisor.start(eventPayload).catch(err => {
  console.error('[main] failed to start:', err);
  process.exit(1);
});