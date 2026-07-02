// main.js
const eventPayload = require('./event.json');
const { NODES, PATH, DEPENDENCY } = require('./utils/enums');
const ProcessSupervisor = require('./src/ProcessSupervisor');

const processSpecs = [
  { name: NODES.JOBS,     file: PATH.JOBS,     dependsOn: DEPENDENCY.JOBS },
  { name: NODES.QUEUE,    file: PATH.QUEUE,    dependsOn: DEPENDENCY.QUEUE },
  { name: NODES.BROWSER,  file: PATH.BROWSER,  dependsOn: DEPENDENCY.BROWSER },
  { name: NODES.WALSERVER,file: PATH.WALSERVER,dependsOn: DEPENDENCY.WALSERVER },
];

const supervisor = new ProcessSupervisor(processSpecs);

supervisor.start(eventPayload).catch(err => {
  console.error('[main] failed to start:', err);
  process.exit(1);
});