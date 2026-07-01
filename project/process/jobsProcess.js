// jobsProcess.js
const { JobBuilder } = require('../core/job/models/jobBuilder');

process.send({ type: 'ready' });

process.on('message', (msg) => {
  switch(msg.type) {
    case 'start' : 
      const data = msg.payload || [];
    
      const jobBuilder = new JobBuilder(data);
      const jobs = jobBuilder.buildAll();
    
      jobs.forEach(job => process.send({ type: 'enqueue', job }));
      break;
    
    case 'ack-confirm' :
      console.log(`[Jobs] job ${msg.jobId} confirmed processed`);
      break;
    
    default : console.log(`message type : ${msg.type}`);
    
  }
});