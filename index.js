const { bootstrap, produceJobs, eventBus } = require('./src');

const main = async() => {
	await bootstrap();        // init browser + start consumer
	produceJobs('prod-1');    // fire all jobs onto the bus
	
	// optional — listen for results
	eventBus.subscribe('browser:response', ({ data }) => console.log(data));
	eventBus.subscribe('browser:dead',     ({ data }) => console.error(data));
	
}

console.log(process.argv);
main();