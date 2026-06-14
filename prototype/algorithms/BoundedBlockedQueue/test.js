const BoundedBlockingQueue = require('./algorithm/boundedBlockedQueue');


const main = async () => {
	const queue = new BoundedBlockingQueue(2);
    
	await queue.enqueue("A");
    await queue.enqueue("B");

    console.log("Queue full");

    // This will wait until something is removed
    queue.enqueue("C").then(() => {
        console.log("C inserted");
    });

    setTimeout(async () => {
        console.log(await queue.dequeue()); // A
        console.log(await queue.dequeue()); // B
        console.log(await queue.dequeue()); // C
    }, 2000);
};

main();