const EventQueue = require("./algorithms/EventQueue");

const WalServer = require("../sqlite/wal/WALServer");
const JobEvent = require("../../events/jobEvent");

const produceEvents = () => {
  JobEvent.emit({ id: "1", type: "job.created", payload: { name: "Job 1" } });
  JobEvent.emit({ id: "2", type: "job.running", payload: { name: "Job 2" } });
  JobEvent.emit({ id: "3", type: "job.completed", payload: { name: "Job 3" } });
}

const main = async () => {
  // Instantiate the WAL server BEFORE any queue is constructed.
  const walServer = await WalServer.ensure({ port: 8766 });

  // queueId scopes this queue's entries on the shared WAL server — needed
  // now that multiple queues can write to the same SQLite-backed WAL.
  const queue = new EventQueue(JobEvent, { queueId: "job-events" });

  produceEvents();

  for (;;) {
    const messages = await queue.receiveMessages();

    for (const msg of messages) {
      const event = JSON.parse(msg.Body);

      console.log("Received:", event);

      await queue.deleteMessage(msg.ReceiptHandle);
    }
  }
};

main();