// supervisor/MessageRouter.js
class MessageRouter {
  wire(procs) {
    const { jobs, queue, browser, walServer } = procs;

    jobs.on('message', (msg) => {
      if (msg.type === 'enqueue') queue.send(msg);
    });

    browser.on('message', (msg) => {
      if (['dequeue-request', 'ack'].includes(msg.type)) {
        queue.send(msg);
      }
    });

    queue.on('message', (msg) => {
      if (msg.type === 'dequeue-response') browser.send(msg);
      if (msg.type === 'ack-confirm') jobs.send(msg);

      if (walServer && ['wal-write', 'recover'].includes(msg.type)) {
        walServer.send(msg);
      }
    });

    if (walServer) {
      walServer.on('message', (msg) => {
        if (['recover-response', 'write-ack'].includes(msg.type)) {
          queue.send(msg);
        }
      });
    }
  }
}

module.exports = MessageRouter;