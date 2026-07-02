// supervisor/Logger.js
class Logger {
  static log(name, msg) {
    console.log(`[${msg.source ?? name}]`, msg.message, msg.port ?? '');
  }

  static error(name, msg) {
    console.error(`[${msg.source ?? name}]`, msg.message, msg.error ?? '');
  }

  // Attaches a persistent listener that forwards log/error messages
  // for the lifetime of the process (used after startup completes).
  static attach(proc, name) {
    proc.on('message', (msg) => {
      if (msg.type === 'log') Logger.log(name, msg);
      if (msg.type === 'error') Logger.error(name, msg);
    });
  }
}

module.exports = Logger;