// utils/logger.js
const fs = require('fs');
const path = require('path');
const os = require('os');

/**
 * Logger - Structured logging with levels, rotation, and formatting
 * Supports console, file, and JSON output
 */
class Logger {
  constructor(options = {}) {
    this.options = {
      level: options.level || 'info',
      pretty: options.pretty !== false,
      outputDir: options.outputDir || './logs',
      maxFiles: options.maxFiles || 10,
      maxSize: options.maxSize || '100m',
      prefix: options.prefix || 'APP',
      timestamp: options.timestamp !== false,
      colors: options.colors !== false,
      ...options,
    };

    // Log levels
    this.levels = {
      debug: 0,
      info: 1,
      warn: 2,
      error: 3,
      fatal: 4,
    };

    // Color codes
    this.colors = {
      debug: '\x1b[36m',    // Cyan
      info: '\x1b[32m',     // Green
      warn: '\x1b[33m',     // Yellow
      error: '\x1b[31m',    // Red
      fatal: '\x1b[35m',    // Magenta
      reset: '\x1b[0m',
      bold: '\x1b[1m',
      dim: '\x1b[2m',
    };

    // Current log file
    this.currentFile = null;
    this.currentSize = 0;
    this.fileStream = null;
    this.buffer = [];
    this.bufferSize = 0;
    this.flushInterval = null;
    this.isShuttingDown = false;

    // Ensure log directory exists
    this.ensureDirectory();

    // Start flush interval
    this.startFlushInterval();

    // Handle process exit
    this.setupExitHandlers();

    console.log(`[Logger] Initialized (level: ${this.options.level})`);
  }

  /**
   * Ensure log directory exists
   */
  ensureDirectory() {
    const dir = this.options.outputDir;
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  }

  /**
   * Get log file path
   */
  getLogFilePath() {
    const date = new Date();
    const timestamp = date.toISOString().split('T')[0];
    return path.join(this.options.outputDir, `app-${timestamp}.log`);
  }

  /**
   * Rotate log file if needed
   */
  rotateLogFile() {
    const maxSizeBytes = this.parseSize(this.options.maxSize);
    
    // Check if current file exceeds max size
    if (this.currentSize >= maxSizeBytes) {
      // Close current stream
      if (this.fileStream) {
        this.fileStream.end();
        this.fileStream = null;
      }

      // Rename old file with timestamp
      const oldPath = this.currentFile;
      if (oldPath && fs.existsSync(oldPath)) {
        const timestamp = Date.now();
        const newPath = oldPath.replace('.log', `-${timestamp}.log`);
        fs.renameSync(oldPath, newPath);
      }

      // Create new file
      this.currentFile = this.getLogFilePath();
      this.currentSize = 0;
      this.fileStream = fs.createWriteStream(this.currentFile, { flags: 'a' });
    }

    // Clean old files
    this.cleanOldFiles();
  }

  /**
   * Clean old log files
   */
  cleanOldFiles() {
    const files = fs.readdirSync(this.options.outputDir)
      .filter(f => f.startsWith('app-') && f.endsWith('.log'))
      .map(f => ({
        name: f,
        path: path.join(this.options.outputDir, f),
        mtime: fs.statSync(path.join(this.options.outputDir, f)).mtime,
      }))
      .sort((a, b) => b.mtime - a.mtime);

    // Keep only maxFiles
    if (files.length > this.options.maxFiles) {
      const toDelete = files.slice(this.options.maxFiles);
      for (const file of toDelete) {
        try {
          fs.unlinkSync(file.path);
        } catch (err) {
          // Ignore
        }
      }
    }
  }

  /**
   * Parse size string to bytes
   */
  parseSize(size) {
    const units = {
      'b': 1,
      'kb': 1024,
      'mb': 1024 * 1024,
      'gb': 1024 * 1024 * 1024,
    };

    const match = String(size).match(/^(\d+)\s*([a-z]+)?$/i);
    if (!match) return 100 * 1024 * 1024; // 100MB default

    const value = parseInt(match[1]);
    const unit = (match[2] || 'mb').toLowerCase();
    return value * (units[unit] || units.mb);
  }

  /**
   * Start flush interval
   */
  startFlushInterval() {
    this.flushInterval = setInterval(() => {
      this.flush();
    }, 1000);
  }

  /**
   * Flush buffer to file
   */
  flush() {
    if (this.isShuttingDown) return;
    if (this.buffer.length === 0) return;

    const data = this.buffer.join('');
    this.buffer = [];
    this.bufferSize = 0;

    try {
      // Rotate if needed
      this.rotateLogFile();

      // Write to file
      if (!this.fileStream) {
        this.currentFile = this.getLogFilePath();
        this.fileStream = fs.createWriteStream(this.currentFile, { flags: 'a' });
      }

      this.fileStream.write(data);
      this.currentSize += Buffer.byteLength(data);

    } catch (error) {
      console.error('Failed to write log:', error);
      // Re-queue buffer
      this.buffer.unshift(data);
    }
  }

  /**
   * Setup exit handlers
   */
  setupExitHandlers() {
    const flushAndExit = () => {
      this.isShuttingDown = true;
      clearInterval(this.flushInterval);
      this.flush();
      if (this.fileStream) {
        this.fileStream.end();
      }
      process.exit(0);
    };

    process.on('SIGINT', flushAndExit);
    process.on('SIGTERM', flushAndExit);
    process.on('exit', () => {
      this.isShuttingDown = true;
      clearInterval(this.flushInterval);
      this.flush();
      if (this.fileStream) {
        this.fileStream.end();
      }
    });
  }

  /**
   * Format a log entry
   */
  format(level, message, data = null, error = null) {
    const timestamp = new Date().toISOString();
    const levelUpper = level.toUpperCase();
    
    let logEntry = {
      timestamp,
      level: levelUpper,
      prefix: this.options.prefix,
      message,
      pid: process.pid,
      hostname: os.hostname(),
    };

    if (data) {
      logEntry.data = data;
    }

    if (error) {
      logEntry.error = {
        message: error.message,
        stack: error.stack,
        ...(error.code && { code: error.code }),
        ...(error.status && { status: error.status }),
      };
    }

    return logEntry;
  }

  /**
   * Format log entry for output
   */
  formatOutput(entry) {
    if (this.options.pretty) {
      return this.formatPretty(entry);
    }
    return this.formatJSON(entry);
  }

  /**
   * Pretty format
   */
  formatPretty(entry) {
    const parts = [];

    // Timestamp
    if (this.options.timestamp) {
      parts.push(this.colors.dim + entry.timestamp + this.colors.reset);
    }

    // Level with color
    const color = this.colors[entry.level.toLowerCase()] || this.colors.reset;
    const levelPad = entry.level.padStart(5);
    parts.push(color + levelPad + this.colors.reset);

    // Prefix
    parts.push(this.colors.bold + '[' + entry.prefix + ']' + this.colors.reset);

    // PID
    parts.push(this.colors.dim + '(' + entry.pid + ')' + this.colors.reset);

    // Message
    parts.push(entry.message);

    // Data (if any)
    if (entry.data) {
      parts.push('\n  ' + this.colors.dim + '└─ Data:' + this.colors.reset);
      const dataStr = JSON.stringify(entry.data, null, 2)
        .split('\n')
        .map(line => '     ' + line)
        .join('\n');
      parts.push(dataStr);
    }

    // Error (if any)
    if (entry.error) {
      parts.push('\n  ' + this.colors.red + '└─ Error:' + this.colors.reset);
      parts.push('     ' + this.colors.red + entry.error.message + this.colors.reset);
      if (entry.error.stack && this.options.level === 'debug') {
        parts.push('     ' + this.colors.dim + entry.error.stack.split('\n').slice(1).join('\n     ') + this.colors.reset);
      }
    }

    return parts.join(' ') + '\n';
  }

  /**
   * JSON format
   */
  formatJSON(entry) {
    return JSON.stringify(entry) + '\n';
  }

  /**
   * Internal log method
   */
  _log(level, message, data = null, error = null) {
    const levelValue = this.levels[level];
    const currentLevel = this.levels[this.options.level] || 1;

    // Check level
    if (levelValue < currentLevel) return;

    // Format entry
    const entry = this.format(level, message, data, error);
    const output = this.formatOutput(entry);

    // Console output
    if (this.options.pretty && this.options.colors) {
      const color = this.colors[level] || this.colors.reset;
      console.log(color + output.trim() + this.colors.reset);
    } else {
      console.log(output.trim());
    }

    // File output
    if (this.options.outputDir) {
      this.buffer.push(output);
      this.bufferSize += Buffer.byteLength(output);

      // Flush if buffer is large
      if (this.bufferSize > 1024 * 1024) { // 1MB
        this.flush();
      }
    }
  }

  /**
   * Debug log
   */
  debug(message, data = null) {
    this._log('debug', message, data);
  }

  /**
   * Info log
   */
  info(message, data = null) {
    this._log('info', message, data);
  }

  /**
   * Warn log
   */
  warn(message, data = null) {
    this._log('warn', message, data);
  }

  /**
   * Error log
   */
  error(message, error = null, data = null) {
    this._log('error', message, data, error);
  }

  /**
   * Fatal log
   */
  fatal(message, error = null, data = null) {
    this._log('fatal', message, data, error);
  }

  /**
   * Log with custom level
   */
  log(level, message, data = null, error = null) {
    this._log(level, message, data, error);
  }

  /**
   * Create child logger
   */
  child(prefix) {
    return new Logger({
      ...this.options,
      prefix: `${this.options.prefix}:${prefix}`,
    });
  }

  /**
   * Get stats
   */
  getStats() {
    return {
      level: this.options.level,
      pretty: this.options.pretty,
      outputDir: this.options.outputDir,
      bufferSize: this.bufferSize,
      bufferLength: this.buffer.length,
      currentFile: this.currentFile,
      currentSize: this.currentSize,
    };
  }

  /**
   * Shutdown logger
   */
  shutdown() {
    this.isShuttingDown = true;
    clearInterval(this.flushInterval);
    this.flush();
    if (this.fileStream) {
      this.fileStream.end();
      this.fileStream = null;
    }
  }
}

// Create singleton instance
const logger = new Logger({
  level: process.env.LOG_LEVEL || 'info',
  pretty: process.env.LOG_PRETTY !== 'false',
  outputDir: process.env.LOG_OUTPUT_DIR || './logs',
  maxFiles: parseInt(process.env.LOG_MAX_FILES) || 10,
  maxSize: process.env.LOG_MAX_SIZE || '100m',
  prefix: 'SCRAPE',
});

module.exports = logger;