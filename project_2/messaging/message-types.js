// messaging/message-types.js

const MessageTypes = {
    // === Lifecycle Messages ===
    READY: 'READY',
    ALIVE: 'ALIVE',
    SHUTDOWN: 'SHUTDOWN',
    SHUTDOWN_COMPLETE: 'SHUTDOWN_COMPLETE',
    HEARTBEAT: 'HEARTBEAT',
    STATUS: 'STATUS',

    // === Job Messages ===
    NEW_JOB: 'NEW_JOB',
    JOB_QUEUED: 'JOB_QUEUED',
    JOB_STARTED: 'JOB_STARTED',
    JOB_COMPLETE: 'JOB_COMPLETE',
    JOB_FAILED: 'JOB_FAILED',
    JOB_FULLY_COMPLETE: 'JOB_FULLY_COMPLETE',

    // === Job Submitter Messages ===
    START_SUBMITTING: 'START_SUBMITTING',
    SUBMIT_JOB: 'SUBMIT_JOB',
    SUBMITTER_STARTED: 'SUBMITTER_STARTED',
    SUBMITTER_COMPLETE: 'SUBMITTER_COMPLETE',
    JOB_SUBMITTED: 'JOB_SUBMITTED',  // ✅ Added

    // === SQLite Messages ===
    SQLITE_REQUEST: 'SQLITE_REQUEST',
    SQLITE_RESPONSE: 'SQLITE_RESPONSE',
    SQLITE_READY: 'SQLITE_READY',
    ALL_TABLES_CREATED: 'ALL_TABLES_CREATED',

    // === Error Messages ===
    ERROR: 'ERROR',
    JOB_SUBMISSION_ERROR: 'JOB_SUBMISSION_ERROR'
};

const MessageDestinations = {
    ORCHESTRATOR: 'orchestrator',
    SQLITE_SERVER: 'sqlite-server',
    ANALYZER: 'analyzer',
    BROWSER: 'browser',
    EXPORTER: 'exporter',
    JOB_SUBMITTER: 'job-submitter',
    ALL: 'all'
};

const VALID_DESTINATIONS = Object.values(MessageDestinations);
const VALID_TYPES = Object.values(MessageTypes);

module.exports = {
    MessageTypes,
    MessageDestinations,
    VALID_DESTINATIONS,
    VALID_TYPES
};