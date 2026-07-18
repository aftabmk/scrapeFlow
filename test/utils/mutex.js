// utils/mutex.js

/**
 * Mutex - Thread lock for preventing race conditions
 * Used to ensure atomic operations on shared resources
 */
class Mutex {
    constructor() {
        this._locked = false;
        this._queue = [];
        this._name = 'mutex';
    }

    /**
     * Acquire the lock
     * @param {string} name - Optional name for debugging
     * @returns {Promise<() => void>} Release function
     */
    async acquire(name = '') {
        const lockName = name || this._name;
        
        return new Promise((resolve) => {
            const release = () => {
                if (this._queue.length > 0) {
                    const next = this._queue.shift();
                    next();
                } else {
                    this._locked = false;
                }
            };

            if (!this._locked) {
                this._locked = true;
                console.log(`[Mutex] 🔒 Acquired: ${lockName}`);
                resolve(release);
            } else {
                console.log(`[Mutex] ⏳ Waiting for lock: ${lockName} (queue: ${this._queue.length + 1})`);
                this._queue.push(() => {
                    console.log(`[Mutex] 🔒 Acquired after wait: ${lockName}`);
                    resolve(release);
                });
            }
        });
    }

    /**
     * Execute a function with lock
     * @param {Function} fn - Function to execute with lock
     * @param {string} name - Optional name for debugging
     * @returns {Promise<any>} Result of the function
     */
    async execute(fn, name = '') {
        const release = await this.acquire(name);
        try {
            return await fn();
        } finally {
            release();
            console.log(`[Mutex] 🔓 Released: ${name || this._name}`);
        }
    }

    /**
     * Check if lock is held
     */
    isLocked() {
        return this._locked;
    }

    /**
     * Get queue length
     */
    queueLength() {
        return this._queue.length;
    }

    /**
     * Get stats
     */
    getStats() {
        return {
            locked: this._locked,
            queueLength: this._queue.length,
            name: this._name
        };
    }
}

/**
 * Named Mutex - Multiple mutex instances with names
 */
class NamedMutex {
    constructor() {
        this._mutexes = new Map();
    }

    /**
     * Get or create a mutex by name
     */
    get(name) {
        if (!this._mutexes.has(name)) {
            this._mutexes.set(name, new Mutex());
        }
        return this._mutexes.get(name);
    }

    /**
     * Execute with named lock
     */
    async execute(name, fn) {
        const mutex = this.get(name);
        return await mutex.execute(fn, name);
    }

    /**
     * Get stats for all mutexes
     */
    getStats() {
        const stats = {};
        for (const [name, mutex] of this._mutexes) {
            stats[name] = mutex.getStats();
        }
        return stats;
    }
}

// Singleton instance
const namedMutex = new NamedMutex();

module.exports = {
    Mutex,
    NamedMutex,
    namedMutex
};