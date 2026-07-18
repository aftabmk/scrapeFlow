// core/load-balancer.js
const { EventEmitter } = require('events');
const { namedMutex } = require('../utils/mutex');

class LoadBalancer extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.options = {
            maxQueueSize: 10000,
            batchSize: 50,
            workerTimeout: 30000,
            ...options,
        };
        
        this.queues = { high: [], normal: [], low: [] };
        this.workers = {
            submitter: [],
            analyzer: [],
            browser: [],
            exporter: [],
        };
        
        this.workerRegistry = new Map();
        this.roundRobinIndex = 0;
        this.processing = false;
        this.isRunning = true;
        this.taskAssignments = new Map();
        this.processingTasks = new Set();
        
        this.stats = {
            tasksQueued: 0,
            tasksProcessed: 0,
            tasksFailed: 0,
            tasksRejected: 0,
            tasksRouted: 0,
        };
        
        this.startProcessing();
        
        console.log('[LoadBalancer] Initialized (Health checks disabled)');
    }

    enqueue(task, priority = 'normal') {
        if (!this.isRunning) return null;
        if (!this.queues[priority]) priority = 'normal';
        
        // ✅ Check for duplicate task
        if (task.id && this.processingTasks.has(task.id)) {
            console.log(`[LoadBalancer] ⚠️ Task ${task.id} already processing, rejecting duplicate`);
            return null;
        }
        
        const wrappedTask = {
            ...task,
            id: task.id || this.generateId(),
            priority,
            enqueuedAt: Date.now(),
            attempts: 0,
            maxAttempts: task.maxAttempts || 3,
            status: 'pending',
        };
        
        if (wrappedTask.type === 'execute' && !wrappedTask.workerType) {
            if (wrappedTask.payload && wrappedTask.payload.workerType) {
                wrappedTask.workerType = wrappedTask.payload.workerType;
            } else {
                wrappedTask.workerType = 'submitter';
            }
        }
        
        const totalSize = this.getQueueSize();
        if (totalSize >= this.options.maxQueueSize) {
            this.stats.tasksRejected++;
            this.emit('queue.full', { task: wrappedTask });
            return null;
        }
        
        this.queues[priority].push(wrappedTask);
        this.stats.tasksQueued++;
        this.emit('task.enqueued', wrappedTask);
        
        console.log(`[LoadBalancer] 📥 Enqueued task: ${wrappedTask.id} (${priority}, ${wrappedTask.workerType})`);
        
        return wrappedTask.id;
    }

    dequeue() {
        if (this.queues.high.length > 0) {
            const task = this.queues.high.shift();
            console.log(`[LoadBalancer] 📤 Dequeued HIGH task: ${task.id}`);
            return task;
        }
        
        if (this.queues.normal.length > 0) {
            const task = this.queues.normal.shift();
            console.log(`[LoadBalancer] 📤 Dequeued NORMAL task: ${task.id}`);
            return task;
        }
        
        if (this.queues.low.length > 0) {
            const task = this.queues.low.shift();
            console.log(`[LoadBalancer] 📤 Dequeued LOW task: ${task.id}`);
            return task;
        }
        
        return null;
    }

    registerWorker(worker, type) {
        const workerId = `worker_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
        
        const info = {
            id: workerId,
            worker,
            type,
            status: 'idle',
            currentTask: null,
            stats: { processed: 0, failed: 0, avgTime: 0, totalTime: 0 },
            registeredAt: Date.now(),
        };
        
        if (!this.workers[type]) this.workers[type] = [];
        this.workers[type].push(info);
        this.workerRegistry.set(workerId, info);
        
        this.emit('worker.registered', { workerId, type });
        console.log(`[LoadBalancer] ✅ Registered ${type} worker: ${workerId}`);
        return workerId;
    }

    unregisterWorker(workerId) {
        const info = this.workerRegistry.get(workerId);
        if (!info) return false;
        
        const workers = this.workers[info.type];
        if (workers) {
            const index = workers.findIndex(w => w.id === workerId);
            if (index !== -1) workers.splice(index, 1);
        }
        
        this.workerRegistry.delete(workerId);
        
        // Clean up task assignments
        for (const [taskId, assignedWorkerId] of this.taskAssignments) {
            if (assignedWorkerId === workerId) {
                this.taskAssignments.delete(taskId);
            }
        }
        
        this.emit('worker.unregistered', { workerId });
        return true;
    }

    /**
     * ✅ Get available worker with mutex to prevent duplicate assignment
     */
    async getAvailableWorkerWithLock(type, taskId) {
        return await namedMutex.execute(`loadbalancer_${type}`, () => {
            return this.getAvailableWorker(type, taskId);
        });
    }

    /**
     * ✅ Get available worker with atomic check
     */
    getAvailableWorker(type, taskId) {
        const workers = this.workers[type] || [];
        const available = workers.filter(w => w.status === 'idle');
        
        if (available.length === 0) {
            console.log(`[LoadBalancer] ⚠️ No available ${type} workers`);
            return null;
        }
        
        // ✅ Check if this task already has a worker assigned
        if (taskId && this.taskAssignments.has(taskId)) {
            const assignedWorkerId = this.taskAssignments.get(taskId);
            const assignedWorker = this.workers[type].find(w => w.id === assignedWorkerId);
            if (assignedWorker && assignedWorker.status === 'idle') {
                console.log(`[LoadBalancer] ✅ Task ${taskId} already assigned to ${assignedWorkerId}`);
                return assignedWorker;
            } else {
                this.taskAssignments.delete(taskId);
            }
        }
        
        // ✅ Round-robin selection
        const index = this.roundRobinIndex % available.length;
        this.roundRobinIndex++;
        const worker = available[index];
        worker.status = 'busy';
        
        if (taskId) {
            this.taskAssignments.set(taskId, worker.id);
            this.processingTasks.add(taskId);
        }
        
        console.log(`[LoadBalancer] ✅ Assigned ${worker.id} (${type}) to task ${taskId || 'unknown'}`);
        return worker;
    }

    /**
     * ✅ Release task assignment
     */
    releaseTaskAssignment(taskId) {
        if (taskId) {
            this.taskAssignments.delete(taskId);
            this.processingTasks.delete(taskId);
            console.log(`[LoadBalancer] ✅ Released task ${taskId}`);
        }
    }

    getWorkerTypeForTask(task) {
        if (task.workerType) return task.workerType;
        switch (task.type) {
            case 'execute':
                if (task.payload && task.payload.workerType) {
                    return task.payload.workerType;
                }
                return 'submitter';
            case 'start_submitting':
            case 'submit_job':
                return 'submitter';
            case 'route_job':
                return task.to || task.payload?.to || 'analyzer';
            default:
                return null;
        }
    }

    handleWorkerResponse(workerId, message) {
        // Release task assignment on completion
        if (message.taskId) {
            this.releaseTaskAssignment(message.taskId);
        }
        this.emit('worker.response', { ...message, workerId });
        this.emit('worker.direct.response', workerId, message);
    }

    async processTasks() {
        if (this.processing) return;
        this.processing = true;
        console.log('[LoadBalancer] Started processing loop');
        
        while (this.isRunning) {
            try {
                const task = this.dequeue();
                if (!task) { await this.sleep(10); continue; }
                await this.processTask(task);
            } catch (error) {
                console.error('[LoadBalancer] Processing error:', error);
                await this.sleep(100);
            }
        }
        
        console.log('[LoadBalancer] Processing loop stopped');
    }

    async processTask(task) {
        const workerType = this.getWorkerTypeForTask(task);
        
        if (!workerType) {
            console.log(`[LoadBalancer] ⚠️ No workerType for task ${task.id}, re-queueing`);
            this.enqueue(task, task.priority);
            await this.sleep(50);
            return;
        }
        
        // ✅ Use mutex to get worker
        const worker = await this.getAvailableWorkerWithLock(workerType, task.id);
        
        if (!worker) {
            console.log(`[LoadBalancer] ⚠️ No worker available for ${workerType}, re-queueing task ${task.id}`);
            this.enqueue(task, task.priority);
            await this.sleep(50);
            return;
        }
        
        await this.executeTask(worker, task);
    }

    async executeTask(worker, task) {
        worker.status = 'busy';
        worker.currentTask = task;
        task.attempts++;
        
        this.emit('task.assigned', { taskId: task.id, workerId: worker.id, workerType: worker.type });
        
        try {
            const startTime = Date.now();
            
            worker.worker.postMessage({
                type: 'execute',
                taskId: task.id,
                payload: task.payload || task,
            });
            
            const result = await this.waitForWorkerResponse(worker.id, task.id);
            const duration = Date.now() - startTime;
            
            worker.stats.processed++;
            worker.stats.totalTime += duration;
            worker.stats.avgTime = worker.stats.totalTime / worker.stats.processed;
            this.stats.tasksProcessed++;
            
            // ✅ Release task assignment
            this.releaseTaskAssignment(task.id);
            
            this.emit('task.complete', { taskId: task.id, workerId: worker.id, result, duration });
            
            if (result && result.requiresRouting && result.nextStage) {
                this.enqueue({
                    type: 'route_job',
                    workerType: result.nextStage,
                    payload: { job: result.job || result, from: result.from, to: result.nextStage },
                    priority: 'high',
                });
                this.stats.tasksRouted++;
            }
            
        } catch (error) {
            worker.stats.failed++;
            this.stats.tasksFailed++;
            
            this.emit('task.failed', { taskId: task.id, workerId: worker.id, error: error.message });
            
            // ✅ Release task assignment
            this.releaseTaskAssignment(task.id);
            
            if (task.attempts < task.maxAttempts) {
                console.log(`[LoadBalancer] 🔄 Retrying task ${task.id} (attempt ${task.attempts + 1}/${task.maxAttempts})`);
                this.enqueue(task, 'high');
            } else {
                console.error(`[LoadBalancer] ❌ Task ${task.id} exceeded max attempts, sending to dead letter`);
                this.emit('task.deadletter', { taskId: task.id, task, error: error.message });
            }
            
        } finally {
            worker.status = 'idle';
            worker.currentTask = null;
        }
    }

    waitForWorkerResponse(workerId, taskId) {
        return new Promise((resolve, reject) => {
            let resolved = false;
            
            const timeout = setTimeout(() => {
                if (!resolved) {
                    resolved = true;
                    // ✅ Release task on timeout
                    this.releaseTaskAssignment(taskId);
                    reject(new Error(`Worker ${workerId} timed out on task ${taskId}`));
                }
            }, this.options.workerTimeout);
            
            const responseHandler = (message) => {
                if (message.taskId !== taskId) return;
                if (message.type === 'task.complete') {
                    resolved = true;
                    clearTimeout(timeout);
                    this.removeListener('worker.response', responseHandler);
                    // ✅ Release task on completion
                    this.releaseTaskAssignment(taskId);
                    resolve(message.result);
                } else if (message.type === 'task.failed') {
                    resolved = true;
                    clearTimeout(timeout);
                    this.removeListener('worker.response', responseHandler);
                    // ✅ Release task on failure
                    this.releaseTaskAssignment(taskId);
                    reject(new Error(message.error || 'Task failed'));
                }
            };
            
            const directHandler = (id, message) => {
                if (id !== workerId) return;
                if (message.taskId !== taskId) return;
                if (message.type === 'task.complete') {
                    resolved = true;
                    clearTimeout(timeout);
                    this.removeListener('worker.direct.response', directHandler);
                    this.releaseTaskAssignment(taskId);
                    resolve(message.result);
                } else if (message.type === 'task.failed') {
                    resolved = true;
                    clearTimeout(timeout);
                    this.removeListener('worker.direct.response', directHandler);
                    this.releaseTaskAssignment(taskId);
                    reject(new Error(message.error || 'Task failed'));
                }
            };
            
            this.on('worker.response', responseHandler);
            this.on('worker.direct.response', directHandler);
        });
    }

    startProcessing() { this.processTasks(); }
    getQueueSize() { return this.queues.high.length + this.queues.normal.length + this.queues.low.length; }

    getWorkerStats() {
        const stats = {};
        for (const type of Object.keys(this.workers)) {
            stats[type] = {
                total: this.workers[type].length,
                idle: this.workers[type].filter(w => w.status === 'idle').length,
                busy: this.workers[type].filter(w => w.status === 'busy').length,
            };
        }
        return stats;
    }

    getStats() {
        return {
            queues: {
                high: this.queues.high.length,
                normal: this.queues.normal.length,
                low: this.queues.low.length,
                total: this.getQueueSize(),
            },
            workers: this.getWorkerStats(),
            stats: this.stats,
            registry: this.workerRegistry.size,
            isRunning: this.isRunning,
            taskAssignments: this.taskAssignments.size,
            processingTasks: this.processingTasks.size,
        };
    }

    sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
    generateId() { return `task_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`; }

    shutdown() {
        console.log('[LoadBalancer] Shutting down...');
        this.isRunning = false;
        this.processing = false;
        this.queues = { high: [], normal: [], low: [] };
        this.workerRegistry.clear();
        this.taskAssignments.clear();
        this.processingTasks.clear();
        this.removeAllListeners();
        console.log('[LoadBalancer] Shutdown complete');
    }
}

module.exports = LoadBalancer;