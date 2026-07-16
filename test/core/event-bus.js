// core/event-bus.js
const { EventEmitter } = require('events');

class EventBus extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.options = {
      maxListeners: 100,
      historySize: 1000,
      ...options,
    };
    
    this.subscribers = new Map();
    this.history = [];
    this.stats = {
      eventsPublished: 0,
      eventsDelivered: 0,
      errors: 0,
    };
    
    this.setMaxListeners(this.options.maxListeners);
    console.log('[EventBus] Initialized');
  }

  subscribe(topic, callback, options = {}) {
    if (!this.subscribers.has(topic)) {
      this.subscribers.set(topic, []);
    }
    
    const subscriber = {
      id: this.generateId(),
      callback,
      options: {
        once: options.once || false,
        priority: options.priority || 0,
        filter: options.filter || null,
      },
      stats: {
        received: 0,
        errors: 0,
        lastReceived: null,
      },
    };
    
    this.subscribers.get(topic).push(subscriber);
    this.subscribers.get(topic).sort((a, b) => b.options.priority - a.options.priority);
    
    console.log(`[EventBus] Subscribed to topic: ${topic} (${subscriber.id})`);
    return subscriber.id;
  }

  unsubscribe(topic, subscriberId) {
    if (!this.subscribers.has(topic)) return false;
    const subscribers = this.subscribers.get(topic);
    const index = subscribers.findIndex(s => s.id === subscriberId);
    if (index === -1) return false;
    subscribers.splice(index, 1);
    console.log(`[EventBus] Unsubscribed from ${topic}`);
    return true;
  }

  async publish(topic, event, options = {}) {
    const eventWrapper = {
      id: this.generateId(),
      topic,
      event,
      timestamp: Date.now(),
      options: {
        ttl: options.ttl || null,
        retry: options.retry || false,
        priority: options.priority || 0,
      },
      attempts: 0,
      delivered: false,
    };
    
    this.history.push(eventWrapper);
    if (this.history.length > this.options.historySize) {
      this.history.shift();
    }
    
    this.stats.eventsPublished++;
    
    const subscribers = this.subscribers.get(topic) || [];
    if (subscribers.length === 0) {
      this.emit('event.published', eventWrapper);
      return eventWrapper.id;
    }
    
    const filtered = this.filterSubscribers(subscribers, event);
    if (filtered.length === 0) {
      this.emit('event.published', eventWrapper);
      return eventWrapper.id;
    }
    
    this.deliverToSubscribers(filtered, eventWrapper);
    this.emit('event.published', eventWrapper);
    return eventWrapper.id;
  }

  filterSubscribers(subscribers, event) {
    return subscribers.filter(sub => {
      if (!sub.options.filter) return true;
      try {
        return sub.options.filter(event);
      } catch (err) {
        console.error(`[EventBus] Filter error:`, err);
        return false;
      }
    });
  }

  async deliverToSubscribers(subscribers, eventWrapper) {
    const promises = subscribers.map(async (subscriber) => {
      if (subscriber.options.once) {
        const topic = eventWrapper.topic;
        const subs = this.subscribers.get(topic);
        if (subs) {
          const idx = subs.findIndex(s => s.id === subscriber.id);
          if (idx !== -1) subs.splice(idx, 1);
        }
      }
      
      try {
        const result = await subscriber.callback(eventWrapper.event);
        subscriber.stats.received++;
        subscriber.stats.lastReceived = Date.now();
        this.stats.eventsDelivered++;
        this.emit('event.delivered', {
          subscriberId: subscriber.id,
          eventId: eventWrapper.id,
          topic: eventWrapper.topic,
          result,
        });
        return { success: true, subscriberId: subscriber.id };
      } catch (error) {
        subscriber.stats.errors++;
        this.stats.errors++;
        this.emit('event.error', {
          subscriberId: subscriber.id,
          eventId: eventWrapper.id,
          topic: eventWrapper.topic,
          error: error.message,
        });
        return { success: false, subscriberId: subscriber.id, error: error.message };
      }
    });
    
    await Promise.allSettled(promises);
  }

  getStats() {
    const topics = Array.from(this.subscribers.keys());
    const totalSubscribers = Array.from(this.subscribers.values())
      .reduce((sum, subs) => sum + subs.length, 0);
    
    return {
      topics: topics.length,
      totalSubscribers,
      eventsPublished: this.stats.eventsPublished,
      eventsDelivered: this.stats.eventsDelivered,
      errors: this.stats.errors,
      historySize: this.history.length,
    };
  }

  generateId() {
    return `evt_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
  }

  shutdown() {
    console.log('[EventBus] Shutting down...');
    this.removeAllListeners();
    this.subscribers.clear();
    this.history = [];
    console.log('[EventBus] Shutdown complete');
  }
}

module.exports = EventBus;