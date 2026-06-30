class HealthMonitor {
  constructor(healthCheckFn) {
    this.healthCheckFn = healthCheckFn;
    this.timer = null;
  }

  start(intervalMs = 10_000) {
    this.timer = setInterval(
      () => this.healthCheckFn(),
      intervalMs
    );
  }

  stop() {
    clearInterval(this.timer);
    this.timer = null;
  }
}

module.exports = HealthMonitor;