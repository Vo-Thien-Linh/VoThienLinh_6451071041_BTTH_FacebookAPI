"use strict";

class CircuitBreaker {
  constructor({ name, failureThreshold, resetTimeoutMs }) {
    this.name = name || "circuit";
    this.failureThreshold = Math.max(1, Number(failureThreshold) || 5);
    this.resetTimeoutMs = Math.max(1000, Number(resetTimeoutMs) || 30_000);
    this.state = "closed";
    this.failureCount = 0;
    this.openedAt = 0;
  }

  canCall() {
    if (this.state !== "open") return true;
    const elapsed = Date.now() - this.openedAt;
    if (elapsed >= this.resetTimeoutMs) {
      this.state = "half_open";
      return true;
    }
    return false;
  }

  rejectError() {
    const err = new Error(`${this.name} circuit breaker is open`);
    err.name = "CircuitBreakerOpenError";
    err.circuitOpen = true;
    err.retryable = true;
    err.data = {
      state: this.state,
      failureCount: this.failureCount,
      resetAfterMs: Math.max(0, this.resetTimeoutMs - (Date.now() - this.openedAt)),
    };
    return err;
  }

  success() {
    this.state = "closed";
    this.failureCount = 0;
    this.openedAt = 0;
  }

  failure() {
    this.failureCount += 1;
    if (this.state === "half_open" || this.failureCount >= this.failureThreshold) {
      this.state = "open";
      this.openedAt = Date.now();
    }
  }

  async execute(fn) {
    if (!this.canCall()) {
      throw this.rejectError();
    }

    try {
      const result = await fn();
      this.success();
      return result;
    } catch (err) {
      this.failure();
      throw err;
    }
  }
}

module.exports = { CircuitBreaker };
