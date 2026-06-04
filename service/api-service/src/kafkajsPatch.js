"use strict";

function patchKafkaJsNegativeTimeoutWarning() {
  try {
    const RequestQueue = require("kafkajs/src/network/requestQueue");
    const proto = RequestQueue && RequestQueue.prototype;
    if (!proto || proto.__copilotPatchedNegativeTimeout) return;

    const original = proto.scheduleCheckPendingRequests;
    if (typeof original !== "function") return;

    proto.scheduleCheckPendingRequests = function patchedScheduleCheckPendingRequests(...args) {
      try {
        const hasPending = Array.isArray(this.pending) && this.pending.length > 0;
        const throttled = typeof this.throttledUntil === "number" && this.throttledUntil > Date.now();
        if (!hasPending && !throttled) {
          return;
        }
      } catch {
        // fall through
      }
      return original.apply(this, args);
    };

    proto.__copilotPatchedNegativeTimeout = true;
  } catch {
    // ignore
  }
}

module.exports = { patchKafkaJsNegativeTimeoutWarning };
