"use strict";

function patchKafkaJsNegativeTimeoutWarning() {
  try {
    // Kafkajs v2.2.4 may schedule setTimeout with a negative delay when there are
    // no pending requests and no throttle in place, causing TimeoutNegativeWarning.
    // This patch avoids scheduling in that no-op case.
    // Ref: kafkajs/src/network/requestQueue/index.js scheduleCheckPendingRequests
    // NOTE: This is a best-effort runtime patch; safe to skip if internals change.
    // eslint-disable-next-line global-require
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
        // fall through to original
      }
      return original.apply(this, args);
    };

    proto.__copilotPatchedNegativeTimeout = true;
  } catch {
    // ignore
  }
}

module.exports = {
  patchKafkaJsNegativeTimeoutWarning,
};
