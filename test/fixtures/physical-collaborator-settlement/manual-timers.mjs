import { performance as nodePerformance } from "node:perf_hooks";

const globalSetTimeoutDescriptor = Object.getOwnPropertyDescriptor(
  globalThis,
  "setTimeout",
);
const globalClearTimeoutDescriptor = Object.getOwnPropertyDescriptor(
  globalThis,
  "clearTimeout",
);
const PromiseConstructor = Promise;
const performancePrototype = Object.getPrototypeOf(nodePerformance);
const performanceNowDescriptor = Object.getOwnPropertyDescriptor(
  performancePrototype,
  "now",
);
const promiseResolveIntrinsic = Promise.resolve;
const reflectApply = Reflect.apply;

export function installManualTimers() {
  const active = new Map();
  let cancelled = 0;
  let clearFailuresRemaining = 0;
  let nextId = 1;
  let now = 0;
  let scheduleFailuresRemaining = 0;
  let synchronousSchedulesRemaining = 0;
  let scheduled = 0;

  function nextDueAtOrBefore(target) {
    let selected = null;
    for (const task of active.values()) {
      if (task.at > target) continue;
      if (
        selected === null ||
        task.at < selected.at ||
        (task.at === selected.at && task.id < selected.id)
      ) {
        selected = task;
      }
    }
    return selected;
  }

  function setTimeoutFixture(callback, delay = 0, ...args) {
    if (scheduleFailuresRemaining > 0) {
      scheduleFailuresRemaining -= 1;
      throw new Error("manual timer schedule failure");
    }
    const normalizedDelay = Number.isFinite(delay) && delay > 0 ? delay : 0;
    const id = nextId;
    nextId += 1;
    scheduled += 1;
    const handle = Object.freeze({ id });
    active.set(handle, {
      args,
      at: now + normalizedDelay,
      callback,
      handle,
      id,
    });
    if (synchronousSchedulesRemaining > 0) {
      synchronousSchedulesRemaining -= 1;
      Reflect.apply(callback, undefined, args);
    }
    return handle;
  }

  function clearTimeoutFixture(handle) {
    if (clearFailuresRemaining > 0) {
      clearFailuresRemaining -= 1;
      throw new Error("manual timer clear failure");
    }
    if (active.delete(handle)) cancelled += 1;
  }

  Object.defineProperty(globalThis, "setTimeout", {
    ...globalSetTimeoutDescriptor,
    value: setTimeoutFixture,
  });
  Object.defineProperty(globalThis, "clearTimeout", {
    ...globalClearTimeoutDescriptor,
    value: clearTimeoutFixture,
  });
  Object.defineProperty(performancePrototype, "now", {
    ...performanceNowDescriptor,
    value() {
      return now;
    },
  });

  let restored = false;
  return Object.freeze({
    advanceBy(milliseconds) {
      if (
        !Number.isSafeInteger(milliseconds) ||
        milliseconds < 0
      ) {
        throw new TypeError("manual timer advance must be a non-negative safe integer");
      }
      const target = now + milliseconds;
      for (;;) {
        const task = nextDueAtOrBefore(target);
        if (task === null) break;
        active.delete(task.handle);
        now = task.at;
        Reflect.apply(task.callback, undefined, task.args);
      }
      if (now < target) now = target;
    },
    elapseWithoutRunning(milliseconds) {
      if (
        !Number.isSafeInteger(milliseconds) ||
        milliseconds < 0
      ) {
        throw new TypeError("manual timer elapsed time must be a non-negative safe integer");
      }
      now += milliseconds;
    },
    failNextClear() {
      clearFailuresRemaining += 1;
    },
    failNextSchedule() {
      scheduleFailuresRemaining += 1;
    },
    runNextScheduleSynchronously() {
      synchronousSchedulesRemaining += 1;
    },
    get activeCount() {
      return active.size;
    },
    get cancelledCount() {
      return cancelled;
    },
    get now() {
      return now;
    },
    restore() {
      if (restored) return;
      restored = true;
      Object.defineProperty(
        globalThis,
        "setTimeout",
        globalSetTimeoutDescriptor,
      );
      Object.defineProperty(
        globalThis,
        "clearTimeout",
        globalClearTimeoutDescriptor,
      );
      Object.defineProperty(
        performancePrototype,
        "now",
        performanceNowDescriptor,
      );
    },
    get scheduledCount() {
      return scheduled;
    },
  });
}

export async function flushMicrotasks(turns = 8) {
  for (let index = 0; index < turns; index += 1) {
    await reflectApply(promiseResolveIntrinsic, PromiseConstructor, [undefined]);
  }
}
