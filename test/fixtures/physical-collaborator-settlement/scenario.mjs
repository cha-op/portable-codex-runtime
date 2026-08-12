import assert from "node:assert/strict";
import { types as utilTypes } from "node:util";

import {
  flushMicrotasks,
  installManualTimers,
} from "./manual-timers.mjs";

const scenario = process.argv[2];
const supportedScenarios = new Set([
  "cached-promise",
  "deadline-late-settlement",
  "grace-breach",
  "hostile-boundaries",
  "real-timers",
  "shape-and-normal-settlement",
  "stop-drain",
]);

assert.equal(
  scenario === undefined || supportedScenarios.has(scenario),
  true,
  `unknown scenario ${scenario}`,
);

const manualTimers =
  scenario === undefined || scenario === "real-timers"
    ? null
    : installManualTimers();
const settlementModule =
  scenario === undefined
    ? Object.create(null)
    : await import(
        `../../../src/physical-collaborator-settlement.mjs?scenario=${scenario}`
      );
manualTimers?.restore();

const {
  PHYSICAL_COLLABORATOR_SETTLEMENT_CONTRACT_VERSION,
  PhysicalCollaboratorSettlementError,
  createPhysicalCollaboratorSettlement,
  isPhysicalCollaboratorSettlement,
} = settlementModule;

const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectApply = Reflect.apply;
const abortSignalAbortedGetter = Object.getOwnPropertyDescriptor(
  AbortSignal.prototype,
  "aborted",
).get;

const OPTION_CODE = "invalid_physical_collaborator_settlement_options";
const REQUEST_CODE = "invalid_physical_collaborator_settlement_request";
const REJECTED_CODE = "physical_collaborator_rejected";
const LATE_CODE = "physical_collaborator_late_settlement";
const NO_SETTLEMENT_CODE = "physical_collaborator_no_settlement";
const OUTCOME_UNCERTAIN_CODE =
  "physical_collaborator_settlement_outcome_uncertain";

function safeCarrier(value) {
  return Object.freeze(Object.assign(Object.create(null), { value }));
}

function deferred() {
  let reject;
  let resolve;
  const promise = new PromiseConstructor((resolvePromise, rejectPromise) => {
    reject = rejectPromise;
    resolve = resolvePromise;
  });
  return { promise, reject, resolve };
}

function invocation(start) {
  return Object.freeze({ start: Object.freeze(start) });
}

function assertSettlementError(error, code) {
  assert(error instanceof PhysicalCollaboratorSettlementError);
  assert.equal(error?.code, code);
  assert.equal(error?.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  assert.equal(Object.hasOwn(error, "cause"), false);
  assert.equal(typeof error.message, "string");
  assert.equal(error.message.length < 160, true);
  return true;
}

async function rejectsCall(call, code) {
  let observed;
  try {
    await call();
  } catch (error) {
    observed = error;
  }
  assert.notEqual(observed, undefined, `expected ${code}`);
  assertSettlementError(observed, code);
  return observed;
}

function observe(pending) {
  let status = "pending";
  let value;
  const completion = new PromiseConstructor((resolve) => {
    reflectApply(promiseThenIntrinsic, pending, [
      (result) => {
        status = "fulfilled";
        value = result;
        resolve();
      },
      (error) => {
        status = "rejected";
        value = error;
        resolve();
      },
    ]);
  });
  return {
    completion,
    get status() {
      return status;
    },
    get value() {
      return value;
    },
  };
}

function signalIsAborted(signal) {
  return reflectApply(abortSignalAbortedGetter, signal, []);
}

function assertSignal(signal, expectedAborted) {
  assert.equal(Object.getPrototypeOf(signal), AbortSignal.prototype);
  assert.equal(signalIsAborted(signal), expectedAborted);
}

function authority({
  deadlineMilliseconds = 10,
  onFatal = Object.freeze(() => undefined),
  settlementGraceMilliseconds = 5,
} = {}) {
  return createPhysicalCollaboratorSettlement(
    Object.freeze({
      deadlineMilliseconds,
      onFatal,
      settlementGraceMilliseconds,
    }),
  );
}

function assertStopped(value) {
  assert.equal(Object.getPrototypeOf(value), null);
  assert.equal(Object.isFrozen(value), true);
  assert.deepEqual({ ...value }, { status: "stopped" });
}

function assertSuccessReceipt(receipt, value, invocationValue) {
  assert.deepEqual(Reflect.ownKeys(receipt), [
    "contractVersion",
    "invocation",
    "outcome",
    "value",
  ]);
  assert.equal(Object.getPrototypeOf(receipt), null);
  assert.equal(Object.isFrozen(receipt), true);
  assert.equal(receipt.contractVersion, 1);
  assert.strictEqual(receipt.invocation, invocationValue);
  assert.equal(receipt.outcome, "success");
  assert.strictEqual(receipt.value, value);
}

function assertFatalReceipt(receipt, invocationValue, trigger) {
  assert.deepEqual(Reflect.ownKeys(receipt), [
    "contractVersion",
    "invocation",
    "outcome",
    "trigger",
  ]);
  assert.equal(Object.getPrototypeOf(receipt), null);
  assert.equal(Object.isFrozen(receipt), true);
  assert.equal(receipt.contractVersion, 1);
  assert.strictEqual(receipt.invocation, invocationValue);
  assert.equal(receipt.outcome, "no-settlement");
  assert.equal(receipt.trigger, trigger);
}

async function shapeAndNormalSettlement() {
  assert.equal(PHYSICAL_COLLABORATOR_SETTLEMENT_CONTRACT_VERSION, 1);
  const fatals = [];
  const settlement = authority({
    onFatal: Object.freeze((...args) => {
      fatals.push(args);
    }),
  });
  assert.deepEqual(Reflect.ownKeys(settlement), [
    "contractVersion",
    "invoke",
    "stop",
  ]);
  assert.equal(Object.getPrototypeOf(settlement), null);
  assert.equal(Object.isFrozen(settlement), true);
  assert.equal(
    settlement.contractVersion,
    PHYSICAL_COLLABORATOR_SETTLEMENT_CONTRACT_VERSION,
  );
  assert.equal(Object.isFrozen(settlement.invoke), true);
  assert.equal(Object.isFrozen(settlement.stop), true);
  assert.equal(isPhysicalCollaboratorSettlement(settlement), true);
  assert.equal(
    isPhysicalCollaboratorSettlement(Object.freeze({ ...settlement })),
    false,
  );
  assert.equal(
    isPhysicalCollaboratorSettlement(new Proxy(settlement, {})),
    false,
  );

  const contexts = [];
  const fulfilled = safeCarrier("fulfilled-value");
  const returned = await settlement.invoke(
    invocation(function successfulStart(context) {
      assert.equal(this, undefined);
      contexts.push(context);
      return PromiseConstructor.resolve(fulfilled);
    }),
  );
  assert.equal(contexts.length, 1);
  assert.deepEqual(Reflect.ownKeys(contexts[0]), ["invocation", "signal"]);
  assert.equal(Object.getPrototypeOf(contexts[0]), null);
  assert.equal(Object.isFrozen(contexts[0]), true);
  assertSignal(contexts[0].signal, false);
  assertSuccessReceipt(returned, fulfilled, contexts[0].invocation);
  assert.equal(manualTimers.activeCount, 0);

  const asynchronousCarrier = safeCarrier("asynchronous-value");
  const asynchronousContexts = [];
  const asynchronousStart = Object.freeze(
    async function asynchronousStart(context) {
      asynchronousContexts.push(context);
      return asynchronousCarrier;
    },
  );
  const boundAsynchronousStart = Object.freeze(
    asynchronousStart.bind(undefined),
  );
  const directAsynchronousReceipt = await settlement.invoke(
    invocation(asynchronousStart),
  );
  const boundAsynchronousReceipt = await settlement.invoke(
    invocation(boundAsynchronousStart),
  );
  assertSuccessReceipt(
    directAsynchronousReceipt,
    asynchronousCarrier,
    asynchronousContexts[0].invocation,
  );
  assertSuccessReceipt(
    boundAsynchronousReceipt,
    asynchronousCarrier,
    asynchronousContexts[1].invocation,
  );

  const privateRejection = new Error("private collaborator rejection");
  await rejectsCall(
    () =>
      settlement.invoke(
        invocation(function rejectedStart() {
          return PromiseConstructor.reject(privateRejection);
        }),
      ),
    REJECTED_CODE,
  );
  assert.equal(manualTimers.activeCount, 0);
  assert.deepEqual(fatals, []);

  const contractFatals = [];
  let throwingContext;
  const contractSettlement = authority({
    onFatal: Object.freeze((receipt) => {
      contractFatals.push(receipt);
    }),
  });
  await rejectsCall(
    () =>
      contractSettlement.invoke(
        invocation(function throwingStart(context) {
          throwingContext = context;
          throw new Error("private synchronous collaborator failure");
        }),
      ),
    NO_SETTLEMENT_CODE,
  );
  assertSignal(throwingContext.signal, true);
  assert.equal(contractFatals.length, 1);
  assertFatalReceipt(
    contractFatals[0],
    throwingContext.invocation,
    "contract-violation",
  );
  await rejectsCall(() => contractSettlement.stop(), NO_SETTLEMENT_CODE);
  assert.equal(manualTimers.activeCount, 0);

  for (const invalid of [
    undefined,
    null,
    {},
    {
      deadlineMilliseconds: 0,
      onFatal: Object.freeze(() => undefined),
      settlementGraceMilliseconds: 5,
    },
    {
      deadlineMilliseconds: 10,
      extra: true,
      onFatal: Object.freeze(() => undefined),
      settlementGraceMilliseconds: 5,
    },
    {
      deadlineMilliseconds: 86_400_001,
      onFatal: Object.freeze(() => undefined),
      settlementGraceMilliseconds: 5,
    },
    {
      deadlineMilliseconds: 10,
      onFatal: Object.freeze(() => undefined),
      settlementGraceMilliseconds: 86_400_001,
    },
  ]) {
    assert.throws(
      () => createPhysicalCollaboratorSettlement(invalid),
      (error) => assertSettlementError(error, OPTION_CODE),
    );
  }

  const maximumBudget = authority({
    deadlineMilliseconds: 86_400_000,
    settlementGraceMilliseconds: 86_400_000,
  });
  assertStopped(await maximumBudget.stop());

  const stopping = settlement.stop();
  assert.strictEqual(settlement.stop(), stopping);
  assertStopped(await stopping);
  await rejectsCall(
    () =>
      settlement.invoke(
        invocation(() => {
          assert.fail("stopped authority dispatched collaborator work");
        }),
      ),
    REQUEST_CODE,
  );
  assert.equal(manualTimers.activeCount, 0);
}

async function deadlineLateSettlement() {
  for (const kind of ["fulfill", "reject"]) {
    const fatals = [];
    const pendingProvider = deferred();
    let abortEvents = 0;
    let signal;
    const settlement = authority({
      onFatal: Object.freeze((...args) => {
        fatals.push(args);
      }),
    });
    const pending = settlement.invoke(
      invocation(function lateStart(context) {
        signal = context.signal;
        signal.addEventListener(
          "abort",
          () => {
            abortEvents += 1;
          },
          { once: true },
        );
        return pendingProvider.promise;
      }),
    );
    const observed = observe(pending);
    assertSignal(signal, false);

    manualTimers.advanceBy(9);
    await flushMicrotasks();
    assert.equal(observed.status, "pending");
    assertSignal(signal, false);

    manualTimers.advanceBy(1);
    assertSignal(signal, true);
    assert.equal(abortEvents, 1);
    await flushMicrotasks();
    assert.equal(observed.status, "pending");

    if (kind === "fulfill") {
      pendingProvider.resolve(safeCarrier("late-value"));
    } else {
      pendingProvider.reject(new Error("private late rejection"));
    }
    await observed.completion;
    assert.equal(observed.status, "rejected");
    assertSettlementError(observed.value, LATE_CODE);
    assert.equal(abortEvents, 1);
    assert.deepEqual(fatals, []);
    assert.equal(manualTimers.activeCount, 0);
    assertStopped(await settlement.stop());
  }

  const synchronousAbortProvider = deferred();
  let signal;
  const settlement = authority();
  const pending = settlement.invoke(
    invocation(function resolveFromAbort(context) {
      signal = context.signal;
      signal.addEventListener(
        "abort",
        () => synchronousAbortProvider.resolve(safeCarrier("abort-value")),
        { once: true },
      );
      return synchronousAbortProvider.promise;
    }),
  );
  manualTimers.advanceBy(10);
  assertSignal(signal, true);
  await rejectsCall(() => pending, LATE_CODE);
  assert.equal(manualTimers.activeCount, 0);
  assertStopped(await settlement.stop());

  const blockedStartFatals = [];
  let blockedStartSignal;
  const blockedStart = authority({
    onFatal: Object.freeze((receipt) => {
      blockedStartFatals.push(receipt);
    }),
  });
  const blockedStartPending = blockedStart.invoke(
    invocation((context) => {
      blockedStartSignal = context.signal;
      manualTimers.elapseWithoutRunning(16);
      return PromiseConstructor.resolve(safeCarrier("blocked-start"));
    }),
  );
  await rejectsCall(() => blockedStartPending, NO_SETTLEMENT_CODE);
  assertSignal(blockedStartSignal, true);
  assert.equal(blockedStartFatals.length, 1);
  await rejectsCall(() => blockedStart.stop(), NO_SETTLEMENT_CODE);
  assert.equal(manualTimers.activeCount, 0);

  const blockedAbortProvider = deferred();
  const blockedAbortFatals = [];
  let blockedAbortSignal;
  const blockedAbort = authority({
    onFatal: Object.freeze((receipt) => {
      blockedAbortFatals.push(receipt);
    }),
  });
  const blockedAbortPending = blockedAbort.invoke(
    invocation((context) => {
      blockedAbortSignal = context.signal;
      blockedAbortSignal.addEventListener(
        "abort",
        () => {
          manualTimers.elapseWithoutRunning(6);
          blockedAbortProvider.resolve(safeCarrier("blocked-abort"));
        },
        { once: true },
      );
      return blockedAbortProvider.promise;
    }),
  );
  manualTimers.advanceBy(10);
  await rejectsCall(() => blockedAbortPending, NO_SETTLEMENT_CODE);
  assertSignal(blockedAbortSignal, true);
  assert.equal(blockedAbortFatals.length, 1);
  await rejectsCall(() => blockedAbort.stop(), NO_SETTLEMENT_CODE);
  assert.equal(manualTimers.activeCount, 0);
}

async function graceBreach() {
  const unhandled = [];
  const onUnhandled = (reason, promise) => unhandled.push({ promise, reason });
  process.prependListener("unhandledRejection", onUnhandled);
  try {
    for (const lateKind of ["fulfill", "reject"]) {
      const pendingProvider = deferred();
      const fatalCalls = [];
      let invocationValue;
      let signal;
      const settlement = authority({
        onFatal: Object.freeze((...args) => {
          fatalCalls.push(args);
        }),
      });
      const observed = observe(
          settlement.invoke(
          invocation(function neverSettledInGrace(context) {
            invocationValue = context.invocation;
            signal = context.signal;
            return pendingProvider.promise;
          }),
        ),
      );
      manualTimers.advanceBy(10);
      assertSignal(signal, true);
      manualTimers.advanceBy(4);
      await flushMicrotasks();
      assert.equal(observed.status, "pending");
      assert.equal(fatalCalls.length, 0);

      manualTimers.advanceBy(1);
      await observed.completion;
      assert.equal(observed.status, "rejected");
      assertSettlementError(observed.value, NO_SETTLEMENT_CODE);
      assert.equal(fatalCalls.length, 1);
      assert.equal(fatalCalls[0].length, 1);
      assertFatalReceipt(
        fatalCalls[0][0],
        invocationValue,
        "deadline",
      );
      assert.equal(manualTimers.activeCount, 0);

      if (lateKind === "fulfill") {
        pendingProvider.resolve(safeCarrier("post-breach-value"));
      } else {
        pendingProvider.reject(new Error("private post-breach rejection"));
      }
      await flushMicrotasks();
      await new PromiseConstructor((resolve) => setImmediate(resolve));
      assert.equal(fatalCalls.length, 1);
      assert.deepEqual(unhandled, []);
      const stopping = settlement.stop();
      assert.strictEqual(settlement.stop(), stopping);
      await rejectsCall(() => stopping, NO_SETTLEMENT_CODE);
      assert.equal(manualTimers.activeCount, 0);
    }

    const reentrantProvider = deferred();
    let reentrantSettlement;
    let reentrantStopObservation;
    reentrantSettlement = authority({
      onFatal: Object.freeze(() => {
        const stopping = reentrantSettlement.stop();
        assert.strictEqual(reentrantSettlement.stop(), stopping);
        reentrantStopObservation = observe(stopping);
      }),
    });
    const reentrantInvocation = observe(
      reentrantSettlement.invoke(
        invocation(() => reentrantProvider.promise),
      ),
    );
    manualTimers.advanceBy(10);
    manualTimers.advanceBy(5);
    await reentrantInvocation.completion;
    await reentrantStopObservation.completion;
    assertSettlementError(reentrantInvocation.value, NO_SETTLEMENT_CODE);
    assertSettlementError(reentrantStopObservation.value, NO_SETTLEMENT_CODE);
    assert.equal(manualTimers.activeCount, 0);

    let throwingFatalCalls = 0;
    const pendingProvider = deferred();
    const settlement = authority({
      onFatal: Object.freeze(() => {
        throwingFatalCalls += 1;
        throw new Error("private fatal hook failure");
      }),
    });
    const observed = observe(
      settlement.invoke(invocation(() => pendingProvider.promise)),
    );
    manualTimers.advanceBy(10);
    assert.doesNotThrow(() => manualTimers.advanceBy(5));
    await observed.completion;
    assertSettlementError(observed.value, NO_SETTLEMENT_CODE);
    assert.equal(throwingFatalCalls, 1);
    await rejectsCall(() => settlement.stop(), OUTCOME_UNCERTAIN_CODE);
    assert.equal(manualTimers.activeCount, 0);

    let promiseReturningFatalCalls = 0;
    const promiseReturningProvider = deferred();
    const promiseReturningSettlement = authority({
      onFatal: Object.freeze(() => {
        promiseReturningFatalCalls += 1;
        return Object.freeze(
          PromiseConstructor.reject(
            new Error("private asynchronous fatal hook failure"),
          ),
        );
      }),
    });
    const promiseReturningObserved = observe(
      promiseReturningSettlement.invoke(
        invocation(() => promiseReturningProvider.promise),
      ),
    );
    manualTimers.advanceBy(10);
    manualTimers.advanceBy(5);
    await promiseReturningObserved.completion;
    await flushMicrotasks();
    await new PromiseConstructor((resolve) => setImmediate(resolve));
    assertSettlementError(promiseReturningObserved.value, NO_SETTLEMENT_CODE);
    assert.equal(promiseReturningFatalCalls, 1);
    await rejectsCall(
      () => promiseReturningSettlement.stop(),
      OUTCOME_UNCERTAIN_CODE,
    );
    assert.deepEqual(unhandled, []);
    assert.equal(manualTimers.activeCount, 0);
  } finally {
    process.removeListener("unhandledRejection", onUnhandled);
  }
}

async function stopDrain() {
  for (const preSettled of [
    PromiseConstructor.resolve(safeCarrier("pre-fulfilled")),
    PromiseConstructor.reject(new Error("private pre-rejected provider")),
  ]) {
    let preSettledContext;
    const preSettledAuthority = authority({ deadlineMilliseconds: 100 });
    const preSettledInvocation = preSettledAuthority.invoke(
      invocation((context) => {
        preSettledContext = context;
        return preSettled;
      }),
    );
    const preSettledStop = preSettledAuthority.stop();
    await rejectsCall(() => preSettledInvocation, LATE_CODE);
    assertSignal(preSettledContext.signal, true);
    assertStopped(await preSettledStop);
  }

  const firstProvider = deferred();
  const secondProvider = deferred();
  const contexts = [];
  const settlement = authority({ deadlineMilliseconds: 100 });
  const first = observe(
    settlement.invoke(
      invocation((context) => {
        contexts.push(context);
        return firstProvider.promise;
      }),
    ),
  );
  const second = observe(
    settlement.invoke(
      invocation((context) => {
        contexts.push(context);
        return secondProvider.promise;
      }),
    ),
  );
  const stopping = settlement.stop();
  assert.strictEqual(settlement.stop(), stopping);
  assert.equal(contexts.length, 2);
  for (const context of contexts) assertSignal(context.signal, true);
  const stopObservation = observe(stopping);
  await flushMicrotasks();
  assert.equal(first.status, "pending");
  assert.equal(second.status, "pending");
  assert.equal(stopObservation.status, "pending");
  await rejectsCall(
    () => settlement.invoke(invocation(() => PromiseConstructor.resolve(safeCarrier(null)))),
    REQUEST_CODE,
  );

  firstProvider.resolve(safeCarrier("first"));
  await first.completion;
  assertSettlementError(first.value, LATE_CODE);
  assert.equal(stopObservation.status, "pending");
  secondProvider.reject(new Error("private stopped rejection"));
  await second.completion;
  assertSettlementError(second.value, LATE_CODE);
  await stopObservation.completion;
  assert.equal(stopObservation.status, "fulfilled");
  assertStopped(stopObservation.value);
  assert.equal(manualTimers.activeCount, 0);

  const neverProvider = deferred();
  const fatalCalls = [];
  let failedInvocation;
  const failed = authority({
    deadlineMilliseconds: 100,
    onFatal: Object.freeze((receipt) => {
      fatalCalls.push(receipt);
    }),
  });
  const invocationObservation = observe(
    failed.invoke(
      invocation((context) => {
        failedInvocation = context.invocation;
        return neverProvider.promise;
      }),
    ),
  );
  const failedStop = failed.stop();
  const failedStopObservation = observe(failedStop);
  manualTimers.advanceBy(4);
  await flushMicrotasks();
  assert.equal(failedStopObservation.status, "pending");
  manualTimers.advanceBy(1);
  await Promise.all([
    invocationObservation.completion,
    failedStopObservation.completion,
  ]);
  assertSettlementError(invocationObservation.value, NO_SETTLEMENT_CODE);
  assertSettlementError(failedStopObservation.value, NO_SETTLEMENT_CODE);
  assert.equal(fatalCalls.length, 1);
  assertFatalReceipt(fatalCalls[0], failedInvocation, "stop");
  assert.strictEqual(failed.stop(), failedStop);
  neverProvider.resolve(safeCarrier("too-late"));
  await flushMicrotasks();
  assert.equal(fatalCalls.length, 1);
  assert.equal(manualTimers.activeCount, 0);

  const originalGraceProvider = deferred();
  const originalGrace = authority();
  const originalInvocation = observe(
    originalGrace.invoke(invocation(() => originalGraceProvider.promise)),
  );
  manualTimers.advanceBy(10);
  manualTimers.advanceBy(4);
  const originalStop = observe(originalGrace.stop());
  manualTimers.advanceBy(1);
  await Promise.all([originalInvocation.completion, originalStop.completion]);
  assertSettlementError(originalInvocation.value, NO_SETTLEMENT_CODE);
  assertSettlementError(originalStop.value, NO_SETTLEMENT_CODE);
  assert.equal(manualTimers.activeCount, 0);
}

async function cachedPromise() {
  const provider = deferred();
  const carrier = safeCarrier("cached-value");
  const contexts = [];
  let calls = 0;
  const start = Object.freeze((context) => {
    calls += 1;
    contexts.push(context);
    return provider.promise;
  });
  const settlement = authority();
  const first = observe(settlement.invoke(invocation(start)));
  manualTimers.advanceBy(5);
  const second = observe(settlement.invoke(invocation(start)));
  manualTimers.advanceBy(5);
  assertSignal(contexts[0].signal, true);
  assertSignal(contexts[1].signal, false);

  provider.resolve(carrier);
  await Promise.all([first.completion, second.completion]);
  assert.equal(first.status, "rejected");
  assertSettlementError(first.value, LATE_CODE);
  assert.equal(second.status, "fulfilled");
  assertSuccessReceipt(second.value, carrier, contexts[1].invocation);
  assert.notStrictEqual(contexts[0].invocation, contexts[1].invocation);
  assert.notStrictEqual(contexts[0].signal, contexts[1].signal);

  const third = await settlement.invoke(invocation(start));
  assertSuccessReceipt(third, carrier, contexts[2].invocation);
  assert.equal(calls, 3);
  assert.notStrictEqual(contexts[1].invocation, contexts[2].invocation);
  assert.notStrictEqual(contexts[1].signal, contexts[2].signal);
  assertSignal(contexts[2].signal, false);
  assert.equal(manualTimers.activeCount, 0);
  assertStopped(await settlement.stop());
  assertSignal(contexts[1].signal, false);
  assertSignal(contexts[2].signal, false);
}

async function hostileBoundaries() {
  let thenCalls = 0;
  let proxyTraps = 0;
  let accessorReads = 0;
  let constructorAccessorReads = 0;
  const carrier = safeCarrier("safe");
  const thenable = Object.freeze({
    then() {
      thenCalls += 1;
    },
  });
  const rawProxyPromise = PromiseConstructor.resolve(carrier);
  const proxyPromise = new Proxy(rawProxyPromise, {
    get() {
      proxyTraps += 1;
      throw new Error("Promise proxy trap must not run");
    },
    getPrototypeOf() {
      proxyTraps += 1;
      throw new Error("Promise proxy prototype trap must not run");
    },
  });
  class PromiseSubclass extends PromiseConstructor {}
  const subclass = new PromiseSubclass((resolve) => resolve(carrier));
  const ownThen = PromiseConstructor.resolve(carrier);
  Object.defineProperty(ownThen, "then", {
    configurable: true,
    get() {
      accessorReads += 1;
      throw new Error("own then accessor must not run");
    },
  });
  const ordinaryProviderValue = Object.freeze({
    value: "ordinary-prototype",
  });
  const ordinaryValuePromise = PromiseConstructor.resolve(
    ordinaryProviderValue,
  );

  for (const [name, returned] of [
    ["undefined", undefined],
    ["thenable", thenable],
    ["Promise proxy", proxyPromise],
    ["Promise subclass", subclass],
    ["own then", ownThen],
  ]) {
    const fatalCalls = [];
    let context;
    const settlement = authority({
      onFatal: Object.freeze((fatal) => {
        fatalCalls.push(fatal);
      }),
    });
    await rejectsCall(
      () =>
        settlement.invoke(
          invocation((value) => {
            context = value;
            return returned;
          }),
        ),
      NO_SETTLEMENT_CODE,
    );
    assert.equal(fatalCalls.length, 1);
    assertFatalReceipt(
      fatalCalls[0],
      context.invocation,
      "contract-violation",
    );
    await rejectsCall(() => settlement.stop(), NO_SETTLEMENT_CODE);
    assert.equal(manualTimers.activeCount, 0);
    assert.equal(typeof name, "string");
  }
  assert.equal(thenCalls, 0);
  assert.equal(proxyTraps, 0);
  assert.equal(accessorReads, 0);

  const invalidPromiseUnhandled = [];
  const onInvalidPromiseUnhandled = (reason, promise) => {
    invalidPromiseUnhandled.push({ promise, reason });
  };
  process.prependListener("unhandledRejection", onInvalidPromiseUnhandled);
  for (const makeRejectedPromise of [
    () => {
      const rejected = PromiseConstructor.reject(
        new Error("private rejected Promise with own constructor"),
      );
      Object.defineProperty(rejected, "constructor", {
        configurable: true,
        get() {
          constructorAccessorReads += 1;
          throw new Error("own constructor accessor must not run");
        },
      });
      return rejected;
    },
    () =>
      Object.freeze(
        PromiseConstructor.reject(
          new Error("private frozen rejected native Promise"),
        ),
      ),
    () =>
      new PromiseSubclass((_resolve, reject) => {
        reject(new Error("private rejected Promise subclass"));
      }),
    () => {
      const rejected = PromiseConstructor.reject(
        new Error("private rejected Promise with own then"),
      );
      Object.defineProperty(rejected, "then", {
        configurable: true,
        get() {
          accessorReads += 1;
          throw new Error("rejected own then accessor must not run");
        },
      });
      return rejected;
    },
  ]) {
    const invalidPromiseSettlement = authority();
    await rejectsCall(
      () =>
        invalidPromiseSettlement.invoke(
          invocation(() => makeRejectedPromise()),
        ),
      NO_SETTLEMENT_CODE,
    );
    await rejectsCall(
      () => invalidPromiseSettlement.stop(),
      NO_SETTLEMENT_CODE,
    );
  }
  await flushMicrotasks();
  await new PromiseConstructor((resolve) => setImmediate(resolve));
  process.removeListener(
    "unhandledRejection",
    onInvalidPromiseUnhandled,
  );
  assert.deepEqual(invalidPromiseUnhandled, []);
  assert.equal(accessorReads, 0);
  assert.equal(constructorAccessorReads, 0);

  const ordinaryValueSettlement = authority();
  let ordinaryValueContext;
  const ordinaryValueReceipt = await ordinaryValueSettlement.invoke(
    invocation((context) => {
      ordinaryValueContext = context;
      return ordinaryValuePromise;
    }),
  );
  assertSuccessReceipt(
    ordinaryValueReceipt,
    ordinaryProviderValue,
    ordinaryValueContext.invocation,
  );
  assertStopped(await ordinaryValueSettlement.stop());

  const proxiedStart = new Proxy(function proxiedStartTarget() {}, {
    apply() {
      proxyTraps += 1;
      throw new Error("proxied start must not run");
    },
  });
  const settlement = authority();
  await rejectsCall(
    () => settlement.invoke(Object.freeze({ start: proxiedStart })),
    REQUEST_CODE,
  );
  assert.equal(proxyTraps, 0);

  const scheduleFatals = [];
  let scheduledStartCalls = 0;
  const scheduleFailure = authority({
    onFatal: Object.freeze((receipt) => {
      scheduleFatals.push(receipt);
    }),
  });
  manualTimers.failNextSchedule();
  await rejectsCall(
    () =>
      scheduleFailure.invoke(
        invocation(() => {
          scheduledStartCalls += 1;
          return PromiseConstructor.resolve(carrier);
        }),
      ),
    OUTCOME_UNCERTAIN_CODE,
  );
  assert.equal(scheduledStartCalls, 0);
  assert.equal(scheduleFatals.length, 1);
  assertFatalReceipt(
    scheduleFatals[0],
    scheduleFatals[0].invocation,
    "contract-violation",
  );
  await rejectsCall(() => scheduleFailure.stop(), OUTCOME_UNCERTAIN_CODE);
  assert.equal(manualTimers.activeCount, 0);

  const synchronousScheduleFatals = [];
  let synchronousScheduleStartCalls = 0;
  const synchronousSchedule = authority({
    onFatal: Object.freeze((receipt) => {
      synchronousScheduleFatals.push(receipt);
    }),
  });
  manualTimers.runNextScheduleSynchronously();
  await rejectsCall(
    () =>
      synchronousSchedule.invoke(
        invocation(() => {
          synchronousScheduleStartCalls += 1;
          return PromiseConstructor.resolve(carrier);
        }),
      ),
    OUTCOME_UNCERTAIN_CODE,
  );
  assert.equal(synchronousScheduleStartCalls, 0);
  assert.equal(synchronousScheduleFatals.length, 1);
  await rejectsCall(
    () => synchronousSchedule.stop(),
    OUTCOME_UNCERTAIN_CODE,
  );
  assert.equal(manualTimers.activeCount, 0);

  const synchronousGraceProvider = deferred();
  const synchronousGraceFatals = [];
  const synchronousGrace = authority({
    onFatal: Object.freeze((receipt) => {
      synchronousGraceFatals.push(receipt);
    }),
  });
  const synchronousGraceObserved = observe(
    synchronousGrace.invoke(
      invocation(() => synchronousGraceProvider.promise),
    ),
  );
  manualTimers.runNextScheduleSynchronously();
  manualTimers.advanceBy(10);
  await synchronousGraceObserved.completion;
  assertSettlementError(
    synchronousGraceObserved.value,
    OUTCOME_UNCERTAIN_CODE,
  );
  assert.equal(synchronousGraceFatals.length, 1);
  await rejectsCall(
    () => synchronousGrace.stop(),
    OUTCOME_UNCERTAIN_CODE,
  );
  assert.equal(manualTimers.activeCount, 0);

  const clearFatals = [];
  const clearFailureProvider = deferred();
  const clearFailure = authority({
    onFatal: Object.freeze((receipt) => {
      clearFatals.push(receipt);
    }),
  });
  const clearFailureObserved = observe(
    clearFailure.invoke(
      invocation(() => clearFailureProvider.promise),
    ),
  );
  manualTimers.failNextClear();
  clearFailureProvider.resolve(carrier);
  await clearFailureObserved.completion;
  assertSettlementError(clearFailureObserved.value, OUTCOME_UNCERTAIN_CODE);
  assert.equal(clearFatals.length, 1);
  assertFatalReceipt(
    clearFatals[0],
    clearFatals[0].invocation,
    "contract-violation",
  );
  await rejectsCall(() => clearFailure.stop(), OUTCOME_UNCERTAIN_CODE);
  assert.equal(manualTimers.activeCount, 0);

  const promiseConstructorDescriptor = Object.getOwnPropertyDescriptor(
    PromiseConstructor.prototype,
    "constructor",
  );
  const promiseThenDescriptor = Object.getOwnPropertyDescriptor(
    PromiseConstructor.prototype,
    "then",
  );
  const objectThenDescriptor = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "then",
  );
  let poisonReads = 0;
  const restorePromisePoison = () => {
    Object.defineProperty(
      PromiseConstructor.prototype,
      "constructor",
      promiseConstructorDescriptor,
    );
    Object.defineProperty(
      PromiseConstructor.prototype,
      "then",
      promiseThenDescriptor,
    );
    if (objectThenDescriptor === undefined) delete Object.prototype.then;
    else Object.defineProperty(Object.prototype, "then", objectThenDescriptor);
  };
  const poisonPromise = () => {
    Object.defineProperty(PromiseConstructor.prototype, "constructor", {
      configurable: true,
      get() {
        poisonReads += 1;
        throw new Error("poisoned Promise constructor ran");
      },
    });
    Object.defineProperty(PromiseConstructor.prototype, "then", {
      configurable: true,
      get() {
        poisonReads += 1;
        throw new Error("poisoned Promise then ran");
      },
    });
    Object.defineProperty(Object.prototype, "then", {
      configurable: true,
      value() {
        poisonReads += 1;
      },
    });
  };

  let poisonedPending;
  let poisonedContext;
  try {
    poisonedPending = authority().invoke(
      invocation((context) => {
        poisonedContext = context;
        const raw = PromiseConstructor.resolve(carrier);
        poisonPromise();
        return raw;
      }),
    );
  } finally {
    restorePromisePoison();
  }
  assertSuccessReceipt(
    await poisonedPending,
    carrier,
    poisonedContext.invocation,
  );
  assert.equal(poisonReads, 0);
  assert.equal(manualTimers.activeCount, 0);

  const abortDescriptor = Object.getOwnPropertyDescriptor(
    AbortController.prototype,
    "abort",
  );
  const globalSetTimeoutDescriptor = Object.getOwnPropertyDescriptor(
    globalThis,
    "setTimeout",
  );
  const globalClearTimeoutDescriptor = Object.getOwnPropertyDescriptor(
    globalThis,
    "clearTimeout",
  );
  const abortProvider = deferred();
  let abortSignal;
  const capturedAuthority = authority();
  const capturedPending = capturedAuthority.invoke(
    invocation((context) => {
      abortSignal = context.signal;
      abortSignal.addEventListener(
        "abort",
        () => abortProvider.resolve(carrier),
        { once: true },
      );
      return abortProvider.promise;
    }),
  );
  try {
    Object.defineProperty(AbortController.prototype, "abort", {
      ...abortDescriptor,
      value() {
        throw new Error("poisoned AbortController.abort ran");
      },
    });
    Object.defineProperty(globalThis, "setTimeout", {
      ...globalSetTimeoutDescriptor,
      value() {
        throw new Error("post-import setTimeout poison ran");
      },
    });
    Object.defineProperty(globalThis, "clearTimeout", {
      ...globalClearTimeoutDescriptor,
      value() {
        throw new Error("post-import clearTimeout poison ran");
      },
    });
    manualTimers.advanceBy(10);
  } finally {
    Object.defineProperty(
      AbortController.prototype,
      "abort",
      abortDescriptor,
    );
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
  }
  assertSignal(abortSignal, true);
  await rejectsCall(() => capturedPending, LATE_CODE);
  assert.equal(manualTimers.activeCount, 0);
}

async function realTimers() {
  const clean = authority({ deadlineMilliseconds: 1_000 });
  const cleanCarrier = safeCarrier("real-clean");
  let cleanContext;
  const cleanReceipt = await clean.invoke(
    invocation((context) => {
      cleanContext = context;
      return PromiseConstructor.resolve(cleanCarrier);
    }),
  );
  assertSuccessReceipt(cleanReceipt, cleanCarrier, cleanContext.invocation);
  assertStopped(await clean.stop());

  let lateSignal;
  const lateProvider = deferred();
  const late = authority({
    deadlineMilliseconds: 15,
    settlementGraceMilliseconds: 100,
  });
  const latePending = late.invoke(
    invocation((context) => {
      lateSignal = context.signal;
      lateSignal.addEventListener(
        "abort",
        () => lateProvider.resolve(safeCarrier("real-late")),
        { once: true },
      );
      return lateProvider.promise;
    }),
  );
  await rejectsCall(() => latePending, LATE_CODE);
  assertSignal(lateSignal, true);
  assertStopped(await late.stop());

  let fatalCalls = 0;
  const never = authority({
    deadlineMilliseconds: 15,
    onFatal: Object.freeze(() => {
      fatalCalls += 1;
    }),
    settlementGraceMilliseconds: 15,
  });
  await rejectsCall(
    () => never.invoke(invocation(() => new PromiseConstructor(() => {}))),
    NO_SETTLEMENT_CODE,
  );
  assert.equal(fatalCalls, 1);
  await rejectsCall(() => never.stop(), NO_SETTLEMENT_CODE);
}

const scenarios = Object.freeze({
  "cached-promise": cachedPromise,
  "deadline-late-settlement": deadlineLateSettlement,
  "grace-breach": graceBreach,
  "hostile-boundaries": hostileBoundaries,
  "real-timers": realTimers,
  "shape-and-normal-settlement": shapeAndNormalSettlement,
  "stop-drain": stopDrain,
});

if (scenario !== undefined) {
  assert.equal(typeof scenarios[scenario], "function");
  await scenarios[scenario]();
  assert.equal(utilTypes.isPromise(PromiseConstructor.resolve()), true);
  process.stdout.write(
    `${JSON.stringify({ scenario, status: "passed" })}\n`,
  );
}
