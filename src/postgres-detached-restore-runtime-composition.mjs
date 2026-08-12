import { types as utilTypes } from "node:util";

import {
  createPostgresCheckpointMutationAuthority,
} from "./postgres-checkpoint-mutation-authority.mjs";
import {
  createPostgresDetachedRestoreForegroundComposition,
} from "./postgres-detached-restore-foreground-composition.mjs";
import {
  createPostgresDetachedRestoreStablePlanRegistry,
} from "./postgres-detached-restore-stable-plan-registry.mjs";
import {
  createPostgresDurableStopCaptureComposition,
} from "./postgres-durable-stop-capture-composition.mjs";
import {
  createPostgresLogicalWriterLauncher,
} from "./postgres-logical-writer-launcher.mjs";
import { PostgresOperationGuard } from "./postgres-operation-guard.mjs";
import {
  createPostgresRestoreActivationRecoveryCoordinator,
} from "./postgres-restore-activation-recovery-coordinator.mjs";
import {
  createPostgresRestoreActivationRecoveryService,
} from "./postgres-restore-activation-recovery-service.mjs";
import {
  createPostgresRestoreLifecycleGuard,
} from "./postgres-restore-lifecycle-guard.mjs";
import {
  createPostgresRestoreRecoveryCursorStore,
} from "./postgres-restore-recovery-cursor-store.mjs";
import {
  createPostgresRestoreRecoveryRunner,
} from "./postgres-restore-recovery-runner.mjs";
import {
  createPostgresRestoreRecoveryScheduler,
} from "./postgres-restore-recovery-scheduler.mjs";
import { PostgresSerializableStore } from "./postgres-serializable-store.mjs";
import { PostgresSessionAuthority } from "./postgres-session-authority.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
  STORAGE_CONTRACT_VERSION,
} from "./session-storage-contracts.mjs";
import { StoppedDirectoryBackend } from "./stopped-directory-backend.mjs";
import {
  createPostgresWriterDetachComposition,
} from "./postgres-writer-detach-composition.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const ErrorConstructor = Error;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isProxyValue = utilTypes.isProxy;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIs = Object.is;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const reflectApply = Reflect.apply;
const reflectConstruct = Reflect.construct;
const reflectOwnKeys = Reflect.ownKeys;
const TypeErrorConstructor = TypeError;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;

const PostgresSerializableStoreConstructor = PostgresSerializableStore;
const PostgresSessionAuthorityConstructor = PostgresSessionAuthority;
const PostgresOperationGuardConstructor = PostgresOperationGuard;
const StoppedDirectoryBackendConstructor = StoppedDirectoryBackend;
const migrateSerializableStoreIntrinsic =
  PostgresSerializableStore.prototype.migrate;
const createCheckpointMutationAuthorityIntrinsic =
  createPostgresCheckpointMutationAuthority;
const createDetachedRestoreForegroundIntrinsic =
  createPostgresDetachedRestoreForegroundComposition;
const createDetachedRestoreStablePlanRegistryIntrinsic =
  createPostgresDetachedRestoreStablePlanRegistry;
const createDurableStopCaptureIntrinsic =
  createPostgresDurableStopCaptureComposition;
const createLogicalWriterLauncherIntrinsic =
  createPostgresLogicalWriterLauncher;
const createRestoreActivationRecoveryCoordinatorIntrinsic =
  createPostgresRestoreActivationRecoveryCoordinator;
const createRestoreActivationRecoveryServiceIntrinsic =
  createPostgresRestoreActivationRecoveryService;
const createRestoreLifecycleGuardIntrinsic =
  createPostgresRestoreLifecycleGuard;
const createRestoreRecoveryCursorStoreIntrinsic =
  createPostgresRestoreRecoveryCursorStore;
const createRestoreRecoveryRunnerIntrinsic =
  createPostgresRestoreRecoveryRunner;
const createRestoreRecoverySchedulerIntrinsic =
  createPostgresRestoreRecoveryScheduler;
const createWriterDetachIntrinsic = createPostgresWriterDetachComposition;

const listCurrentWriterLaunchCandidatesIntrinsic =
  PostgresSessionAuthority.prototype
    .listCurrentWriterLaunchRecoveryCandidates;
const finalizeRestoreAttachmentActivationIntrinsic =
  PostgresSessionAuthority.prototype
    .finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt;
const finalizeRestoreDestinationGenerationIntrinsic =
  PostgresSessionAuthority.prototype.finalizeRestoreDestinationGeneration;
const listRestoreAttachmentActivationCandidatesIntrinsic =
  PostgresSessionAuthority.prototype
    .listRestoreAttachmentActivationRecoveryCandidates;
const listRestoreGenerationCandidatesIntrinsic =
  PostgresSessionAuthority.prototype
    .listRestoreDestinationGenerationRecoveryCandidates;
const listWriterLaunchAttemptCandidatesIntrinsic =
  PostgresSessionAuthority.prototype
    .listWriterLaunchAttemptRecoveryCandidates;
const markOperationUncertainIntrinsic =
  PostgresSessionAuthority.prototype.markOperationUncertain;
const readRestoreAttachmentActivationIntrinsic =
  PostgresSessionAuthority.prototype.readRestoreAttachmentActivation;
const readRestoreDestinationGenerationIntrinsic =
  PostgresSessionAuthority.prototype.readRestoreDestinationGeneration;

const OPTION_ERROR_CODE =
  "invalid_postgres_detached_restore_runtime_composition_options";
const MAX_PROTOTYPE_DEPTH = 64;

const TOP_LEVEL_OPTION_KEYS = objectFreeze([
  "authority",
  "foreground",
  "launch",
  "planRegistry",
  "pools",
  "recovery",
  "storage",
]);
const POOL_OPTION_KEYS = objectFreeze([
  "authority",
  "foregroundLifecycle",
  "operation",
  "recoveryLifecycle",
]);
const AUTHORITY_OPTION_KEYS = objectFreeze([
  "maxTransactionAttempts",
  "restoreAttachmentActivationV2FleetCompatible",
  "restoreAttachmentActivationV2GenerationPredecessorFleetCompatible",
  "restoreGenerationV2FleetCompatible",
  "writerLaunchStopV3FleetCompatible",
]);
const STORAGE_OPTION_KEYS = objectFreeze([
  "backendId",
  "lifecycleBackend",
  "publication",
  "resolveArtifactPaths",
  "resolveRestoreDestination",
  "resolveSourceOwnedRoot",
]);
const LAUNCH_OPTION_KEYS = objectFreeze([
  "imageReservations",
  "prepareImageReservation",
  "stoppedWriterCoordinator",
  "supervisor",
]);
const FOREGROUND_OPTION_KEYS = objectFreeze(["fleetCapabilityGate"]);
const PLAN_REGISTRY_OPTION_KEYS = objectFreeze([
  "provisioningFleetCapabilityGate",
]);
const RECOVERY_OPTION_KEYS = objectFreeze([
  "intervalMilliseconds",
  "limits",
  "onStep",
  "recoveryScopeId",
]);
const STORAGE_BACKEND_METHOD_KEYS = objectFreeze([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareWritableAttachment",
  "provisionSession",
  "restoreCheckpoint",
]);
const STORAGE_CAPABILITY_KEYS = objectFreeze([
  "atomicPointInTimeCheckpoint",
  "exclusiveWriterAttachment",
  "fencing",
  "normalDirectoryAttachment",
]);
const FENCING_MODES = objectFreeze([
  "epoch-enforced",
  "manual",
  "verified-detach",
]);

const ERROR_MESSAGES = objectFreeze({
  [OPTION_ERROR_CODE]:
    "PostgreSQL detached restore runtime composition options are invalid",
});
const internalErrors = new WeakSetConstructor();
const runtimeCompositions = new WeakSetConstructor();

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function weakSetAdd(value, entry) {
  callIntrinsic(weakSetAddIntrinsic, value, [entry]);
}

function weakSetHas(value, entry) {
  return callIntrinsic(weakSetHasIntrinsic, value, [entry]);
}

export class PostgresDetachedRestoreRuntimeCompositionError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL detached restore runtime composition error",
      );
    }
    super(ERROR_MESSAGES[code]);
    objectDefineProperty(this, "name", {
      configurable: false,
      enumerable: false,
      value: "PostgresDetachedRestoreRuntimeCompositionError",
      writable: false,
    });
    objectDefineProperty(this, "code", {
      configurable: false,
      enumerable: true,
      value: code,
      writable: false,
    });
    objectDefineProperty(this, "retryable", {
      configurable: false,
      enumerable: true,
      value: false,
      writable: false,
    });
    objectFreeze(this);
  }
}

function fail() {
  const error = new PostgresDetachedRestoreRuntimeCompositionError(
    OPTION_ERROR_CODE,
  );
  weakSetAdd(internalErrors, error);
  throw error;
}

function ensure(condition) {
  if (!condition) fail();
}

function isInternalError(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    weakSetHas(internalErrors, value)
  );
}

function exactFrozenRecord(value) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    ensure(descriptor !== undefined && objectHasOwn(descriptor, "value"));
    objectDefineProperty(result, key, {
      configurable: false,
      enumerable: true,
      value: descriptor.value,
      writable: false,
    });
  }
  return objectFreeze(result);
}

function exactDataObject(value, expectedKeys) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value),
  );
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail();
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      keys.length === expectedKeys.length,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string" && arrayIncludes(expectedKeys, key));
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      fail();
    }
    ensure(
      descriptor?.enumerable === true &&
        objectHasOwn(descriptor, "value"),
    );
    objectDefineProperty(normalized, key, {
      configurable: false,
      enumerable: true,
      value: descriptor.value,
      writable: false,
    });
  }
  for (let index = 0; index < expectedKeys.length; index += 1) {
    ensure(objectHasOwn(normalized, expectedKeys[index]));
  }
  return objectFreeze(normalized);
}

function prototypeDataValue(receiver, name) {
  ensure(
    receiver !== null &&
      (typeof receiver === "object" || typeof receiver === "function") &&
      !isProxyValue(receiver),
  );
  let current = receiver;
  for (let depth = 0; depth <= MAX_PROTOTYPE_DEPTH; depth += 1) {
    if (current === null) fail();
    ensure(!isProxyValue(current));
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, name);
    } catch {
      fail();
    }
    if (descriptor !== undefined) {
      ensure(objectHasOwn(descriptor, "value"));
      return descriptor.value;
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail();
    }
  }
  fail();
}

function trustedFunction(value) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
  );
  return value;
}

function preflightPrototypeChain(value) {
  ensure(
    value !== null &&
      (typeof value === "object" || typeof value === "function") &&
      !isProxyValue(value),
  );
  let current = value;
  for (let depth = 0; depth <= MAX_PROTOTYPE_DEPTH; depth += 1) {
    if (current === null) return;
    ensure(!isProxyValue(current));
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail();
    }
  }
  fail();
}

function preflightPool(pool) {
  preflightPrototypeChain(pool);
  trustedFunction(prototypeDataValue(pool, "connect"));
}

function preflightDistinctPools(pools) {
  ensure(
    !objectIs(pools.authority, pools.operation) &&
      !objectIs(pools.authority, pools.foregroundLifecycle) &&
      !objectIs(pools.authority, pools.recoveryLifecycle) &&
      !objectIs(pools.operation, pools.foregroundLifecycle) &&
      !objectIs(pools.operation, pools.recoveryLifecycle) &&
      !objectIs(pools.foregroundLifecycle, pools.recoveryLifecycle),
  );
  preflightPool(pools.authority);
  preflightPool(pools.operation);
  preflightPool(pools.foregroundLifecycle);
  preflightPool(pools.recoveryLifecycle);
}

function preflightLifecycleBackend(backend) {
  preflightPrototypeChain(backend);
  const backendId = prototypeDataValue(backend, "backendId");
  const capabilities = prototypeDataValue(backend, "capabilities");
  const contractVersion = prototypeDataValue(backend, "contractVersion");
  ensure(typeof backendId === "string");
  ensure(contractVersion === STORAGE_CONTRACT_VERSION);

  const normalizedCapabilities = exactDataObject(
    capabilities,
    STORAGE_CAPABILITY_KEYS,
  );
  ensure(
    typeof normalizedCapabilities.atomicPointInTimeCheckpoint ===
      "boolean" &&
      normalizedCapabilities.exclusiveWriterAttachment === true &&
      normalizedCapabilities.normalDirectoryAttachment === true &&
      arrayIncludes(FENCING_MODES, normalizedCapabilities.fencing),
  );

  for (let index = 0; index < STORAGE_BACKEND_METHOD_KEYS.length; index += 1) {
    trustedFunction(
      prototypeDataValue(backend, STORAGE_BACKEND_METHOD_KEYS[index]),
    );
  }

  const activationVersion = prototypeDataValue(
    backend,
    "restoreAttachmentActivationContractVersion",
  );
  const prepareRestoreAttachment = prototypeDataValue(
    backend,
    "prepareRestoreAttachment",
  );
  ensure(
    activationVersion === RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
  );
  trustedFunction(prepareRestoreAttachment);

}

function ownFrozenDataFunction(receiver, name) {
  ensure(
    receiver !== null &&
      typeof receiver === "object" &&
      !isProxyValue(receiver) &&
      objectIsFrozen(receiver),
  );
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(receiver, name);
  } catch {
    fail();
  }
  ensure(
    descriptor?.enumerable === true &&
      objectHasOwn(descriptor, "value"),
  );
  return trustedFunction(descriptor.value);
}

function callFactory(factory, options) {
  return callIntrinsic(factory, undefined, [options]);
}

function construct(Constructor, options) {
  return callIntrinsic(reflectConstruct, undefined, [
    Constructor,
    [options],
  ]);
}

function normalizeOptions(args) {
  ensure(args.length === 1);
  const options = exactDataObject(args[0], TOP_LEVEL_OPTION_KEYS);
  return exactFrozenRecord({
    authority: exactDataObject(options.authority, AUTHORITY_OPTION_KEYS),
    foreground: exactDataObject(options.foreground, FOREGROUND_OPTION_KEYS),
    launch: exactDataObject(options.launch, LAUNCH_OPTION_KEYS),
    planRegistry: exactDataObject(
      options.planRegistry,
      PLAN_REGISTRY_OPTION_KEYS,
    ),
    pools: exactDataObject(options.pools, POOL_OPTION_KEYS),
    recovery: exactDataObject(options.recovery, RECOVERY_OPTION_KEYS),
    storage: exactDataObject(options.storage, STORAGE_OPTION_KEYS),
  });
}

function createResolveStoppedWriter(launcher) {
  const method = ownFrozenDataFunction(launcher, "resolveStoppedWriter");
  const resolveStoppedWriter = function resolveStoppedWriter(...args) {
    return callIntrinsic(method, launcher, args);
  };
  return objectFreeze(resolveStoppedWriter);
}

function createWriterLaunchFacet(launcher) {
  const reconcileLaunchAttemptMethod = ownFrozenDataFunction(
    launcher,
    "reconcileLaunchAttempt",
  );
  const runLaunchMethod = ownFrozenDataFunction(launcher, "runLaunch");
  const reconcileLaunchAttempt = function reconcileLaunchAttempt(...args) {
    return callIntrinsic(reconcileLaunchAttemptMethod, launcher, args);
  };
  const runLaunch = function runLaunch(...args) {
    return callIntrinsic(runLaunchMethod, launcher, args);
  };
  objectFreeze(reconcileLaunchAttempt);
  objectFreeze(runLaunch);
  return exactFrozenRecord({ reconcileLaunchAttempt, runLaunch });
}

function receiverCallback(method, receiver) {
  trustedFunction(method);
  const callback = function receiverCallback(...args) {
    return callIntrinsic(method, receiver, args);
  };
  return objectFreeze(callback);
}

function createRestoreActivationAuthority(authority) {
  return exactFrozenRecord({
    finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt:
      receiverCallback(
        finalizeRestoreAttachmentActivationIntrinsic,
        authority,
      ),
    finalizeRestoreDestinationGeneration: receiverCallback(
      finalizeRestoreDestinationGenerationIntrinsic,
      authority,
    ),
    markOperationUncertain: receiverCallback(
      markOperationUncertainIntrinsic,
      authority,
    ),
    readRestoreAttachmentActivation: receiverCallback(
      readRestoreAttachmentActivationIntrinsic,
      authority,
    ),
    readRestoreDestinationGeneration: receiverCallback(
      readRestoreDestinationGenerationIntrinsic,
      authority,
    ),
  });
}

function createRecoveryService(authority, coordinator, launcher) {
  const reconcileRestoreAttachmentActivationMethod = ownFrozenDataFunction(
    coordinator,
    "reconcileRestoreAttachmentActivation",
  );
  const reconcileRestoreGenerationMethod = ownFrozenDataFunction(
    coordinator,
    "reconcileRestoreGeneration",
  );
  const reconcileWriterLaunchAttemptMethod = ownFrozenDataFunction(
    launcher,
    "reconcileLaunchAttempt",
  );

  const listCurrentWriterLaunchCandidates =
    function listCurrentWriterLaunchCandidates(...args) {
      return callIntrinsic(
        listCurrentWriterLaunchCandidatesIntrinsic,
        authority,
        args,
      );
    };
  const listRestoreAttachmentActivationCandidates =
    function listRestoreAttachmentActivationCandidates(...args) {
      return callIntrinsic(
        listRestoreAttachmentActivationCandidatesIntrinsic,
        authority,
        args,
      );
    };
  const listRestoreGenerationCandidates =
    function listRestoreGenerationCandidates(...args) {
      return callIntrinsic(
        listRestoreGenerationCandidatesIntrinsic,
        authority,
        args,
      );
    };
  const listWriterLaunchAttemptCandidates =
    function listWriterLaunchAttemptCandidates(...args) {
      return callIntrinsic(
        listWriterLaunchAttemptCandidatesIntrinsic,
        authority,
        args,
      );
    };
  const reconcileRestoreAttachmentActivation =
    function reconcileRestoreAttachmentActivation(...args) {
      return callIntrinsic(
        reconcileRestoreAttachmentActivationMethod,
        coordinator,
        args,
      );
    };
  const reconcileRestoreGeneration =
    function reconcileRestoreGeneration(...args) {
      return callIntrinsic(
        reconcileRestoreGenerationMethod,
        coordinator,
        args,
      );
    };
  const reconcileWriterLaunchAttempt =
    function reconcileWriterLaunchAttempt(...args) {
      return callIntrinsic(
        reconcileWriterLaunchAttemptMethod,
        launcher,
        args,
      );
    };

  objectFreeze(listCurrentWriterLaunchCandidates);
  objectFreeze(listRestoreAttachmentActivationCandidates);
  objectFreeze(listRestoreGenerationCandidates);
  objectFreeze(listWriterLaunchAttemptCandidates);
  objectFreeze(reconcileRestoreAttachmentActivation);
  objectFreeze(reconcileRestoreGeneration);
  objectFreeze(reconcileWriterLaunchAttempt);

  return callFactory(
    createRestoreActivationRecoveryServiceIntrinsic,
    exactFrozenRecord({
      listCurrentWriterLaunchCandidates,
      listRestoreAttachmentActivationCandidates,
      listRestoreGenerationCandidates,
      listWriterLaunchAttemptCandidates,
      reconcileRestoreAttachmentActivation,
      reconcileRestoreGeneration,
      reconcileWriterLaunchAttempt,
    }),
  );
}

function assemble(options) {
  preflightDistinctPools(options.pools);
  preflightLifecycleBackend(options.storage.lifecycleBackend);
  preflightPrototypeChain(options.launch.imageReservations);
  preflightPrototypeChain(options.launch.stoppedWriterCoordinator);
  preflightPrototypeChain(options.storage.publication);

  const store = construct(
    PostgresSerializableStoreConstructor,
    exactFrozenRecord({
      dedicatedPool: options.pools.authority,
      maxTransactionAttempts: options.authority.maxTransactionAttempts,
    }),
  );
  const bootstrap = exactFrozenRecord({
    migrate: receiverCallback(migrateSerializableStoreIntrinsic, store),
  });
  const stablePlanRegistry = callFactory(
    createDetachedRestoreStablePlanRegistryIntrinsic,
    exactFrozenRecord({
      provisioningFleetCapabilityGate:
        options.planRegistry.provisioningFleetCapabilityGate,
      store,
    }),
  );
  const resolveStablePlan = receiverCallback(
    ownFrozenDataFunction(stablePlanRegistry, "resolveStablePlan"),
    stablePlanRegistry,
  );
  const stablePlanProvisioning = exactFrozenRecord({
    provisionStablePlan: receiverCallback(
      ownFrozenDataFunction(stablePlanRegistry, "provisionStablePlan"),
      stablePlanRegistry,
    ),
  });
  const authority = construct(
    PostgresSessionAuthorityConstructor,
    exactFrozenRecord({
      restoreAttachmentActivationV2FleetCompatible:
        options.authority.restoreAttachmentActivationV2FleetCompatible,
      restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
        options.authority
          .restoreAttachmentActivationV2GenerationPredecessorFleetCompatible,
      restoreGenerationV2FleetCompatible:
        options.authority.restoreGenerationV2FleetCompatible,
      store,
      writerLaunchStopV3FleetCompatible:
        options.authority.writerLaunchStopV3FleetCompatible,
    }),
  );

  const operationGuard = construct(
    PostgresOperationGuardConstructor,
    exactFrozenRecord({ dedicatedPool: options.pools.operation }),
  );
  const foregroundOperationGuard = construct(
    PostgresOperationGuardConstructor,
    exactFrozenRecord({
      dedicatedPool: options.pools.foregroundLifecycle,
    }),
  );
  const recoveryOperationGuard = construct(
    PostgresOperationGuardConstructor,
    exactFrozenRecord({ dedicatedPool: options.pools.recoveryLifecycle }),
  );
  const lifecycleGuard = callFactory(
    createRestoreLifecycleGuardIntrinsic,
    exactFrozenRecord({
      foregroundOperationGuard,
      recoveryOperationGuard,
    }),
  );

  const launcher = callFactory(
    createLogicalWriterLauncherIntrinsic,
    exactFrozenRecord({
      authority,
      imageReservations: options.launch.imageReservations,
      operationGuard,
      stoppedWriterCoordinator: options.launch.stoppedWriterCoordinator,
      supervisor: options.launch.supervisor,
    }),
  );
  const writerLaunch = createWriterLaunchFacet(launcher);
  const checkpointMutationAuthority = callFactory(
    createCheckpointMutationAuthorityIntrinsic,
    exactFrozenRecord({
      authority,
      operationGuard,
      resolveArtifactPaths: options.storage.resolveArtifactPaths,
      resolveSourceOwnedRoot: options.storage.resolveSourceOwnedRoot,
    }),
  );
  const backend = construct(
    StoppedDirectoryBackendConstructor,
    exactFrozenRecord({
      backendId: options.storage.backendId,
      coordinator: options.launch.stoppedWriterCoordinator,
      lifecycleBackend: options.storage.lifecycleBackend,
      mutationAuthority: checkpointMutationAuthority,
      publication: options.storage.publication,
      resolveStoppedWriter: createResolveStoppedWriter(launcher),
    }),
  );
  const durableStopCapture = callFactory(
    createDurableStopCaptureIntrinsic,
    exactFrozenRecord({ launcher }),
  );
  const writerDetach = callFactory(
    createWriterDetachIntrinsic,
    exactFrozenRecord({
      authority,
      operationGuard,
      storageBackend: backend,
    }),
  );
  const restoreActivationCoordinator = callFactory(
    createRestoreActivationRecoveryCoordinatorIntrinsic,
    exactFrozenRecord({
      authority: createRestoreActivationAuthority(authority),
      operationGuard,
      publication: options.storage.publication,
      resolveRestoreDestination: options.storage.resolveRestoreDestination,
      storageBackend: backend,
    }),
  );
  const foreground = callFactory(
    createDetachedRestoreForegroundIntrinsic,
    exactFrozenRecord({
      authority,
      captureBackend: backend,
      durableStopCapture,
      fleetCapabilityGate: options.foreground.fleetCapabilityGate,
      launcher,
      lifecycleGuard,
      operationGuard,
      prepareImageReservation: options.launch.prepareImageReservation,
      resolveStablePlan,
      restoreActivationCoordinator,
      writerDetach,
    }),
  );

  const recoveryService = createRecoveryService(
    authority,
    restoreActivationCoordinator,
    launcher,
  );
  const cursorStore = callFactory(
    createRestoreRecoveryCursorStoreIntrinsic,
    exactFrozenRecord({ store }),
  );
  const runner = callFactory(
    createRestoreRecoveryRunnerIntrinsic,
    exactFrozenRecord({
      cursorStore,
      lifecycleGuard,
      limits: options.recovery.limits,
      recoveryScopeId: options.recovery.recoveryScopeId,
      recoveryService,
    }),
  );
  const scheduler = callFactory(
    createRestoreRecoverySchedulerIntrinsic,
    exactFrozenRecord({
      intervalMilliseconds: options.recovery.intervalMilliseconds,
      onStep: options.recovery.onStep,
      runner,
    }),
  );

  const runtime = exactFrozenRecord({
    backend,
    bootstrap,
    foreground,
    scheduler,
    stablePlanProvisioning,
    writerLaunch,
  });
  weakSetAdd(runtimeCompositions, runtime);
  return runtime;
}

export function createPostgresDetachedRestoreRuntimeComposition(...args) {
  try {
    return assemble(normalizeOptions(args));
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail();
  }
}

export function isPostgresDetachedRestoreRuntimeComposition(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      weakSetHas(runtimeCompositions, value)
    );
  } catch {
    return false;
  }
}

objectFreeze(PostgresDetachedRestoreRuntimeCompositionError.prototype);
objectFreeze(PostgresDetachedRestoreRuntimeCompositionError);
objectFreeze(createPostgresDetachedRestoreRuntimeComposition);
objectFreeze(isPostgresDetachedRestoreRuntimeComposition);
