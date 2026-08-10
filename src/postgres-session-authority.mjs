import { createHash } from "node:crypto";
import { isAbsolute, parse, posix as posixPath, resolve } from "node:path";
import { types as utilTypes } from "node:util";

import {
  PostgresSerializableStore,
} from "./postgres-serializable-store.mjs";
import {
  MAX_IMAGE_CONFIG_BYTES,
  MAX_PLATFORM_MANIFEST_BYTES,
} from "./platform-image-reservation.mjs";
import {
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertRestoreAttachmentActivationRequest,
  assertRestoreAttachmentActivationResult,
  assertSessionAttachment,
  assertSessionAttachmentMatches,
  assertSessionManifest,
  assertSessionStorageRef,
  assertStorageBackendCapabilities,
  assertStorageForceFenceRequest,
  assertStorageForceFenceResult,
  assertStorageMutationMatchesLeaseSnapshot,
  assertStorageMutationRequest,
  assertStorageMutationResult,
  STORAGE_CONTRACT_VERSION,
} from "./session-storage-contracts.mjs";

export const SESSION_AUTHORITY_DOCUMENT_VERSION = 3;
export const SESSION_OPERATION_CONFLICT_CLASS = "session-mutation";
export const WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND =
  "writer-attachment-acquire-v1";
export const WRITER_LEASE_RENEW_OPERATION_KIND = "writer-lease-renew-v1";
export const WRITER_RELEASE_OPERATION_KIND = "writer-release-v1";
export const WRITER_FORCE_FENCE_OPERATION_KIND = "writer-force-fence-v1";
export const CHECKPOINT_CAPTURE_OPERATION_KIND = "checkpoint-capture-v1";
export const RESTORE_DESTINATION_GENERATION_OPERATION_KIND =
  "restore-destination-generation-v1";
export const RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND =
  "restore-attachment-activation-v1";
export const WRITER_LAUNCH_ATTEMPT_OPERATION_KIND =
  "writer-launch-attempt-v1";
export const WRITER_LAUNCH_STOP_OPERATION_KIND = "writer-launch-stop-v1";
export const WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON =
  "launch-dispatch-not-started";
export const MAX_WRITER_LEASE_DURATION_MILLISECONDS = 86_400_000;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const REVISION_PATTERN = /^(?:0|[1-9][0-9]{0,18})$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const PERSISTENT_OBJECT_ID_PATTERN =
  /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u;
const OCI_SHA256_DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const WRITER_EPOCH_PATTERN = /^(?:0|[1-9][0-9]{0,19})$/u;
const SENSITIVE_OPERATION_KEY_PATTERN =
  /(?:api[_-]?key|auth(?:json|orization)?|cookie|credential|password|private.?key|secret|token)/iu;
const MAX_POSTGRES_BIGINT = 9_223_372_036_854_775_807n;
const UINT64_MAX = 18_446_744_073_709_551_615n;
const MAX_OPERATION_JSON_BYTES = 65_536;
const MAX_OPERATION_ENVELOPE_JSON_BYTES = 131_072;
const MAX_OPERATION_JSON_DEPTH = 32;
const MAX_OPERATION_JSON_NODES = 4_096;
const MAX_ATTACHMENT_ROOT_PATH_BYTES = 4_096;
const OPERATION_REQUEST_VERSION = 1;
const RESERVATION_PAYLOAD_VERSION = 1;
const OPERATION_RESULT_VERSION = 1;
const WRITER_OPERATION_CONTRACT_VERSION = 1;
const CHECKPOINT_CAPTURE_OPERATION_CONTRACT_VERSION = 1;
const CHECKPOINT_CAPTURE_ATTEMPT_CONTRACT_VERSION = 1;
const CHECKPOINT_CAPTURE_BINDING_CONTRACT_VERSION = 2;
const CHECKPOINT_CATALOGUE_CONTRACT_VERSION = 1;
const CHECKPOINT_MATERIALIZATION_CONTRACT_VERSION = 2;
const RESTORE_DESTINATION_MATERIALIZATION_CONTRACT_VERSION = 3;
const RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION = 1;
const RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2 = 2;
const RESTORE_DESTINATION_GENERATION_BINDING_CONTRACT_VERSION = 1;
export const RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION = 2;
const RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION = 1;
const RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION_V2 = 2;
const DIRECT_OPERATION_ID_CLAIM_TYPE = "direct-operation";
const RESTORE_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE =
  "restore-launch-intent-v2";
const RESTORE_ACTIVATION_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE =
  "restore-activation-launch-intent-v1";
const WRITER_LAUNCH_ATTEMPT_OPERATION_CONTRACT_VERSION = 1;
const WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION = 1;
const WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2 = 2;
const WRITER_LAUNCH_SUPERVISOR_CONTRACT_VERSION = 1;
const WRITER_LAUNCH_POINTER_CONTRACT_VERSION = 1;
const LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION = 1;
const PREVIOUS_SESSION_AUTHORITY_DOCUMENT_VERSION = 2;
const LEGACY_DOCUMENT_KEYS = Object.freeze([
  "activeOperation",
  "attachment",
  "backendCapabilities",
  "documentVersion",
  "launch",
  "lease",
  "lifecycle",
  "manifest",
  "recovery",
  "storageRef",
  "writerEpoch",
]);
const DOCUMENT_KEYS = Object.freeze([
  ...LEGACY_DOCUMENT_KEYS,
  "lastOperation",
]);
const ACTIVE_OPERATION_KEYS = Object.freeze([
  "conflictClass",
  "expectedSessionRevision",
  "kind",
  "operationId",
  "operationRevision",
  "requestSha256",
  "reservationId",
  "state",
]);
const LAST_OPERATION_KEYS = Object.freeze([
  ...ACTIVE_OPERATION_KEYS,
  "resultSha256",
]);
const ROW_KEYS = Object.freeze([
  "created_at",
  "document",
  "revision",
  "session_id",
  "updated_at",
]);
const SNAPSHOT_KEYS = Object.freeze([
  "createdAt",
  "document",
  "revision",
  "sessionId",
  "updatedAt",
]);
const OPERATION_INPUT_KEYS = Object.freeze([
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const OPERATION_TRANSITION_INPUT_KEYS = Object.freeze([
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const WRITER_LAUNCH_STOP_RECONCILE_INPUT_KEYS = Object.freeze([
  "claimToken",
  ...OPERATION_INPUT_KEYS,
]);
const WRITER_LAUNCH_STOP_TRANSITION_INPUT_KEYS = Object.freeze([
  "claimToken",
  ...OPERATION_TRANSITION_INPUT_KEYS,
]);
const OPERATION_CANCELLATION_INPUT_KEYS = Object.freeze([
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "reason",
  "request",
]);
const WRITER_ATTACHMENT_FINALIZATION_INPUT_KEYS = Object.freeze([
  "attachment",
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "mutationResult",
  "operationId",
  "request",
]);
const WRITER_RELEASE_FINALIZATION_INPUT_KEYS = Object.freeze([
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "mutationResult",
  "operationId",
  "request",
]);
const WRITER_FORCE_FENCE_FINALIZATION_INPUT_KEYS = Object.freeze([
  "expectedOperationRevision",
  "expectedSession",
  "fenceResult",
  "kind",
  "operationId",
  "request",
]);
const WRITER_BLOCKED_FINALIZATION_INPUT_KEYS = Object.freeze([
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "reason",
  "request",
]);
const CHECKPOINT_CAPTURE_OPERATION_REQUEST_KEYS = Object.freeze([
  "admission",
  "contractVersion",
  "predeterminedResult",
]);
const CHECKPOINT_CAPTURE_ADMISSION_KEYS = Object.freeze([
  "attachment",
  "captureAttemptId",
  "checkpoint",
  "processIncarnationId",
  "request",
  "stopOperationId",
  "writerIncarnationId",
]);
const CHECKPOINT_CAPTURE_RESULT_KEYS = Object.freeze([
  "checkpoint",
  "mutation",
]);
const CHECKPOINT_CAPTURE_FINALIZATION_INPUT_KEYS = Object.freeze([
  "completion",
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const CHECKPOINT_CAPTURE_READ_KEYS = Object.freeze([
  "checkpoint",
  "request",
]);
const CHECKPOINT_CAPTURE_RECOVERY_LIST_KEYS = Object.freeze([
  "afterSessionId",
  "limit",
]);
const CHECKPOINT_CATALOGUE_READ_KEYS = Object.freeze(["checkpoint"]);
const CHECKPOINT_CAPTURE_COMPLETION_KEYS = Object.freeze([
  "artifactProof",
  "materialization",
  "replayed",
  "result",
]);
const CHECKPOINT_ARTIFACT_PROOF_KEYS = Object.freeze([
  "artifactManifestDigest",
  "captureOperationId",
  "modeledDigest",
]);
const CHECKPOINT_MATERIALIZATION_KEYS = Object.freeze([
  "artifactManifestDigest",
  "contractVersion",
  "modeledDigest",
  "publicationId",
  "publicationKind",
  "stagedRoot",
  "treeIdentityDigest",
]);
const RESTORE_DESTINATION_MATERIALIZATION_KEYS = Object.freeze([
  ...CHECKPOINT_MATERIALIZATION_KEYS,
  "coordinatorBindingSha256",
]);
const CHECKPOINT_STAGED_ROOT_KEYS = Object.freeze([
  "filesystemId",
  "objectIdentityScheme",
  "objectId",
]);
const CHECKPOINT_CAPTURE_BINDING_KEYS = Object.freeze([
  "attachmentId",
  "attachmentOperationId",
  "attachmentProofId",
  "captureAttemptId",
  "checkpoint",
  "contractVersion",
  "processIncarnationId",
  "reservationId",
  "stopOperationId",
  "writerIncarnationId",
]);
const CHECKPOINT_CATALOGUE_DOCUMENT_KEYS = Object.freeze([
  "artifactProof",
  "contractVersion",
  "materialization",
  "result",
]);
const CHECKPOINT_CAPTURE_TERMINAL_RESULT_KEYS = Object.freeze([
  "captureAttemptId",
  "catalogueSha256",
  "checkpointId",
  "outcome",
  "resultVersion",
]);
const RESTORE_GENERATION_OPERATION_REQUEST_KEYS = Object.freeze([
  "admission",
  "contractVersion",
  "predeterminedResult",
]);
const RESTORE_GENERATION_OPERATION_REQUEST_V2_KEYS = Object.freeze([
  "admission",
  "contractVersion",
  "launchIntent",
  "predeterminedResult",
]);
const RESTORE_GENERATION_LAUNCH_INTENT_KEYS = Object.freeze([
  "launchAttemptId",
  "measuredImage",
  "supervisor",
]);
const RESTORE_GENERATION_ADMISSION_KEYS = Object.freeze([
  "checkpoint",
  "request",
]);
const RESTORE_GENERATION_CLAIM_INPUT_KEYS = Object.freeze([
  "destinationIsolationProofId",
  "expectedOperationRevision",
  "expectedSession",
  "generationId",
  "kind",
  "operationId",
  "request",
]);
const RESTORE_GENERATION_RESULT_KEYS = Object.freeze([
  "checkpoint",
  "mutation",
]);
const RESTORE_GENERATION_FINALIZATION_INPUT_KEYS = Object.freeze([
  "completion",
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const RESTORE_GENERATION_LAUNCH_HANDOFF_INPUT_KEYS = Object.freeze([
  "launch",
  "restore",
]);
const RESTORE_GENERATION_COMPLETION_KEYS = Object.freeze([
  "materialization",
  "replayed",
  "result",
]);
const RESTORE_GENERATION_READ_KEYS = Object.freeze([
  "checkpoint",
  "generationId",
  "request",
]);
const RESTORE_GENERATION_RECOVERY_LIST_KEYS = Object.freeze([
  "afterSessionId",
  "limit",
]);
const RESTORE_GENERATION_BINDING_KEYS = Object.freeze([
  "attachment",
  "captureAttemptId",
  "captureOperationId",
  "catalogueSha256",
  "checkpoint",
  "contractVersion",
  "destinationIsolationProofId",
  "destinationState",
  "generationId",
  "request",
  "reservationId",
]);
const RESTORE_GENERATION_DOCUMENT_KEYS = Object.freeze([
  "artifactProof",
  "contractVersion",
  "materialization",
  "result",
]);
const RESTORE_GENERATION_TERMINAL_RESULT_KEYS = Object.freeze([
  "catalogueSha256",
  "checkpointId",
  "generationDocumentSha256",
  "generationId",
  "outcome",
  "resultVersion",
]);
const RESTORE_ATTACHMENT_ACTIVATION_OPERATION_REQUEST_KEYS = Object.freeze([
  "contractVersion",
  "destinationRootPath",
  "generation",
  "holderId",
  "launchIntent",
  "leaseDurationMilliseconds",
  "predecessor",
]);
const RESTORE_ATTACHMENT_ACTIVATION_PREDECESSOR_KEYS = Object.freeze([
  "attachmentId",
  "detachOperationId",
  "stopOperationId",
]);
const RESTORE_ATTACHMENT_ACTIVATION_PREDECESSOR_V2_KEYS = Object.freeze([
  "attachmentId",
  "captureOperationId",
  "detachOperationId",
  "stopOperationId",
]);
const RESTORE_ATTACHMENT_ACTIVATION_CLAIM_INPUT_KEYS = Object.freeze([
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const RESTORE_ATTACHMENT_ACTIVATION_FINALIZATION_INPUT_KEYS = Object.freeze([
  "activationResult",
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const RESTORE_ATTACHMENT_ACTIVATION_TERMINAL_RESULT_KEYS = Object.freeze([
  "activationRequest",
  "activationResult",
  "outcome",
  "resultVersion",
]);
const RESTORE_ATTACHMENT_ACTIVATION_READ_KEYS = Object.freeze([
  "operationId",
]);
const RESTORE_ATTACHMENT_ACTIVATION_RECOVERY_LIST_KEYS = Object.freeze([
  "afterSessionId",
  "limit",
]);
const COMMITTED_WRITER_LAUNCH_STOP_PROOF_KEYS = Object.freeze([
  "after",
  "before",
  "operation",
  "reservation",
]);
const COMMITTED_WRITER_LAUNCH_STOP_OPERATION_KEYS = Object.freeze([
  "conflictClass",
  "createdAt",
  "expectedSession",
  "kind",
  "operationId",
  "request",
  "requestSha256",
  "result",
  "retiredAt",
  "revision",
  "sessionId",
  "state",
  "updatedAt",
]);
const COMMITTED_WRITER_LAUNCH_STOP_RESERVATION_KEYS = Object.freeze([
  "conflictClass",
  "createdAt",
  "expectedSessionRevision",
  "expiresAt",
  "kind",
  "operationId",
  "releasedAt",
  "requestSha256",
  "reservationId",
  "sessionId",
  "state",
  "updatedAt",
]);
const WRITER_LAUNCH_ATTEMPT_OPERATION_REQUEST_KEYS = Object.freeze([
  "attachment",
  "contractVersion",
  "fencingEpoch",
  "generation",
  "lease",
  "measuredImage",
  "supervisor",
]);
const WRITER_LAUNCH_STOP_OPERATION_REQUEST_KEYS = Object.freeze([
  "contractVersion",
  "launch",
]);
const WRITER_LAUNCH_STOP_OPERATION_REQUEST_V2_KEYS = Object.freeze([
  "contractVersion",
  "dispatchClaimSha256",
  "launch",
]);
const WRITER_LAUNCH_STOP_CLAIM_DOMAIN =
  "portable-codex-runtime:writer-launch-stop-claim:v1";
const WRITER_LAUNCH_GENERATION_KEYS = Object.freeze([
  "bindingSha256",
  "checkpointId",
  "claimedAt",
  "committedAt",
  "documentSha256",
  "generationId",
  "operationId",
  "sessionId",
  "state",
]);
const WRITER_LAUNCH_MEASURED_IMAGE_KEYS = Object.freeze([
  "projection",
  "runtimeIdentity",
]);
const WRITER_LAUNCH_IMAGE_PROJECTION_KEYS = Object.freeze([
  "codexSandbox",
  "codexVersion",
  "platformImage",
]);
const WRITER_LAUNCH_PLATFORM_IMAGE_KEYS = Object.freeze([
  "architecture",
  "config",
  "digest",
  "mediaType",
  "os",
  "size",
]);
const WRITER_LAUNCH_IMAGE_CONFIG_KEYS = Object.freeze([
  "digest",
  "mediaType",
  "size",
]);
const WRITER_LAUNCH_RUNTIME_IDENTITY_KEYS = Object.freeze([
  "codexBinaryPath",
  "codexBinarySha256",
  "codexVersion",
  "platformImageDigest",
]);
const WRITER_LAUNCH_SUPERVISOR_KEYS = Object.freeze([
  "contractVersion",
  "supervisorId",
]);
const WRITER_LAUNCH_EVIDENCE_KEYS = Object.freeze([
  "contractVersion",
  "launchAttemptId",
  "processIncarnationId",
  "proofId",
  "status",
  "supervisorId",
  "writerIncarnationId",
]);
const WRITER_LAUNCH_FINALIZATION_INPUT_KEYS = Object.freeze([
  "evidence",
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const WRITER_LAUNCH_STOP_FINALIZATION_INPUT_KEYS = Object.freeze([
  "evidence",
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const WRITER_LAUNCH_TERMINAL_RESULT_KEYS = Object.freeze([
  "evidence",
  "outcome",
  "resultVersion",
]);
const WRITER_LAUNCH_STOP_TERMINAL_RESULT_KEYS = Object.freeze([
  "evidence",
  "outcome",
  "resultVersion",
]);
const WRITER_LAUNCH_POINTER_KEYS = Object.freeze([
  "attachmentId",
  "attachmentSha256",
  "contractVersion",
  "fencingEpoch",
  "generation",
  "launchAttemptId",
  "launchResultSha256",
  "leaseId",
  "leaseSha256",
  "measuredImageSha256",
  "processIncarnationId",
  "startedAt",
  "supervisorId",
  "supervisorProofId",
  "writerIncarnationId",
]);
const WRITER_LAUNCH_READ_KEYS = Object.freeze(["operationId"]);
const WRITER_LAUNCH_RECOVERY_LIST_KEYS = Object.freeze([
  "afterSessionId",
  "limit",
]);
const CURRENT_WRITER_LAUNCH_RECOVERY_LIST_KEYS = Object.freeze([
  "afterSessionId",
  "limit",
]);
const WRITER_ATTACHMENT_REQUEST_KEYS = Object.freeze([
  "contractVersion",
  "holderId",
  "leaseDurationMilliseconds",
]);
const WRITER_LEASE_RENEWAL_REQUEST_KEYS = Object.freeze([
  "contractVersion",
  "leaseDurationMilliseconds",
]);
const WRITER_LIFECYCLE_REQUEST_KEYS = Object.freeze([
  "contractVersion",
  "target",
]);
const OPERATION_REQUEST_KEYS = Object.freeze([
  "conflictClass",
  "expectedSession",
  "payload",
  "requestVersion",
]);
const OPERATION_ROW_KEYS = Object.freeze([
  "created_at",
  "kind",
  "operation_id",
  "request",
  "result",
  "retired_at",
  "revision",
  "session_id",
  "state",
  "updated_at",
]);
const OPERATION_ID_CLAIM_ROW_KEYS = Object.freeze([
  "binding",
  "claim_type",
  "claimant_operation_id",
  "claimed_at",
  "materialized_at",
  "operation_id",
  "session_id",
]);
const RESERVATION_PAYLOAD_KEYS = Object.freeze([
  "conflictClass",
  "requestSha256",
  "reservationVersion",
]);
const RESERVATION_ROW_KEYS = Object.freeze([
  "created_at",
  "expected_session_revision",
  "expires_at",
  "kind",
  "operation_id",
  "payload",
  "released_at",
  "reservation_id",
  "session_id",
  "state",
  "updated_at",
]);
const CAPTURE_ATTEMPT_ROW_KEYS = Object.freeze([
  "binding",
  "capture_attempt_id",
  "claimed_at",
  "operation_id",
  "session_id",
]);
const CAPTURE_ATTEMPT_TOMBSTONE_ROW_KEYS = Object.freeze([
  "capture_attempt_id",
  "operation_id",
  "retired_at",
  "session_id",
  "tombstone",
]);
const CHECKPOINT_CATALOGUE_ROW_KEYS = Object.freeze([
  "capture_attempt_id",
  "checkpoint_id",
  "committed_at",
  "document",
  "session_id",
]);
const RESTORE_GENERATION_ROW_KEYS = Object.freeze([
  "binding",
  "checkpoint_id",
  "claimed_at",
  "committed_at",
  "document",
  "generation_id",
  "operation_id",
  "session_id",
  "state",
]);
const RESTORE_GENERATION_SNAPSHOT_KEYS = Object.freeze([
  "binding",
  "checkpointId",
  "claimedAt",
  "committedAt",
  "document",
  "generationId",
  "operationId",
  "sessionId",
  "state",
]);
const CANCELLATION_RESULT_KEYS = Object.freeze([
  "outcome",
  "reason",
  "resultVersion",
]);
const WRITER_ATTACHMENT_RESULT_KEYS = Object.freeze([
  "attachment",
  "lease",
  "mutationResult",
  "outcome",
  "resultVersion",
]);
const WRITER_LEASE_RENEWAL_RESULT_KEYS = Object.freeze([
  "attachment",
  "lease",
  "outcome",
  "resultVersion",
]);
const WRITER_RELEASE_RESULT_KEYS = Object.freeze([
  "attachment",
  "lease",
  "mutationResult",
  "outcome",
  "resultVersion",
]);
const WRITER_FORCE_FENCE_RESULT_KEYS = Object.freeze([
  "attachment",
  "fenceResult",
  "fenceTarget",
  "lease",
  "outcome",
  "resultVersion",
  "writerEpoch",
]);
const WRITER_BLOCKED_RESULT_KEYS = Object.freeze([
  "attachment",
  "fenceTarget",
  "lease",
  "outcome",
  "reason",
  "resultVersion",
  "writerEpoch",
]);
const ATTACH_MUTATION_REQUEST_KEYS = Object.freeze([
  "backendId",
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "operation",
  "operationId",
  "sessionId",
  "storageId",
  "target",
]);
const WRITER_ATTACH_MUTATION_RESULT_KEYS = Object.freeze([
  "backendId",
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "operation",
  "operationId",
  "proofId",
  "rootPath",
  "sessionId",
  "status",
  "storageId",
  "target",
]);
const DETACH_MUTATION_RESULT_KEYS = Object.freeze([
  "backendId",
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "operation",
  "operationId",
  "proofId",
  "sessionId",
  "status",
  "storageId",
  "target",
]);
const ATTACH_MUTATION_TARGET_KEYS = Object.freeze([
  "attachmentId",
  "kind",
]);
const WRITER_BLOCKED_REASONS = Object.freeze([
  "fence-unavailable",
  "provider-outcome-unresolved",
]);
const ACTIVE_OPERATION_STATES = Object.freeze([
  "prepared",
  "starting",
  "uncertain",
]);
const ERROR_MESSAGES = Object.freeze({
  checkpoint_capture_not_authorized:
    "Checkpoint capture attempt is not actively authorized",
  checkpoint_catalogue_not_found: "Checkpoint catalogue entry was not found",
  checkpoint_identity_conflict:
    "Checkpoint identity is already bound to a different capture",
  invalid_authority_options: "PostgreSQL session authority options are invalid",
  invalid_operation_request: "Session operation request is invalid",
  invalid_session_read: "Session read request is invalid",
  invalid_session_registration: "Session registration request is invalid",
  operation_identity_conflict:
    "Operation ID is already bound to a different canonical request",
  operation_result_conflict:
    "Operation ID is already bound to a different terminal result",
  operation_state_invalid: "Stored operation authority state is invalid",
  operation_transition_conflict:
    "Operation cannot perform the requested phase transition",
  restore_generation_identity_conflict:
    "Restore destination generation identity is already bound to a different operation",
  restore_generation_not_authorized:
    "Restore destination generation is not actively authorized",
  restore_attachment_activation_not_authorized:
    "Restore attachment activation is not actively authorized",
  restore_attachment_activation_v2_fleet_capability_required:
    "Restore attachment activation version 2 requires confirmed fleet compatibility",
  restore_generation_v2_fleet_capability_required:
    "Restore destination generation version 2 requires confirmed fleet compatibility",
  writer_launch_attempt_not_authorized:
    "Writer launch attempt is not actively authorized",
  session_identity_conflict:
    "Session ID is already bound to a different canonical document",
  session_not_found: "Session is not registered",
  session_operation_conflict:
    "Session already has an active conflicting operation",
  session_revision_conflict:
    "Session revision does not match the expected canonical snapshot",
  session_revision_exhausted: "Session revision is exhausted",
  session_state_invalid: "Stored session authority state is invalid",
  writer_epoch_exhausted: "Writer fencing epoch is exhausted",
  writer_fence_unsupported:
    "Storage backend cannot provide automatic physical fencing",
  writer_lease_expired: "Writer lease has expired",
  writer_lease_not_extended:
    "Writer lease renewal would not extend the canonical expiration",
});

const BigIntConstructor = BigInt;
const bigIntToStringIntrinsic = BigInt.prototype.toString;
const ArrayConstructor = Array;
const arrayPrototype = Array.prototype;
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const BufferConstructor = Buffer;
const bufferByteLengthIntrinsic = BufferConstructor.byteLength;
const createHashIntrinsic = createHash;
const DateConstructor = Date;
const dateGetTimeIntrinsic = Date.prototype.getTime;
const dateParseIntrinsic = Date.parse;
const datePrototype = Date.prototype;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const hashPrototype = Object.getPrototypeOf(createHash("sha256"));
const hashDigestIntrinsic = hashPrototype.digest;
const hashUpdateIntrinsic = hashPrototype.update;
const JsonObject = JSON;
const jsonStringifyIntrinsic = JsonObject.stringify;
const isProxyValue = utilTypes.isProxy;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIs = Object.is;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const objectSetPrototypeOf = Object.setPrototypeOf;
const pathIsAbsoluteIntrinsic = isAbsolute;
const pathParseIntrinsic = parse;
const pathResolveIntrinsic = resolve;
const posixPathIsAbsoluteIntrinsic = posixPath.isAbsolute;
const posixPathNormalizeIntrinsic = posixPath.normalize;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const runSerializableIntrinsic =
  PostgresSerializableStore.prototype.runSerializable;
const StringConstructor = String;
const stringCharCodeAtIntrinsic = String.prototype.charCodeAt;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetDeleteIntrinsic = WeakSet.prototype.delete;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;
const OPERATION_VISIBILITY_RETRY = objectFreeze(
  new Error("operation identity visibility retry"),
);

const INSERT_SESSION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.sessions",
    "(session_id, revision, document, created_at, updated_at)",
    "VALUES ($1::uuid, 0, $2::jsonb, $3::timestamptz, $3::timestamptz)",
    "ON CONFLICT (session_id) DO NOTHING",
    "RETURNING session_id, revision, document, created_at, updated_at",
  ].join(" "),
});
const READ_SESSION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT session_id, revision, document, created_at, updated_at",
    "FROM session_authority.sessions",
    "WHERE session_id = $1::uuid",
  ].join(" "),
});
const READ_SESSION_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_SESSION_QUERY.text} FOR UPDATE`,
});
const LIST_CURRENT_WRITER_LAUNCH_FIRST_PAGE_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT session_id, revision, document, created_at, updated_at",
    "FROM session_authority.sessions",
    "ORDER BY session_id ASC",
    "LIMIT $1::integer",
  ].join(" "),
});
const LIST_CURRENT_WRITER_LAUNCH_AFTER_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT session_id, revision, document, created_at, updated_at",
    "FROM session_authority.sessions",
    "WHERE session_id > $1::uuid",
    "ORDER BY session_id ASC",
    "LIMIT $2::integer",
  ].join(" "),
});
const READ_AUTHORITY_CLOCK_QUERY = Object.freeze({
  queryMode: "extended",
  text: "SELECT pg_catalog.clock_timestamp() AS authority_now",
});
const OPERATION_RETURNING_COLUMNS = [
  "operation_id",
  "session_id",
  "kind",
  "request",
  "result",
  "state",
  "revision",
  "created_at",
  "updated_at",
  "retired_at",
].join(", ");
const OPERATION_ID_CLAIM_RETURNING_COLUMNS = [
  "operation_id",
  "session_id",
  "claim_type",
  "claimant_operation_id",
  "binding",
  "claimed_at",
  "materialized_at",
].join(", ");
const RESERVATION_RETURNING_COLUMNS = [
  "reservation_id",
  "operation_id",
  "session_id",
  "kind",
  "expected_session_revision",
  "state",
  "payload",
  "created_at",
  "updated_at",
  "expires_at",
  "released_at",
].join(", ");
const READ_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${OPERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.operation_claims",
    "WHERE operation_id = $1",
  ].join(" "),
});
const READ_OPERATION_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_OPERATION_QUERY.text} FOR UPDATE`,
});
const READ_OPERATION_ID_CLAIM_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${OPERATION_ID_CLAIM_RETURNING_COLUMNS}`,
    "FROM session_authority.operation_id_registry",
    "WHERE operation_id = $1",
  ].join(" "),
});
const READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_OPERATION_ID_CLAIM_QUERY.text} FOR UPDATE`,
});
const LIST_CHECKPOINT_CAPTURE_RECOVERY_FIRST_PAGE_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${OPERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.operation_claims",
    "WHERE kind = 'checkpoint-capture-v1'",
    "AND state IN ('starting', 'uncertain')",
    "AND retired_at IS NULL",
    "ORDER BY session_id ASC",
    "LIMIT $1::integer",
  ].join(" "),
});
const LIST_CHECKPOINT_CAPTURE_RECOVERY_AFTER_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${OPERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.operation_claims",
    "WHERE kind = 'checkpoint-capture-v1'",
    "AND state IN ('starting', 'uncertain')",
    "AND retired_at IS NULL",
    "AND session_id > $1::uuid",
    "ORDER BY session_id ASC",
    "LIMIT $2::integer",
  ].join(" "),
});
const LIST_RESTORE_GENERATION_RECOVERY_FIRST_PAGE_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${OPERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.operation_claims",
    "WHERE kind = 'restore-destination-generation-v1'",
    "AND state IN ('starting', 'uncertain')",
    "AND retired_at IS NULL",
    "ORDER BY session_id ASC",
    "LIMIT $1::integer",
  ].join(" "),
});
const LIST_RESTORE_GENERATION_RECOVERY_AFTER_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${OPERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.operation_claims",
    "WHERE kind = 'restore-destination-generation-v1'",
    "AND state IN ('starting', 'uncertain')",
    "AND retired_at IS NULL",
    "AND session_id > $1::uuid",
    "ORDER BY session_id ASC",
    "LIMIT $2::integer",
  ].join(" "),
});
const LIST_RESTORE_ATTACHMENT_ACTIVATION_RECOVERY_FIRST_PAGE_QUERY =
  Object.freeze({
    queryMode: "extended",
    text: [
      `SELECT ${OPERATION_RETURNING_COLUMNS}`,
      "FROM session_authority.operation_claims",
      "WHERE kind = 'restore-attachment-activation-v1'",
      "AND state IN ('starting', 'uncertain')",
      "AND retired_at IS NULL",
      "ORDER BY session_id ASC",
      "LIMIT $1::integer",
    ].join(" "),
  });
const LIST_RESTORE_ATTACHMENT_ACTIVATION_RECOVERY_AFTER_QUERY =
  Object.freeze({
    queryMode: "extended",
    text: [
      `SELECT ${OPERATION_RETURNING_COLUMNS}`,
      "FROM session_authority.operation_claims",
      "WHERE kind = 'restore-attachment-activation-v1'",
      "AND state IN ('starting', 'uncertain')",
      "AND retired_at IS NULL",
      "AND session_id > $1::uuid",
      "ORDER BY session_id ASC",
      "LIMIT $2::integer",
    ].join(" "),
  });
const LIST_WRITER_LAUNCH_RECOVERY_FIRST_PAGE_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${OPERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.operation_claims",
    "WHERE kind = 'writer-launch-attempt-v1'",
    "AND state IN ('prepared', 'starting', 'uncertain')",
    "AND retired_at IS NULL",
    "ORDER BY session_id ASC",
    "LIMIT $1::integer",
  ].join(" "),
});
const LIST_WRITER_LAUNCH_RECOVERY_AFTER_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${OPERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.operation_claims",
    "WHERE kind = 'writer-launch-attempt-v1'",
    "AND state IN ('prepared', 'starting', 'uncertain')",
    "AND retired_at IS NULL",
    "AND session_id > $1::uuid",
    "ORDER BY session_id ASC",
    "LIMIT $2::integer",
  ].join(" "),
});
const READ_RESERVATION_BY_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${RESERVATION_RETURNING_COLUMNS}`,
    "FROM session_authority.reservations",
    "WHERE operation_id = $1",
  ].join(" "),
});
const READ_RESERVATION_BY_OPERATION_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_RESERVATION_BY_OPERATION_QUERY.text} FOR UPDATE`,
});
const READ_ACTIVE_COUNTS_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "(SELECT count(*)::integer",
    "FROM session_authority.operation_claims",
    "WHERE session_id = $1::uuid AND retired_at IS NULL)",
    "AS operation_count,",
    "(SELECT count(*)::integer",
    "FROM session_authority.reservations",
    "WHERE session_id = $1::uuid AND released_at IS NULL)",
    "AS reservation_count",
  ].join(" "),
});
const INSERT_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "WITH claimed_id AS (",
    "INSERT INTO session_authority.operation_id_registry",
    "(operation_id, session_id, claim_type, claimant_operation_id, binding,",
    "claimed_at, materialized_at)",
    `VALUES ($1, $2::uuid, '${DIRECT_OPERATION_ID_CLAIM_TYPE}', NULL, NULL,`,
    "$5, $5)",
    "ON CONFLICT (operation_id) DO NOTHING",
    "RETURNING operation_id)",
    "INSERT INTO session_authority.operation_claims",
    "(operation_id, session_id, kind, request, result, state, revision,",
    "created_at, updated_at, retired_at)",
    "SELECT $1, $2::uuid, $3, $4::jsonb, NULL, 'prepared', 0, $5, $5, NULL",
    "FROM claimed_id",
    "ON CONFLICT (operation_id) DO NOTHING",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const INSERT_RESTORE_LAUNCH_ID_CLAIM_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.operation_id_registry",
    "(operation_id, session_id, claim_type, claimant_operation_id, binding,",
    "claimed_at, materialized_at)",
    `VALUES ($1, $2::uuid, '${RESTORE_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE}',`,
    "$3, $4::jsonb, $5, NULL)",
    "ON CONFLICT DO NOTHING",
    `RETURNING ${OPERATION_ID_CLAIM_RETURNING_COLUMNS}`,
  ].join(" "),
});
const INSERT_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.operation_id_registry",
    "(operation_id, session_id, claim_type, claimant_operation_id, binding,",
    "claimed_at, materialized_at)",
    `VALUES ($1, $2::uuid, '${RESTORE_ACTIVATION_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE}',`,
    "$3, $4::jsonb, $5, NULL)",
    "ON CONFLICT DO NOTHING",
    `RETURNING ${OPERATION_ID_CLAIM_RETURNING_COLUMNS}`,
  ].join(" "),
});
const INSERT_PRECLAIMED_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.operation_claims",
    "(operation_id, session_id, kind, request, result, state, revision,",
    "created_at, updated_at, retired_at)",
    "SELECT $1::character varying(128), $2::uuid, $3, $4::jsonb,",
    "NULL, 'prepared', 0, $5, $5, NULL",
    "FROM session_authority.operation_id_registry",
    "WHERE operation_id = $1::character varying(128)",
    "AND session_id = $2::uuid",
    `AND claim_type = '${RESTORE_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE}'`,
    "AND claimant_operation_id = $6 AND binding = $7::jsonb",
    "AND materialized_at IS NULL",
    "ON CONFLICT (operation_id) DO NOTHING",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const MATERIALIZE_RESTORE_LAUNCH_ID_CLAIM_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.operation_id_registry",
    "SET materialized_at = $3",
    "WHERE operation_id = $1 AND session_id = $2::uuid",
    `AND claim_type = '${RESTORE_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE}'`,
    "AND claimant_operation_id = $4 AND binding = $5::jsonb",
    "AND materialized_at IS NULL",
    `RETURNING ${OPERATION_ID_CLAIM_RETURNING_COLUMNS}`,
  ].join(" "),
});
const INSERT_PRECLAIMED_RESTORE_ACTIVATION_LAUNCH_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.operation_claims",
    "(operation_id, session_id, kind, request, result, state, revision,",
    "created_at, updated_at, retired_at)",
    "SELECT $1::character varying(128), $2::uuid, $3, $4::jsonb,",
    "NULL, 'prepared', 0, $5, $5, NULL",
    "FROM session_authority.operation_id_registry",
    "WHERE operation_id = $1::character varying(128)",
    "AND session_id = $2::uuid",
    `AND claim_type = '${RESTORE_ACTIVATION_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE}'`,
    "AND claimant_operation_id = $6 AND binding = $7::jsonb",
    "AND materialized_at IS NULL",
    "ON CONFLICT (operation_id) DO NOTHING",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const MATERIALIZE_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.operation_id_registry",
    "SET materialized_at = $3",
    "WHERE operation_id = $1 AND session_id = $2::uuid",
    `AND claim_type = '${RESTORE_ACTIVATION_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE}'`,
    "AND claimant_operation_id = $4 AND binding = $5::jsonb",
    "AND materialized_at IS NULL",
    `RETURNING ${OPERATION_ID_CLAIM_RETURNING_COLUMNS}`,
  ].join(" "),
});
const INSERT_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.reservations",
    "(reservation_id, operation_id, session_id, kind,",
    "expected_session_revision, state, payload, created_at, updated_at,",
    "expires_at, released_at)",
    "VALUES ($1, $2, $3::uuid, $4, $5::bigint, 'prepared',",
    "$6::jsonb, $7, $7, NULL, NULL)",
    "ON CONFLICT (reservation_id) DO NOTHING",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const UPDATE_SESSION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.sessions",
    "SET revision = revision + 1, document = $3::jsonb, updated_at = $4",
    "WHERE session_id = $1::uuid AND revision = $2::bigint",
    "RETURNING session_id, revision, document, created_at, updated_at",
  ].join(" "),
});
const START_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.operation_claims",
    "SET state = 'starting', revision = revision + 1, updated_at = $3",
    "WHERE operation_id = $1 AND revision = $2::bigint",
    "AND state = 'prepared' AND retired_at IS NULL",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const START_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.reservations",
    "SET state = 'starting', updated_at = $2",
    "WHERE operation_id = $1 AND state = 'prepared' AND released_at IS NULL",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const UNCERTAIN_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.operation_claims",
    "SET state = 'uncertain', revision = revision + 1, updated_at = $3",
    "WHERE operation_id = $1 AND revision = $2::bigint",
    "AND state = 'starting' AND retired_at IS NULL",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const UNCERTAIN_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.reservations",
    "SET state = 'uncertain', updated_at = $2",
    "WHERE operation_id = $1 AND state = 'starting' AND released_at IS NULL",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const CANCEL_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.operation_claims",
    "SET state = 'committed', result = $3::jsonb,",
    "revision = revision + 1, updated_at = $4, retired_at = $4",
    "WHERE operation_id = $1 AND revision = $2::bigint",
    "AND state = 'prepared' AND retired_at IS NULL",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const RELEASE_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.reservations",
    "SET state = 'released', updated_at = $2, released_at = $2",
    "WHERE operation_id = $1 AND state = 'prepared' AND released_at IS NULL",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const COMMIT_ACTIVE_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.operation_claims",
    "SET state = 'committed', result = $3::jsonb,",
    "revision = revision + 1, updated_at = $4, retired_at = $4",
    "WHERE operation_id = $1 AND revision = $2::bigint",
    "AND state = $5 AND retired_at IS NULL",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const RELEASE_ACTIVE_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.reservations",
    "SET state = 'released', updated_at = $2, released_at = $2",
    "WHERE operation_id = $1 AND state = $3 AND released_at IS NULL",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const INSERT_COMMITTED_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "WITH claimed_id AS (",
    "INSERT INTO session_authority.operation_id_registry",
    "(operation_id, session_id, claim_type, claimant_operation_id, binding,",
    "claimed_at, materialized_at)",
    `VALUES ($1, $2::uuid, '${DIRECT_OPERATION_ID_CLAIM_TYPE}', NULL, NULL,`,
    "$6, $6)",
    "ON CONFLICT (operation_id) DO NOTHING",
    "RETURNING operation_id)",
    "INSERT INTO session_authority.operation_claims",
    "(operation_id, session_id, kind, request, result, state, revision,",
    "created_at, updated_at, retired_at)",
    "SELECT $1, $2::uuid, $3, $4::jsonb, $5::jsonb, 'committed', 0,",
    "$6, $6, $6",
    "FROM claimed_id",
    "ON CONFLICT (operation_id) DO NOTHING",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const INSERT_RELEASED_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.reservations",
    "(reservation_id, operation_id, session_id, kind,",
    "expected_session_revision, state, payload, created_at, updated_at,",
    "expires_at, released_at)",
    "VALUES ($1, $2, $3::uuid, $4, $5::bigint, 'released',",
    "$6::jsonb, $7, $7, NULL, $7)",
    "ON CONFLICT (reservation_id) DO NOTHING",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const CAPTURE_ATTEMPT_RETURNING_COLUMNS = [
  "capture_attempt_id",
  "operation_id",
  "session_id",
  "binding",
  "claimed_at",
].join(", ");
const CAPTURE_ATTEMPT_TOMBSTONE_RETURNING_COLUMNS = [
  "capture_attempt_id",
  "operation_id",
  "session_id",
  "retired_at",
  "tombstone",
].join(", ");
const CHECKPOINT_CATALOGUE_RETURNING_COLUMNS = [
  "checkpoint_id",
  "session_id",
  "capture_attempt_id",
  "document",
  "committed_at",
].join(", ");
const RESTORE_GENERATION_RETURNING_COLUMNS = [
  "generation_id",
  "operation_id",
  "session_id",
  "checkpoint_id",
  "state",
  "binding",
  "document",
  "claimed_at",
  "committed_at",
].join(", ");
const READ_CAPTURE_ATTEMPT_BY_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${CAPTURE_ATTEMPT_RETURNING_COLUMNS}`,
    "FROM session_authority.capture_attempt_claims",
    "WHERE operation_id = $1",
  ].join(" "),
});
const READ_CAPTURE_ATTEMPT_BY_OPERATION_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_CAPTURE_ATTEMPT_BY_OPERATION_QUERY.text} FOR UPDATE`,
});
const READ_CAPTURE_ATTEMPT_BY_ID_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${CAPTURE_ATTEMPT_RETURNING_COLUMNS}`,
    "FROM session_authority.capture_attempt_claims",
    "WHERE capture_attempt_id = $1::uuid",
  ].join(" "),
});
const INSERT_CAPTURE_ATTEMPT_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.capture_attempt_claims",
    "(capture_attempt_id, operation_id, session_id, binding, claimed_at)",
    "VALUES ($1::uuid, $2, $3::uuid, $4::jsonb, $5)",
    "ON CONFLICT DO NOTHING",
    `RETURNING ${CAPTURE_ATTEMPT_RETURNING_COLUMNS}`,
  ].join(" "),
});
const READ_CAPTURE_ATTEMPT_TOMBSTONE_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${CAPTURE_ATTEMPT_TOMBSTONE_RETURNING_COLUMNS}`,
    "FROM session_authority.capture_attempt_tombstones",
    "WHERE operation_id = $1",
  ].join(" "),
});
const READ_CAPTURE_ATTEMPT_TOMBSTONE_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_CAPTURE_ATTEMPT_TOMBSTONE_QUERY.text} FOR UPDATE`,
});
const READ_CHECKPOINT_CATALOGUE_BY_ID_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${CHECKPOINT_CATALOGUE_RETURNING_COLUMNS}`,
    "FROM session_authority.checkpoint_catalogue",
    "WHERE checkpoint_id = $1",
  ].join(" "),
});
const READ_CHECKPOINT_CATALOGUE_BY_ID_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_CHECKPOINT_CATALOGUE_BY_ID_QUERY.text} FOR UPDATE`,
});
const READ_CHECKPOINT_CATALOGUE_BY_ATTEMPT_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${CHECKPOINT_CATALOGUE_RETURNING_COLUMNS}`,
    "FROM session_authority.checkpoint_catalogue",
    "WHERE capture_attempt_id = $1::uuid",
  ].join(" "),
});
const INSERT_CHECKPOINT_CATALOGUE_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.checkpoint_catalogue",
    "(checkpoint_id, session_id, capture_attempt_id, document, committed_at)",
    "VALUES ($1, $2::uuid, $3::uuid, $4::jsonb, $5)",
    "ON CONFLICT DO NOTHING",
    `RETURNING ${CHECKPOINT_CATALOGUE_RETURNING_COLUMNS}`,
  ].join(" "),
});
const READ_RESTORE_GENERATION_BY_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${RESTORE_GENERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.restore_destination_generations",
    "WHERE operation_id = $1",
  ].join(" "),
});
const READ_RESTORE_GENERATION_BY_OPERATION_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_RESTORE_GENERATION_BY_OPERATION_QUERY.text} FOR UPDATE`,
});
const READ_RESTORE_GENERATION_BY_ID_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${RESTORE_GENERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.restore_destination_generations",
    "WHERE generation_id = $1",
  ].join(" "),
});
const READ_RESTORE_GENERATION_BY_ID_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_RESTORE_GENERATION_BY_ID_QUERY.text} FOR UPDATE`,
});
const INSERT_RESTORE_GENERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.restore_destination_generations",
    "(generation_id, operation_id, session_id, checkpoint_id, state,",
    "binding, document, claimed_at, committed_at)",
    "VALUES ($1, $2, $3::uuid, $4, 'authorized', $5::jsonb, NULL, $6, NULL)",
    "ON CONFLICT DO NOTHING",
    `RETURNING ${RESTORE_GENERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const COMMIT_RESTORE_GENERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.restore_destination_generations",
    "SET state = 'committed', document = $2::jsonb, committed_at = $3",
    "WHERE operation_id = $1 AND state = 'authorized'",
    "AND document IS NULL AND committed_at IS NULL",
    `RETURNING ${RESTORE_GENERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});

export class PostgresSessionAuthorityError extends Error {
  constructor(code) {
    if (!objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported PostgreSQL session authority error");
    }
    super(ERROR_MESSAGES[code]);
    this.name = "PostgresSessionAuthorityError";
    this.code = code;
    this.retryable = false;
    objectFreeze(this);
  }
}

function fail(code) {
  throw new PostgresSessionAuthorityError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function regexpTest(pattern, value) {
  return reflectApply(regexpExecIntrinsic, pattern, [value]) !== null;
}

function sha256(value) {
  const hash = createHashIntrinsic("sha256");
  reflectApply(hashUpdateIntrinsic, hash, [value, "utf8"]);
  return reflectApply(hashDigestIntrinsic, hash, ["hex"]);
}

function operationJournalBindingSha256(value) {
  return sha256(
    `portable-codex-operation-journal-binding-v1\0${canonicalSerialize(value)}\n`,
  );
}

function assertLosslessString(value, code) {
  ensure(typeof value === "string", code);
  for (let index = 0; index < value.length; index += 1) {
    const unit = reflectApply(stringCharCodeAtIntrinsic, value, [index]);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      ensure(index + 1 < value.length, code);
      const next = reflectApply(stringCharCodeAtIntrinsic, value, [index + 1]);
      ensure(next >= 0xdc00 && next <= 0xdfff, code);
      index += 1;
    } else {
      ensure(unit < 0xdc00 || unit > 0xdfff, code);
    }
  }
  return value;
}

function consumeOperationJsonBytes(state, additionalBytes, code) {
  ensure(
    numberIsSafeInteger(additionalBytes) &&
      additionalBytes >= 0 &&
      state.budget.bytes <= MAX_OPERATION_JSON_BYTES - additionalBytes,
    code,
  );
  state.budget.bytes += additionalBytes;
}

function consumeOperationJsonString(state, value, code) {
  ensure(typeof value === "string", code);
  consumeOperationJsonBytes(state, 2, code);
  for (let index = 0; index < value.length; index += 1) {
    const unit = reflectApply(stringCharCodeAtIntrinsic, value, [index]);
    ensure(unit !== 0, code);
    if (unit === 0x22 || unit === 0x5c) {
      consumeOperationJsonBytes(state, 2, code);
    } else if (unit <= 0x1f) {
      const shortEscape =
        unit === 0x08 ||
        unit === 0x09 ||
        unit === 0x0a ||
        unit === 0x0c ||
        unit === 0x0d;
      consumeOperationJsonBytes(state, shortEscape ? 2 : 6, code);
    } else if (unit <= 0x7f) {
      consumeOperationJsonBytes(state, 1, code);
    } else if (unit <= 0x7ff) {
      consumeOperationJsonBytes(state, 2, code);
    } else if (unit >= 0xd800 && unit <= 0xdbff) {
      ensure(index + 1 < value.length, code);
      const next = reflectApply(stringCharCodeAtIntrinsic, value, [index + 1]);
      ensure(next >= 0xdc00 && next <= 0xdfff, code);
      consumeOperationJsonBytes(state, 4, code);
      index += 1;
    } else {
      ensure(unit < 0xdc00 || unit > 0xdfff, code);
      consumeOperationJsonBytes(state, 3, code);
    }
  }
  return value;
}

function canonicalOpaqueId(value, maxLength, code) {
  assertLosslessString(value, code);
  ensure(value.length >= 1 && value.length <= maxLength, code);
  for (let index = 0; index < value.length; index += 1) {
    const unit = reflectApply(stringCharCodeAtIntrinsic, value, [index]);
    const alphaNumeric =
      (unit >= 0x30 && unit <= 0x39) ||
      (unit >= 0x41 && unit <= 0x5a) ||
      (unit >= 0x61 && unit <= 0x7a);
    const punctuation =
      unit === 0x2d || unit === 0x2e || unit === 0x3a || unit === 0x5f;
    ensure(alphaNumeric || punctuation, code);
  }
  return value;
}

function sortedStringKeys(keys, code) {
  const copy = new ArrayConstructor(keys.length);
  objectSetPrototypeOf(copy, null);
  for (let index = 0; index < keys.length; index += 1) {
    ensure(typeof keys[index] === "string", code);
    copy[index] = keys[index];
  }
  for (let outer = 1; outer < copy.length; outer += 1) {
    const value = copy[outer];
    let inner = outer - 1;
    while (inner >= 0 && copy[inner] > value) {
      copy[inner + 1] = copy[inner];
      inner -= 1;
    }
    copy[inner + 1] = value;
  }
  return copy;
}

function canonicalJsonValue(value, state, code) {
  state.budget.nodes += 1;
  ensure(
    state.budget.nodes <= MAX_OPERATION_JSON_NODES &&
      state.depth <= MAX_OPERATION_JSON_DEPTH,
    code,
  );
  if (value === null) {
    consumeOperationJsonBytes(state, 4, code);
    return value;
  }
  if (typeof value === "boolean") {
    consumeOperationJsonBytes(state, value ? 4 : 5, code);
    return value;
  }
  if (typeof value === "string") {
    return consumeOperationJsonString(state, value, code);
  }
  if (typeof value === "number") {
    ensure(numberIsFinite(value), code);
    const normalized = objectIs(value, -0) ? 0 : value;
    const serialized = reflectApply(jsonStringifyIntrinsic, JsonObject, [
      normalized,
    ]);
    consumeOperationJsonBytes(
      state,
      reflectApply(bufferByteLengthIntrinsic, BufferConstructor, [
        serialized,
        "utf8",
      ]),
      code,
    );
    return normalized;
  }
  ensure(
    typeof value === "object" &&
      !isProxyValue(value) &&
      !reflectApply(weakSetHasIntrinsic, state.seen, [value]),
    code,
  );
  reflectApply(weakSetAddIntrinsic, state.seen, [value]);
  let result;
  if (arrayIsArray(value)) {
    ensure(
      numberIsSafeInteger(value.length) &&
        value.length <=
          MAX_OPERATION_JSON_NODES - state.budget.nodes,
      code,
    );
    consumeOperationJsonBytes(
      state,
      2 + (value.length === 0 ? 0 : value.length - 1),
      code,
    );
    const ownKeys = reflectOwnKeys(value);
    ensure(ownKeys.length === value.length + 1, code);
    result = new ArrayConstructor(value.length);
    objectSetPrototypeOf(result, null);
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = objectGetOwnPropertyDescriptor(
        value,
        reflectApply(StringConstructor, undefined, [index]),
      );
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwn(descriptor, "value"),
        code,
      );
      const childState = {
        budget: state.budget,
        depth: state.depth + 1,
        seen: state.seen,
      };
      result[index] = canonicalJsonValue(
        descriptor.value,
        childState,
        code,
      );
    }
    const lengthDescriptor = objectGetOwnPropertyDescriptor(value, "length");
    ensure(
      lengthDescriptor !== undefined &&
        objectHasOwn(lengthDescriptor, "value") &&
        lengthDescriptor.value === value.length,
      code,
    );
  } else {
    let prototype;
    let ownKeys;
    try {
      prototype = objectGetPrototypeOf(value);
      ownKeys = reflectOwnKeys(value);
    } catch {
      fail(code);
    }
    ensure(prototype === objectPrototype || prototype === null, code);
    ensure(
      ownKeys.length <=
        MAX_OPERATION_JSON_NODES - state.budget.nodes,
      code,
    );
    consumeOperationJsonBytes(
      state,
      2 + (ownKeys.length === 0 ? 0 : ownKeys.length - 1),
      code,
    );
    for (let index = 0; index < ownKeys.length; index += 1) {
      const key = consumeOperationJsonString(
        state,
        ownKeys[index],
        code,
      );
      // Session manifests carry the public runtime selector `authMode`.
      // Restore activation must persist that exact manifest, while all other
      // auth- and credential-shaped operation keys remain rejected.
      ensure(
        key === "authMode" ||
          !regexpTest(SENSITIVE_OPERATION_KEY_PATTERN, key),
        code,
      );
      consumeOperationJsonBytes(state, 1, code);
    }
    const keys = sortedStringKeys(ownKeys, code);
    result = objectCreate(null);
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      const descriptor = objectGetOwnPropertyDescriptor(value, key);
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwn(descriptor, "value"),
        code,
      );
      const childState = {
        budget: state.budget,
        depth: state.depth + 1,
        seen: state.seen,
      };
      result[key] = canonicalJsonValue(descriptor.value, childState, code);
    }
  }
  reflectApply(weakSetDeleteIntrinsic, state.seen, [value]);
  return result;
}

function canonicalJsonObject(value, code = "invalid_operation_request") {
  const state = {
    budget: {
      bytes: 0,
      nodes: 0,
    },
    depth: 0,
    seen: new WeakSetConstructor(),
  };
  const canonical = canonicalJsonValue(value, state, code);
  ensure(
    canonical !== null &&
      typeof canonical === "object" &&
      !arrayIsArray(canonical),
    code,
  );
  const serialized = reflectApply(
    jsonStringifyIntrinsic,
    JsonObject,
    [canonical],
  );
  const serializedBytes = reflectApply(
    bufferByteLengthIntrinsic,
    BufferConstructor,
    [serialized, "utf8"],
  );
  ensure(
    typeof serialized === "string" &&
      serializedBytes === state.budget.bytes &&
      serializedBytes <= MAX_OPERATION_JSON_BYTES,
    code,
  );
  return deepFreeze(canonical);
}

function arrayEvery(value, callback) {
  return reflectApply(arrayEveryIntrinsic, value, [callback]);
}

function ownDataValue(value, key, code) {
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, key);
  } catch {
    fail(code);
  }
  ensure(
    descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
    code,
  );
  return descriptor.value;
}

function exactPlainObject(value, keys, code) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxyValue(value) ||
    arrayIsArray(value)
  ) {
    fail(code);
  }
  let actual;
  let prototype;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(prototype === objectPrototype || prototype === null, code);
  ensure(
    actual.length === keys.length &&
      arrayEvery(
        actual,
        (key) =>
          typeof key === "string" &&
          reflectApply(arrayIncludesIntrinsic, keys, [key]),
    ),
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    normalized[key] = ownDataValue(value, key, code);
  }
  return normalized;
}

function canonicalSessionId(value, code) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value), code);
  return value;
}

function canonicalWriterEpoch(value, code, positive = false) {
  ensure(
    typeof value === "string" && regexpTest(WRITER_EPOCH_PATTERN, value),
    code,
  );
  let epoch;
  try {
    epoch = BigIntConstructor(value);
  } catch {
    fail(code);
  }
  ensure(epoch <= UINT64_MAX && (!positive || epoch > 0n), code);
  return value;
}

function nextWriterEpochForCode(value, code) {
  const current = BigIntConstructor(canonicalWriterEpoch(value, code));
  ensure(current < UINT64_MAX, code);
  return reflectApply(bigIntToStringIntrinsic, current + 1n, []);
}

function nextWriterEpoch(value) {
  canonicalWriterEpoch(value, "session_state_invalid");
  if (BigIntConstructor(value) === UINT64_MAX) {
    fail("writer_epoch_exhausted");
  }
  return nextWriterEpochForCode(value, "session_state_invalid");
}

function canonicalLeaseGrant(value, code) {
  let lease;
  try {
    lease = assertLeaseGrant(value);
  } catch {
    fail(code);
  }
  const fencingEpoch = canonicalWriterEpoch(
    lease.fencingEpoch,
    code,
    true,
  );
  const expiresAt = canonicalTimestampString(lease.expiresAt, code);
  return deepFreeze({
    contractVersion: STORAGE_CONTRACT_VERSION,
    sessionId: canonicalSessionId(lease.sessionId, code),
    leaseId: canonicalOpaqueId(lease.leaseId, 128, code),
    holderId: canonicalOpaqueId(lease.holderId, 128, code),
    fencingEpoch,
    expiresAt,
  });
}

function canonicalAttachmentRootPath(value, code) {
  ensure(
    typeof value === "string" &&
      value.length <= MAX_ATTACHMENT_ROOT_PATH_BYTES,
    code,
  );
  ensure(
    reflectApply(bufferByteLengthIntrinsic, BufferConstructor, [
      value,
      "utf8",
    ]) <= MAX_ATTACHMENT_ROOT_PATH_BYTES,
    code,
  );
  for (let index = 0; index < value.length; index += 1) {
    ensure(
      reflectApply(stringCharCodeAtIntrinsic, value, [index]) !== 0,
      code,
    );
  }
  ensure(
    pathIsAbsoluteIntrinsic(value) &&
      pathResolveIntrinsic(value) === value &&
      value !== pathParseIntrinsic(value).root,
    code,
  );
  return value;
}

function attachmentRootPathFromPlainRecord(value, code) {
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  let prototype;
  try {
    prototype = objectGetPrototypeOf(value);
  } catch {
    fail(code);
  }
  ensure(prototype === objectPrototype || prototype === null, code);
  return canonicalAttachmentRootPath(
    ownDataValue(value, "rootPath", code),
    code,
  );
}

function canonicalSessionAttachment(value, code) {
  const rootPath = attachmentRootPathFromPlainRecord(value, code);
  let attachment;
  try {
    attachment = assertSessionAttachment(value);
  } catch {
    fail(code);
  }
  return deepFreeze({
    contractVersion: STORAGE_CONTRACT_VERSION,
    backendId: canonicalOpaqueId(attachment.backendId, 128, code),
    storageId: canonicalOpaqueId(attachment.storageId, 128, code),
    sessionId: canonicalSessionId(attachment.sessionId, code),
    attachmentId: canonicalOpaqueId(attachment.attachmentId, 128, code),
    leaseId: canonicalOpaqueId(attachment.leaseId, 128, code),
    holderId: canonicalOpaqueId(attachment.holderId, 128, code),
    fencingEpoch: canonicalWriterEpoch(
      attachment.fencingEpoch,
      code,
      true,
    ),
    operationId: canonicalOpaqueId(attachment.operationId, 128, code),
    proofId: canonicalOpaqueId(attachment.proofId, 128, code),
    kind: "directory",
    rootPath,
    mode: "read-write",
  });
}

function canonicalLeaseAttachmentBinding({
  attachment,
  lease,
  manifest,
  storageRef,
  code,
}) {
  try {
    assertSessionAttachmentMatches({
      attachment,
      lease,
      manifest,
      storageRef,
    });
  } catch {
    fail(code);
  }
}

function canonicalActiveOperation(value, code) {
  const active = exactPlainObject(value, ACTIVE_OPERATION_KEYS, code);
  const state = canonicalOpaqueId(active.state, 32, code);
  ensure(
    reflectApply(arrayIncludesIntrinsic, ACTIVE_OPERATION_STATES, [state]),
    code,
  );
  const expectedSessionRevision = canonicalRevisionForCode(
    active.expectedSessionRevision,
    code,
  );
  const operationRevision = canonicalRevisionForCode(
    active.operationRevision,
    code,
  );
  const expectedOperationRevision =
    state === "prepared" ? "0" : state === "starting" ? "1" : "2";
  ensure(operationRevision === expectedOperationRevision, code);
  ensure(
    active.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      typeof active.requestSha256 === "string" &&
      regexpTest(SHA256_PATTERN, active.requestSha256),
    code,
  );
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision,
    kind: canonicalOpaqueId(active.kind, 64, code),
    operationId: canonicalOpaqueId(active.operationId, 128, code),
    operationRevision,
    requestSha256: active.requestSha256,
    reservationId: canonicalOpaqueId(active.reservationId, 128, code),
    state,
  });
}

function canonicalLastOperation(value, code) {
  const operation = exactPlainObject(value, LAST_OPERATION_KEYS, code);
  const expectedSessionRevision = canonicalRevisionForCode(
    operation.expectedSessionRevision,
    code,
  );
  const operationRevision = canonicalRevisionForCode(
    operation.operationRevision,
    code,
  );
  ensure(
    operation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      operation.state === "committed" &&
      BigIntConstructor(operationRevision) <= 3n &&
      typeof operation.requestSha256 === "string" &&
      regexpTest(SHA256_PATTERN, operation.requestSha256) &&
      typeof operation.resultSha256 === "string" &&
      regexpTest(SHA256_PATTERN, operation.resultSha256),
    code,
  );
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision,
    kind: canonicalOpaqueId(operation.kind, 64, code),
    operationId: canonicalOpaqueId(operation.operationId, 128, code),
    operationRevision,
    requestSha256: operation.requestSha256,
    reservationId: canonicalOpaqueId(operation.reservationId, 128, code),
    resultSha256: operation.resultSha256,
    state: "committed",
  });
}

function canonicalWriterLaunchPointer(value, sessionId, code) {
  const launch = exactPlainObject(value, WRITER_LAUNCH_POINTER_KEYS, code);
  const generation = canonicalWriterLaunchGeneration(
    launch.generation,
    { sessionId },
    code,
  );
  const startedAt = canonicalTimestampString(launch.startedAt, code);
  ensure(
    launch.contractVersion === WRITER_LAUNCH_POINTER_CONTRACT_VERSION &&
      launch.fencingEpoch ===
        canonicalWriterEpoch(launch.fencingEpoch, code, true) &&
      typeof launch.attachmentSha256 === "string" &&
      regexpTest(SHA256_PATTERN, launch.attachmentSha256) &&
      typeof launch.launchResultSha256 === "string" &&
      regexpTest(SHA256_PATTERN, launch.launchResultSha256) &&
      typeof launch.leaseSha256 === "string" &&
      regexpTest(SHA256_PATTERN, launch.leaseSha256) &&
      typeof launch.measuredImageSha256 === "string" &&
      regexpTest(SHA256_PATTERN, launch.measuredImageSha256) &&
      timestampMilliseconds(startedAt) >=
        timestampMilliseconds(generation.committedAt),
    code,
  );
  return deepFreeze({
    attachmentId: canonicalOpaqueId(launch.attachmentId, 128, code),
    attachmentSha256: launch.attachmentSha256,
    contractVersion: WRITER_LAUNCH_POINTER_CONTRACT_VERSION,
    fencingEpoch: launch.fencingEpoch,
    generation,
    launchAttemptId: canonicalOpaqueId(
      launch.launchAttemptId,
      128,
      code,
    ),
    launchResultSha256: launch.launchResultSha256,
    leaseId: canonicalOpaqueId(launch.leaseId, 128, code),
    leaseSha256: launch.leaseSha256,
    measuredImageSha256: launch.measuredImageSha256,
    processIncarnationId: canonicalOpaqueId(
      launch.processIncarnationId,
      128,
      code,
    ),
    startedAt,
    supervisorId: canonicalOpaqueId(launch.supervisorId, 128, code),
    supervisorProofId: canonicalOpaqueId(
      launch.supervisorProofId,
      128,
      code,
    ),
    writerIncarnationId: canonicalOpaqueId(
      launch.writerIncarnationId,
      128,
      code,
    ),
  });
}

function canonicalDocument(value, code) {
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  const documentVersion = ownDataValue(value, "documentVersion", code);
  ensure(
    documentVersion === LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION ||
      documentVersion === PREVIOUS_SESSION_AUTHORITY_DOCUMENT_VERSION ||
      documentVersion === SESSION_AUTHORITY_DOCUMENT_VERSION,
    code,
  );
  const document = exactPlainObject(
    value,
    documentVersion === LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION
      ? LEGACY_DOCUMENT_KEYS
      : DOCUMENT_KEYS,
    code,
  );
  ensure(
    document.documentVersion === documentVersion &&
      document.recovery === null &&
      (documentVersion === SESSION_AUTHORITY_DOCUMENT_VERSION ||
        document.launch === null),
    code,
  );
  const lifecycle = canonicalOpaqueId(document.lifecycle, 32, code);
  const writerEpoch = canonicalWriterEpoch(document.writerEpoch, code);
  const lease =
    document.lease === null
      ? null
      : canonicalLeaseGrant(document.lease, code);
  const attachment =
    document.attachment === null
      ? null
      : canonicalSessionAttachment(document.attachment, code);
  let manifest;
  let storageRef;
  let backendCapabilities;
  try {
    manifest = assertSessionManifest(document.manifest);
    storageRef = assertSessionStorageRef(document.storageRef);
    backendCapabilities = assertStorageBackendCapabilities(
      document.backendCapabilities,
    );
  } catch {
    fail(code);
  }
  ensure(manifest.sessionId === storageRef.sessionId, code);
  const launch =
    document.launch === null
      ? null
      : canonicalWriterLaunchPointer(
          document.launch,
          manifest.sessionId,
          code,
        );
  const activeOperation =
    document.activeOperation === null
      ? null
      : canonicalActiveOperation(document.activeOperation, code);
  const lastOperation =
    documentVersion === LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION ||
    document.lastOperation === null
      ? null
      : canonicalLastOperation(document.lastOperation, code);
  if (documentVersion === LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION) {
    ensure(
      lifecycle === "DETACHED" &&
        writerEpoch === "0" &&
        lease === null &&
        attachment === null &&
        launch === null,
      code,
    );
  } else if (lifecycle === "DETACHED") {
    ensure(lease === null && attachment === null && launch === null, code);
    ensure(
      activeOperation === null ||
        activeOperation.kind !== WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND ||
        activeOperation.state === "prepared",
      code,
    );
  } else if (lifecycle === "ATTACHING") {
    ensure(
      lease !== null &&
        attachment === null &&
        lease.sessionId === manifest.sessionId &&
        lease.fencingEpoch === writerEpoch &&
        launch === null &&
        activeOperation !== null &&
        (activeOperation.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND ||
          activeOperation.kind ===
            RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND) &&
        (activeOperation.state === "starting" ||
          activeOperation.state === "uncertain"),
      code,
    );
  } else if (lifecycle === "ATTACHED") {
    ensure(
      lease !== null &&
        attachment !== null &&
        lease.sessionId === manifest.sessionId &&
        lease.fencingEpoch === writerEpoch,
      code,
    );
    canonicalLeaseAttachmentBinding({
      attachment,
      lease,
      manifest,
      storageRef,
      code,
    });
  } else if (lifecycle === "RELEASING") {
    ensure(
      lease !== null &&
        attachment !== null &&
        lease.sessionId === manifest.sessionId &&
        lease.fencingEpoch === writerEpoch &&
        launch === null &&
        activeOperation !== null &&
        activeOperation.kind === WRITER_RELEASE_OPERATION_KIND &&
        (activeOperation.state === "starting" ||
          activeOperation.state === "uncertain"),
      code,
    );
    canonicalLeaseAttachmentBinding({
      attachment,
      lease,
      manifest,
      storageRef,
      code,
    });
  } else if (lifecycle === "FENCING") {
    ensure(
      lease !== null &&
        lease.sessionId === manifest.sessionId &&
        BigIntConstructor(writerEpoch) >
          BigIntConstructor(lease.fencingEpoch) &&
        activeOperation !== null &&
        activeOperation.kind === WRITER_FORCE_FENCE_OPERATION_KIND &&
        (activeOperation.state === "starting" ||
          activeOperation.state === "uncertain"),
      code,
    );
    if (attachment !== null) {
      canonicalLeaseAttachmentBinding({
        attachment,
        lease,
        manifest,
        storageRef,
        code,
      });
    }
  } else if (lifecycle === "BLOCKED") {
    ensure(
      lease !== null &&
        lease.sessionId === manifest.sessionId &&
        BigIntConstructor(writerEpoch) >=
          BigIntConstructor(lease.fencingEpoch) &&
        lastOperation !== null &&
        (activeOperation === null ||
          (activeOperation.kind === WRITER_FORCE_FENCE_OPERATION_KIND &&
            activeOperation.state === "prepared")),
      code,
    );
    if (attachment !== null) {
      canonicalLeaseAttachmentBinding({
        attachment,
        lease,
        manifest,
        storageRef,
        code,
      });
    }
  } else {
    fail(code);
  }
  if (
    lifecycle === "ATTACHED" ||
    (lifecycle === "DETACHED" && writerEpoch !== "0")
  ) {
    ensure(lastOperation !== null, code);
  }
  return assembleCanonicalDocument({
    activeOperation,
    attachment,
    backendCapabilities,
    documentVersion,
    lastOperation,
    launch,
    lease,
    lifecycle,
    manifest,
    storageRef,
    writerEpoch,
  });
}

function registrationDocument(options) {
  const normalized = exactPlainObject(
    options,
    ["backendCapabilities", "manifest", "storageRef"],
    "invalid_session_registration",
  );
  let manifest;
  let storageRef;
  let backendCapabilities;
  try {
    manifest = assertSessionManifest(normalized.manifest);
    storageRef = assertSessionStorageRef(normalized.storageRef);
    backendCapabilities = assertStorageBackendCapabilities(
      normalized.backendCapabilities,
    );
  } catch {
    fail("invalid_session_registration");
  }
  ensure(
    manifest.sessionId === storageRef.sessionId,
    "invalid_session_registration",
  );
  return assembleCanonicalDocument({
    backendCapabilities,
    manifest,
    storageRef,
  });
}

function assembleCanonicalDocument({
  activeOperation = null,
  attachment = null,
  backendCapabilities,
  documentVersion = SESSION_AUTHORITY_DOCUMENT_VERSION,
  lastOperation = null,
  launch = null,
  lease = null,
  lifecycle = "DETACHED",
  manifest,
  storageRef,
  writerEpoch = "0",
}) {
  const common = {
    manifest: {
      schemaVersion: manifest.schemaVersion,
      sessionId: manifest.sessionId,
      codex: {
        rootThreadId: manifest.codex.rootThreadId,
        sessionId: manifest.codex.sessionId,
        ephemeral: manifest.codex.ephemeral,
        historyMode: manifest.codex.historyMode,
      },
      runtime: {
        imageDigest: manifest.runtime.imageDigest,
        imageMediaType: manifest.runtime.imageMediaType,
        platform: manifest.runtime.platform,
        codexVersion: manifest.runtime.codexVersion,
        codexSandbox: manifest.runtime.codexSandbox,
      },
      layoutVersion: manifest.layoutVersion,
      authMode: manifest.authMode,
      agents: {
        defaultMaxSubagents: manifest.agents.defaultMaxSubagents,
        maxSubagents: manifest.agents.maxSubagents,
        maxDepth: manifest.agents.maxDepth,
      },
    },
    storageRef: {
      contractVersion: storageRef.contractVersion,
      backendId: storageRef.backendId,
      storageId: storageRef.storageId,
      sessionId: storageRef.sessionId,
    },
    backendCapabilities: {
      atomicPointInTimeCheckpoint:
        backendCapabilities.atomicPointInTimeCheckpoint,
      exclusiveWriterAttachment:
        backendCapabilities.exclusiveWriterAttachment,
      fencing: backendCapabilities.fencing,
      normalDirectoryAttachment:
        backendCapabilities.normalDirectoryAttachment,
    },
    lifecycle,
    writerEpoch,
    lease,
    attachment,
    activeOperation,
  };
  if (documentVersion === LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION) {
    return deepFreeze({
      documentVersion: LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION,
      ...common,
      recovery: null,
      launch: null,
    });
  }
  if (documentVersion === PREVIOUS_SESSION_AUTHORITY_DOCUMENT_VERSION) {
    ensure(launch === null, "session_state_invalid");
    return deepFreeze({
      documentVersion: PREVIOUS_SESSION_AUTHORITY_DOCUMENT_VERSION,
      ...common,
      lastOperation,
      recovery: null,
      launch: null,
    });
  }
  ensure(
    documentVersion === SESSION_AUTHORITY_DOCUMENT_VERSION,
    "session_state_invalid",
  );
  return deepFreeze({
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    ...common,
    lastOperation,
    recovery: null,
    launch,
  });
}

function deepFreeze(value) {
  if (
    value !== null &&
    typeof value === "object" &&
    !objectIsFrozen(value)
  ) {
    const keys = reflectOwnKeys(value);
    for (let index = 0; index < keys.length; index += 1) {
      const descriptor = objectGetOwnPropertyDescriptor(value, keys[index]);
      if (descriptor && objectHasOwn(descriptor, "value")) {
        deepFreeze(descriptor.value);
      }
    }
    objectFreeze(value);
  }
  return value;
}

function nullPrototypeJsonDataTree(value) {
  if (value === null || typeof value !== "object") return value;
  const keys = reflectOwnKeys(value);
  let copy;
  if (arrayIsArray(value)) {
    copy = new ArrayConstructor(value.length);
    objectSetPrototypeOf(copy, null);
  } else {
    copy = objectCreate(null);
  }
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (key === "length") continue;
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    if (descriptor && objectHasOwn(descriptor, "value")) {
      copy[key] = nullPrototypeJsonDataTree(descriptor.value);
    }
  }
  return copy;
}

function canonicalSerialize(document) {
  return reflectApply(jsonStringifyIntrinsic, JsonObject, [
    nullPrototypeJsonDataTree(document),
  ]);
}

function canonicalRevisionForCode(value, code) {
  ensure(
    typeof value === "string" && regexpTest(REVISION_PATTERN, value),
    code,
  );
  let revision;
  try {
    revision = BigIntConstructor(value);
  } catch {
    fail(code);
  }
  ensure(revision <= MAX_POSTGRES_BIGINT, code);
  return value;
}

function canonicalRevision(value) {
  return canonicalRevisionForCode(value, "session_state_invalid");
}

function canonicalTimestamp(value) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === datePrototype,
    "session_state_invalid",
  );
  const milliseconds = reflectApply(dateGetTimeIntrinsic, value, []);
  ensure(numberIsFinite(milliseconds), "session_state_invalid");
  return reflectApply(dateToISOStringIntrinsic, value, []);
}

function canonicalTimestampString(value, code) {
  assertLosslessString(value, code);
  const milliseconds = reflectApply(
    dateParseIntrinsic,
    DateConstructor,
    [value],
  );
  ensure(numberIsFinite(milliseconds), code);
  const normalized = new DateConstructor(milliseconds);
  ensure(
    reflectApply(dateToISOStringIntrinsic, normalized, []) === value,
    code,
  );
  return value;
}

function timestampMilliseconds(value) {
  return reflectApply(dateParseIntrinsic, DateConstructor, [value]);
}

function documentLastOperation(document) {
  return document.documentVersion === LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION
    ? null
    : document.lastOperation;
}

function sameLastOperation(left, right) {
  if (left === null || right === null) return left === right;
  return canonicalSerialize(left) === canonicalSerialize(right);
}

function validateSessionRevisionState(snapshot, code) {
  const revision = BigIntConstructor(snapshot.revision);
  ensure(
    timestampMilliseconds(snapshot.updatedAt) >=
      timestampMilliseconds(snapshot.createdAt),
    code,
  );
  const active = snapshot.document.activeOperation;
  const last = documentLastOperation(snapshot.document);
  if (active === null) {
    if (last === null) {
      ensure(revision === 0n, code);
      ensure(snapshot.createdAt === snapshot.updatedAt, code);
    } else {
      ensure(
        BigIntConstructor(last.expectedSessionRevision) +
          BigIntConstructor(last.operationRevision) +
          1n ===
          revision,
        code,
      );
    }
    return;
  }
  ensure(
    snapshot.document.documentVersion !==
      LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION,
    code,
  );
  const expected = BigIntConstructor(active.expectedSessionRevision);
  const operationRevision = BigIntConstructor(active.operationRevision);
  ensure(
    expected + operationRevision + 1n === revision &&
      revision <= MAX_POSTGRES_BIGINT,
    code,
  );
  if (last === null) {
    ensure(expected === 0n, code);
  } else {
    ensure(
      BigIntConstructor(last.expectedSessionRevision) +
        BigIntConstructor(last.operationRevision) +
        1n ===
        expected,
      code,
    );
  }
}

function rowsFromResult(result, code = "session_state_invalid") {
  ensure(
    result !== null &&
      typeof result === "object" &&
      !isProxyValue(result),
    code,
  );
  const rows = ownDataValue(result, "rows", code);
  ensure(
    arrayIsArray(rows) &&
      !isProxyValue(rows) &&
      (rows.length === 0 || rows.length === 1),
    code,
  );
  for (let index = 0; index < rows.length; index += 1) {
    ownDataValue(
      rows,
      reflectApply(StringConstructor, undefined, [index]),
      code,
    );
  }
  return rows;
}

function pageRowsFromResult(result, maximumRows, code) {
  ensure(
    result !== null &&
      typeof result === "object" &&
      !isProxyValue(result),
    code,
  );
  const rows = ownDataValue(result, "rows", code);
  ensure(
    arrayIsArray(rows) &&
      !isProxyValue(rows) &&
      rows.length <= maximumRows,
    code,
  );
  for (let index = 0; index < rows.length; index += 1) {
    ownDataValue(
      rows,
      reflectApply(StringConstructor, undefined, [index]),
      code,
    );
  }
  return rows;
}

function snapshotFromRow(row, expectedSessionId) {
  const normalized = exactPlainObject(
    row,
    ROW_KEYS,
    "session_state_invalid",
  );
  const sessionId = canonicalSessionId(
    normalized.session_id,
    "session_state_invalid",
  );
  ensure(sessionId === expectedSessionId, "session_state_invalid");
  const revision = canonicalRevision(normalized.revision);
  const document = canonicalDocument(
    normalized.document,
    "session_state_invalid",
  );
  ensure(document.manifest.sessionId === sessionId, "session_state_invalid");
  const createdAt = canonicalTimestamp(normalized.created_at);
  const updatedAt = canonicalTimestamp(normalized.updated_at);
  const snapshot = deepFreeze({
    sessionId,
    revision,
    document,
    createdAt,
    updatedAt,
  });
  validateSessionRevisionState(snapshot, "session_state_invalid");
  return snapshot;
}

function expectedSnapshotFromValue(value, code = "invalid_operation_request") {
  const normalized = exactPlainObject(value, SNAPSHOT_KEYS, code);
  const sessionId = canonicalSessionId(normalized.sessionId, code);
  const revision = canonicalRevisionForCode(normalized.revision, code);
  const document = canonicalDocument(normalized.document, code);
  ensure(document.manifest.sessionId === sessionId, code);
  const createdAt = canonicalTimestampString(normalized.createdAt, code);
  const updatedAt = canonicalTimestampString(normalized.updatedAt, code);
  const snapshot = deepFreeze({
    sessionId,
    revision,
    document,
    createdAt,
    updatedAt,
  });
  validateSessionRevisionState(snapshot, code);
  return snapshot;
}

function canonicalSnapshotBytes(snapshot) {
  return canonicalSerialize({
    sessionId: snapshot.sessionId,
    revision: snapshot.revision,
    document: snapshot.document,
    createdAt: snapshot.createdAt,
    updatedAt: snapshot.updatedAt,
  });
}

function canonicalIdentityBytes(document) {
  return canonicalSerialize({
    manifest: document.manifest,
    storageRef: document.storageRef,
    backendCapabilities: document.backendCapabilities,
  });
}

function canonicalRestoreIdentityBytes(document) {
  return canonicalSerialize({
    manifest: document.manifest,
    storageRef: {
      contractVersion: document.storageRef.contractVersion,
      backendId: document.storageRef.backendId,
      sessionId: document.storageRef.sessionId,
    },
    backendCapabilities: document.backendCapabilities,
  });
}

function canonicalBusinessBytes(document) {
  return canonicalSerialize({
    lifecycle: document.lifecycle,
    writerEpoch: document.writerEpoch,
    lease: document.lease,
    attachment: document.attachment,
    recovery: document.recovery,
    launch: document.launch,
  });
}

function canonicalCheckpointDescriptorForSession(value, expectedSession, code) {
  let checkpoint;
  try {
    checkpoint = assertCheckpointDescriptor(value, {
      manifest: expectedSession.document.manifest,
      storageRef: expectedSession.document.storageRef,
    });
  } catch {
    fail(code);
  }
  return canonicalJsonObject(checkpoint, code);
}

function canonicalRestoreCheckpointForSession(value, expectedSession, code) {
  let checkpoint;
  try {
    checkpoint = assertCheckpointDescriptor(value, {
      manifest: expectedSession.document.manifest,
    });
  } catch {
    fail(code);
  }
  return canonicalJsonObject(checkpoint, code);
}

function canonicalCheckpointMutationRequest(value, code) {
  let request;
  try {
    request = assertStorageMutationRequest(value);
  } catch {
    fail(code);
  }
  ensure(request.operation === "checkpoint", code);
  return canonicalJsonObject(request, code);
}

function canonicalCheckpointMutationResult(value, request, code) {
  let result;
  try {
    result = assertStorageMutationResult(value, { request });
  } catch {
    fail(code);
  }
  ensure(
    result.operation === "checkpoint" &&
      result.status === "checkpoint-created",
    code,
  );
  return canonicalJsonObject(result, code);
}

function checkpointCaptureAdmission(value, expectedSession, code) {
  const admission = exactPlainObject(
    value,
    CHECKPOINT_CAPTURE_ADMISSION_KEYS,
    code,
  );
  const attachment = canonicalSessionAttachment(admission.attachment, code);
  const captureAttemptId = canonicalSessionId(admission.captureAttemptId, code);
  const checkpoint = canonicalCheckpointDescriptorForSession(
    admission.checkpoint,
    expectedSession,
    code,
  );
  const request = canonicalCheckpointMutationRequest(admission.request, code);
  const processIncarnationId = canonicalOpaqueId(
    admission.processIncarnationId,
    128,
    code,
  );
  const stopOperationId = canonicalOpaqueId(
    admission.stopOperationId,
    128,
    code,
  );
  const writerIncarnationId = canonicalOpaqueId(
    admission.writerIncarnationId,
    128,
    code,
  );
  const document = expectedSession.document;
  ensure(
    document.documentVersion !== LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION &&
      document.lifecycle === "ATTACHED" &&
      document.activeOperation === null &&
      document.lease !== null &&
      document.attachment !== null &&
      (document.launch === null ||
        (processIncarnationId ===
          document.launch.processIncarnationId &&
          writerIncarnationId ===
            document.launch.writerIncarnationId)) &&
      canonicalSerialize(attachment) ===
        canonicalSerialize(document.attachment) &&
      request.operationId ===
        canonicalOpaqueId(request.operationId, 128, code) &&
      checkpoint.checkpointClass === "clean" &&
      request.sessionId === expectedSession.sessionId &&
      request.sessionId === checkpoint.sessionId &&
      request.backendId === document.storageRef.backendId &&
      request.storageId === document.storageRef.storageId &&
      request.leaseId === document.lease.leaseId &&
      request.holderId === document.lease.holderId &&
      request.fencingEpoch === document.lease.fencingEpoch &&
      request.fencingEpoch === checkpoint.sourceFencingEpoch &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId,
    code,
  );
  return canonicalJsonObject(
    {
      attachment,
      captureAttemptId,
      checkpoint,
      processIncarnationId,
      request,
      stopOperationId,
      writerIncarnationId,
    },
    code,
  );
}

function predeterminedCheckpointCaptureResult(admission, code) {
  const mutation = canonicalCheckpointMutationResult(
    {
      ...admission.request,
      proofId: `proof-checkpoint-${sha256(
        `checkpoint-capture-proof:${admission.request.operationId}`,
      )}`,
      status: "checkpoint-created",
    },
    admission.request,
    code,
  );
  return canonicalJsonObject(
    {
      checkpoint: admission.checkpoint,
      mutation,
    },
    code,
  );
}

function canonicalCheckpointCaptureResult(value, admission, code) {
  const result = exactPlainObject(
    value,
    CHECKPOINT_CAPTURE_RESULT_KEYS,
    code,
  );
  const checkpoint = canonicalJsonObject(result.checkpoint, code);
  const mutation = canonicalCheckpointMutationResult(
    result.mutation,
    admission.request,
    code,
  );
  const expected = predeterminedCheckpointCaptureResult(admission, code);
  ensure(
    canonicalSerialize(checkpoint) ===
      canonicalSerialize(admission.checkpoint) &&
      canonicalSerialize(mutation) === canonicalSerialize(expected.mutation),
    code,
  );
  return expected;
}

function checkpointCaptureOperationRequest(value, expectedSession, code) {
  const request = exactPlainObject(
    value,
    CHECKPOINT_CAPTURE_OPERATION_REQUEST_KEYS,
    code,
  );
  ensure(
    request.contractVersion ===
      CHECKPOINT_CAPTURE_OPERATION_CONTRACT_VERSION,
    code,
  );
  const admission = checkpointCaptureAdmission(
    request.admission,
    expectedSession,
    code,
  );
  const predeterminedResult = canonicalCheckpointCaptureResult(
    request.predeterminedResult,
    admission,
    code,
  );
  return canonicalJsonObject(
    {
      admission,
      contractVersion: CHECKPOINT_CAPTURE_OPERATION_CONTRACT_VERSION,
      predeterminedResult,
    },
    code,
  );
}

export function createCheckpointCaptureOperationRequest(options) {
  const normalized = exactPlainObject(
    options,
    ["admission", "expectedSession"],
    "invalid_operation_request",
  );
  const expectedSession = expectedSnapshotFromValue(
    normalized.expectedSession,
    "invalid_operation_request",
  );
  const admission = checkpointCaptureAdmission(
    normalized.admission,
    expectedSession,
    "invalid_operation_request",
  );
  return canonicalJsonObject(
    {
      admission,
      contractVersion: CHECKPOINT_CAPTURE_OPERATION_CONTRACT_VERSION,
      predeterminedResult: predeterminedCheckpointCaptureResult(
        admission,
        "invalid_operation_request",
      ),
    },
    "invalid_operation_request",
  );
}

function canonicalRestoreMutationRequest(value, code) {
  let request;
  try {
    request = assertStorageMutationRequest(value);
  } catch {
    fail(code);
  }
  ensure(request.operation === "restore", code);
  return canonicalJsonObject(request, code);
}

function canonicalRestoreMutationResult(value, request, code) {
  let result;
  try {
    result = assertStorageMutationResult(value, { request });
  } catch {
    fail(code);
  }
  ensure(result.operation === "restore" && result.status === "restored", code);
  return canonicalJsonObject(result, code);
}

function restoreGenerationAdmission(value, expectedSession, code) {
  const admission = exactPlainObject(
    value,
    RESTORE_GENERATION_ADMISSION_KEYS,
    code,
  );
  const checkpoint = canonicalRestoreCheckpointForSession(
    admission.checkpoint,
    expectedSession,
    code,
  );
  const request = canonicalRestoreMutationRequest(admission.request, code);
  const document = expectedSession.document;
  ensure(
    document.documentVersion !== LEGACY_SESSION_AUTHORITY_DOCUMENT_VERSION &&
      document.lifecycle === "ATTACHED" &&
      document.activeOperation === null &&
      document.launch === null &&
      document.lease !== null &&
      document.attachment !== null &&
      request.operationId ===
        canonicalOpaqueId(request.operationId, 128, code) &&
      checkpoint.checkpointClass === "clean" &&
      request.sessionId === expectedSession.sessionId &&
      request.sessionId === checkpoint.sessionId &&
      request.backendId === document.storageRef.backendId &&
      request.backendId === checkpoint.backendId &&
      request.storageId === document.storageRef.storageId &&
      request.leaseId === document.lease.leaseId &&
      request.holderId === document.lease.holderId &&
      request.fencingEpoch === document.lease.fencingEpoch &&
      BigIntConstructor(request.fencingEpoch) >
        BigIntConstructor(checkpoint.sourceFencingEpoch) &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId,
    code,
  );
  return canonicalJsonObject(
    {
      checkpoint,
      request,
    },
    code,
  );
}

function predeterminedRestoreGenerationResult(admission, code) {
  const mutation = canonicalRestoreMutationResult(
    {
      ...admission.request,
      proofId: `proof-restore-${sha256(
        `restore-destination-proof:${admission.request.operationId}`,
      )}`,
      status: "restored",
    },
    admission.request,
    code,
  );
  return canonicalJsonObject(
    {
      checkpoint: admission.checkpoint,
      mutation,
    },
    code,
  );
}

function canonicalRestoreGenerationResult(value, admission, code) {
  const result = exactPlainObject(value, RESTORE_GENERATION_RESULT_KEYS, code);
  const checkpoint = canonicalJsonObject(result.checkpoint, code);
  const mutation = canonicalRestoreMutationResult(
    result.mutation,
    admission.request,
    code,
  );
  const expected = predeterminedRestoreGenerationResult(admission, code);
  ensure(
    canonicalSerialize(checkpoint) ===
      canonicalSerialize(admission.checkpoint) &&
      canonicalSerialize(mutation) === canonicalSerialize(expected.mutation),
    code,
  );
  return expected;
}

function canonicalRestoreGenerationLaunchIntent(
  value,
  expectedSession,
  code,
) {
  const launchIntent = exactPlainObject(
    value,
    RESTORE_GENERATION_LAUNCH_INTENT_KEYS,
    code,
  );
  return canonicalJsonObject(
    {
      launchAttemptId: canonicalOpaqueId(
        launchIntent.launchAttemptId,
        128,
        code,
      ),
      measuredImage: canonicalWriterLaunchMeasuredImage(
        launchIntent.measuredImage,
        expectedSession.document.manifest,
        code,
      ),
      supervisor: canonicalWriterLaunchSupervisor(
        launchIntent.supervisor,
        code,
      ),
    },
    code,
  );
}

function restoreGenerationOperationRequest(value, expectedSession, code) {
  const contractVersion = ownDataValue(value, "contractVersion", code);
  ensure(
    contractVersion ===
        RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION ||
      contractVersion ===
        RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2,
    code,
  );
  const request = exactPlainObject(
    value,
    contractVersion ===
      RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION
      ? RESTORE_GENERATION_OPERATION_REQUEST_KEYS
      : RESTORE_GENERATION_OPERATION_REQUEST_V2_KEYS,
    code,
  );
  const admission = restoreGenerationAdmission(
    request.admission,
    expectedSession,
    code,
  );
  const predeterminedResult = canonicalRestoreGenerationResult(
    request.predeterminedResult,
    admission,
    code,
  );
  const launchIntent =
    contractVersion ===
    RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2
      ? canonicalRestoreGenerationLaunchIntent(
          request.launchIntent,
          expectedSession,
          code,
        )
      : null;
  ensure(
    launchIntent === null ||
      launchIntent.launchAttemptId !== admission.request.operationId,
    code,
  );
  return canonicalJsonObject(
    {
      admission,
      contractVersion,
      ...(launchIntent === null ? {} : { launchIntent }),
      predeterminedResult,
    },
    code,
  );
}

export function createRestoreDestinationGenerationOperationRequest(options) {
  const normalized = exactPlainObject(
    options,
    ["admission", "expectedSession"],
    "invalid_operation_request",
  );
  const expectedSession = expectedSnapshotFromValue(
    normalized.expectedSession,
    "invalid_operation_request",
  );
  const admission = restoreGenerationAdmission(
    normalized.admission,
    expectedSession,
    "invalid_operation_request",
  );
  return canonicalJsonObject(
    {
      admission,
      contractVersion:
        RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION,
      predeterminedResult: predeterminedRestoreGenerationResult(
        admission,
        "invalid_operation_request",
      ),
    },
    "invalid_operation_request",
  );
}

export function createRestoreDestinationGenerationOperationRequestV2(
  options,
) {
  const normalized = exactPlainObject(
    options,
    ["admission", "expectedSession", "launchIntent"],
    "invalid_operation_request",
  );
  const expectedSession = expectedSnapshotFromValue(
    normalized.expectedSession,
    "invalid_operation_request",
  );
  const admission = restoreGenerationAdmission(
    normalized.admission,
    expectedSession,
    "invalid_operation_request",
  );
  const launchIntent = canonicalRestoreGenerationLaunchIntent(
    normalized.launchIntent,
    expectedSession,
    "invalid_operation_request",
  );
  ensure(
    launchIntent.launchAttemptId !== admission.request.operationId,
    "invalid_operation_request",
  );
  return canonicalJsonObject(
    {
      admission,
      contractVersion:
        RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2,
      launchIntent,
      predeterminedResult: predeterminedRestoreGenerationResult(
        admission,
        "invalid_operation_request",
      ),
    },
    "invalid_operation_request",
  );
}

function canonicalRestoreAttachmentActivationPredecessor(
  value,
  expectedSession,
  contractVersion,
  code,
) {
  const predecessor = exactPlainObject(
    value,
    contractVersion ===
      RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION
      ? RESTORE_ATTACHMENT_ACTIVATION_PREDECESSOR_KEYS
      : RESTORE_ATTACHMENT_ACTIVATION_PREDECESSOR_V2_KEYS,
    code,
  );
  const lastOperation = expectedSession.document.lastOperation;
  ensure(
    lastOperation !== null &&
      (lastOperation.kind === WRITER_RELEASE_OPERATION_KIND ||
        lastOperation.kind === WRITER_FORCE_FENCE_OPERATION_KIND),
    code,
  );
  const result = deepFreeze({
    attachmentId: canonicalOpaqueId(
      predecessor.attachmentId,
      128,
      code,
    ),
    ...(contractVersion ===
      RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION_V2
      ? {
          captureOperationId: canonicalOpaqueId(
            predecessor.captureOperationId,
            128,
            code,
          ),
        }
      : {}),
    detachOperationId: canonicalOpaqueId(
      predecessor.detachOperationId,
      128,
      code,
    ),
    stopOperationId: canonicalOpaqueId(
      predecessor.stopOperationId,
      128,
      code,
    ),
  });
  ensure(
    result.detachOperationId === lastOperation.operationId &&
      result.stopOperationId !== result.detachOperationId &&
      (contractVersion ===
        RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION ||
        (result.captureOperationId !== result.detachOperationId &&
          result.captureOperationId !== result.stopOperationId)),
    code,
  );
  return result;
}

function restoreAttachmentActivationOperationRequest(
  value,
  expectedSession,
  code,
  epochExhaustionCode = code,
) {
  const request = exactPlainObject(
    value,
    RESTORE_ATTACHMENT_ACTIVATION_OPERATION_REQUEST_KEYS,
    code,
  );
  const contractVersion = ownDataValue(request, "contractVersion", code);
  ensure(
    contractVersion ===
        RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION ||
      contractVersion ===
        RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION_V2,
    code,
  );
  const document = expectedSession.document;
  ensure(
    document.documentVersion === SESSION_AUTHORITY_DOCUMENT_VERSION &&
      document.lifecycle === "DETACHED" &&
      document.lease === null &&
      document.attachment === null &&
      document.launch === null &&
      document.activeOperation === null,
    code,
  );
  ensure(
    BigIntConstructor(document.writerEpoch) < UINT64_MAX,
    epochExhaustionCode,
  );
  const generation = canonicalWriterLaunchGeneration(
    request.generation,
    expectedSession,
    code,
  );
  const launchIntent = canonicalRestoreGenerationLaunchIntent(
    request.launchIntent,
    expectedSession,
    code,
  );
  const predecessor = canonicalRestoreAttachmentActivationPredecessor(
    request.predecessor,
    expectedSession,
    contractVersion,
    code,
  );
  return canonicalJsonObject(
    {
      contractVersion,
      destinationRootPath: canonicalAttachmentRootPath(
        request.destinationRootPath,
        code,
      ),
      generation,
      holderId: canonicalOpaqueId(request.holderId, 128, code),
      launchIntent,
      leaseDurationMilliseconds: canonicalLeaseDuration(
        request.leaseDurationMilliseconds,
        code,
      ),
      predecessor,
    },
    code,
  );
}

export function createRestoreAttachmentActivationOperationRequest(options) {
  const normalized = exactPlainObject(
    options,
    [
      "destinationRootPath",
      "expectedSession",
      "generation",
      "holderId",
      "launchIntent",
      "leaseDurationMilliseconds",
      "predecessor",
    ],
    "invalid_operation_request",
  );
  const expectedSession = expectedSnapshotFromValue(
    normalized.expectedSession,
    "invalid_operation_request",
  );
  return restoreAttachmentActivationOperationRequest(
    {
      contractVersion:
        RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION,
      destinationRootPath: normalized.destinationRootPath,
      generation: writerLaunchGenerationReference(
        normalized.generation,
        expectedSession,
        "invalid_operation_request",
      ),
      holderId: normalized.holderId,
      launchIntent: normalized.launchIntent,
      leaseDurationMilliseconds: normalized.leaseDurationMilliseconds,
      predecessor: normalized.predecessor,
    },
    expectedSession,
    "invalid_operation_request",
  );
}

export function createRestoreAttachmentActivationOperationRequestV2(
  options,
) {
  const normalized = exactPlainObject(
    options,
    [
      "destinationRootPath",
      "expectedSession",
      "generation",
      "holderId",
      "launchIntent",
      "leaseDurationMilliseconds",
      "predecessor",
    ],
    "invalid_operation_request",
  );
  const expectedSession = expectedSnapshotFromValue(
    normalized.expectedSession,
    "invalid_operation_request",
  );
  return restoreAttachmentActivationOperationRequest(
    {
      contractVersion:
        RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION_V2,
      destinationRootPath: normalized.destinationRootPath,
      generation: writerLaunchGenerationReference(
        normalized.generation,
        expectedSession,
        "invalid_operation_request",
      ),
      holderId: normalized.holderId,
      launchIntent: normalized.launchIntent,
      leaseDurationMilliseconds: normalized.leaseDurationMilliseconds,
      predecessor: normalized.predecessor,
    },
    expectedSession,
    "invalid_operation_request",
  );
}

function canonicalWriterLaunchGeneration(value, expectedSession, code) {
  const generation = exactPlainObject(
    value,
    WRITER_LAUNCH_GENERATION_KEYS,
    code,
  );
  const claimedAt = canonicalTimestampString(generation.claimedAt, code);
  const committedAt = canonicalTimestampString(
    generation.committedAt,
    code,
  );
  const sessionId = canonicalSessionId(generation.sessionId, code);
  ensure(
    generation.state === "committed" &&
      sessionId === expectedSession.sessionId &&
      timestampMilliseconds(committedAt) >=
        timestampMilliseconds(claimedAt) &&
      typeof generation.bindingSha256 === "string" &&
      regexpTest(SHA256_PATTERN, generation.bindingSha256) &&
      typeof generation.documentSha256 === "string" &&
      regexpTest(SHA256_PATTERN, generation.documentSha256),
    code,
  );
  return deepFreeze({
    bindingSha256: generation.bindingSha256,
    checkpointId: canonicalOpaqueId(generation.checkpointId, 128, code),
    claimedAt,
    committedAt,
    documentSha256: generation.documentSha256,
    generationId: canonicalOpaqueId(generation.generationId, 128, code),
    operationId: canonicalOpaqueId(generation.operationId, 128, code),
    sessionId,
    state: "committed",
  });
}

function writerLaunchGenerationReference(generation, expectedSession, code) {
  const normalized = exactPlainObject(
    generation,
    RESTORE_GENERATION_SNAPSHOT_KEYS,
    code,
  );
  ensure(
    normalized.state === "committed" &&
      normalized.document !== null &&
      normalized.committedAt !== null,
    code,
  );
  return canonicalWriterLaunchGeneration(
    {
      bindingSha256: sha256(
        canonicalSerialize(canonicalJsonObject(normalized.binding, code)),
      ),
      checkpointId: normalized.checkpointId,
      claimedAt: normalized.claimedAt,
      committedAt: normalized.committedAt,
      documentSha256: sha256(
        canonicalSerialize(canonicalJsonObject(normalized.document, code)),
      ),
      generationId: normalized.generationId,
      operationId: normalized.operationId,
      sessionId: normalized.sessionId,
      state: normalized.state,
    },
    expectedSession,
    code,
  );
}

function canonicalWriterLaunchMeasuredImage(value, manifest, code) {
  const measuredImage = exactPlainObject(
    value,
    WRITER_LAUNCH_MEASURED_IMAGE_KEYS,
    code,
  );
  const projection = exactPlainObject(
    measuredImage.projection,
    WRITER_LAUNCH_IMAGE_PROJECTION_KEYS,
    code,
  );
  const platformImage = exactPlainObject(
    projection.platformImage,
    WRITER_LAUNCH_PLATFORM_IMAGE_KEYS,
    code,
  );
  const config = exactPlainObject(
    platformImage.config,
    WRITER_LAUNCH_IMAGE_CONFIG_KEYS,
    code,
  );
  const runtimeIdentity = exactPlainObject(
    measuredImage.runtimeIdentity,
    WRITER_LAUNCH_RUNTIME_IDENTITY_KEYS,
    code,
  );
  assertLosslessString(platformImage.architecture, code);
  assertLosslessString(platformImage.mediaType, code);
  assertLosslessString(platformImage.os, code);
  assertLosslessString(config.mediaType, code);
  assertLosslessString(runtimeIdentity.codexBinaryPath, code);
  ensure(
    platformImage.architecture.length >= 1 &&
      platformImage.architecture.length <= 64 &&
      platformImage.os.length >= 1 &&
      platformImage.os.length <= 64 &&
      platformImage.mediaType.length >= 1 &&
      platformImage.mediaType.length <= 256 &&
      config.mediaType.length >= 1 &&
      config.mediaType.length <= 256 &&
      numberIsSafeInteger(platformImage.size) &&
      platformImage.size > 0 &&
      platformImage.size <= MAX_PLATFORM_MANIFEST_BYTES &&
      numberIsSafeInteger(config.size) &&
      config.size > 0 &&
      config.size <= MAX_IMAGE_CONFIG_BYTES &&
      typeof platformImage.digest === "string" &&
      regexpTest(OCI_SHA256_DIGEST_PATTERN, platformImage.digest) &&
      typeof config.digest === "string" &&
      regexpTest(OCI_SHA256_DIGEST_PATTERN, config.digest) &&
      typeof runtimeIdentity.codexBinarySha256 === "string" &&
      regexpTest(SHA256_PATTERN, runtimeIdentity.codexBinarySha256) &&
      runtimeIdentity.codexBinaryPath.length > 1 &&
      runtimeIdentity.codexBinaryPath.length <= 4_096 &&
      posixPathIsAbsoluteIntrinsic(runtimeIdentity.codexBinaryPath) &&
      posixPathNormalizeIntrinsic(runtimeIdentity.codexBinaryPath) ===
        runtimeIdentity.codexBinaryPath &&
      projection.codexSandbox === manifest.runtime.codexSandbox &&
      projection.codexVersion === manifest.runtime.codexVersion &&
      `${platformImage.os}/${platformImage.architecture}` ===
        manifest.runtime.platform &&
      platformImage.digest === manifest.runtime.imageDigest &&
      platformImage.mediaType === manifest.runtime.imageMediaType &&
      runtimeIdentity.codexVersion === manifest.runtime.codexVersion &&
      runtimeIdentity.platformImageDigest === platformImage.digest,
    code,
  );
  return canonicalJsonObject(
    {
      projection: {
        codexSandbox: projection.codexSandbox,
        codexVersion: projection.codexVersion,
        platformImage: {
          architecture: platformImage.architecture,
          config: {
            digest: config.digest,
            mediaType: config.mediaType,
            size: config.size,
          },
          digest: platformImage.digest,
          mediaType: platformImage.mediaType,
          os: platformImage.os,
          size: platformImage.size,
        },
      },
      runtimeIdentity: {
        codexBinaryPath: runtimeIdentity.codexBinaryPath,
        codexBinarySha256: runtimeIdentity.codexBinarySha256,
        codexVersion: runtimeIdentity.codexVersion,
        platformImageDigest: runtimeIdentity.platformImageDigest,
      },
    },
    code,
  );
}

function canonicalWriterLaunchSupervisor(value, code) {
  const supervisor = exactPlainObject(
    value,
    WRITER_LAUNCH_SUPERVISOR_KEYS,
    code,
  );
  ensure(
    supervisor.contractVersion ===
      WRITER_LAUNCH_SUPERVISOR_CONTRACT_VERSION,
    code,
  );
  return deepFreeze({
    contractVersion: WRITER_LAUNCH_SUPERVISOR_CONTRACT_VERSION,
    supervisorId: canonicalOpaqueId(supervisor.supervisorId, 128, code),
  });
}

function writerLaunchAttemptOperationRequest(value, expectedSession, code) {
  const request = exactPlainObject(
    value,
    WRITER_LAUNCH_ATTEMPT_OPERATION_REQUEST_KEYS,
    code,
  );
  const document = expectedSession.document;
  const attachment = canonicalSessionAttachment(request.attachment, code);
  const lease = canonicalLeaseGrant(request.lease, code);
  const fencingEpoch = canonicalWriterEpoch(
    request.fencingEpoch,
    code,
    true,
  );
  const generation = canonicalWriterLaunchGeneration(
    request.generation,
    expectedSession,
    code,
  );
  const measuredImage = canonicalWriterLaunchMeasuredImage(
    request.measuredImage,
    document.manifest,
    code,
  );
  const supervisor = canonicalWriterLaunchSupervisor(
    request.supervisor,
    code,
  );
  ensure(
    request.contractVersion ===
        WRITER_LAUNCH_ATTEMPT_OPERATION_CONTRACT_VERSION &&
      (document.documentVersion ===
        PREVIOUS_SESSION_AUTHORITY_DOCUMENT_VERSION ||
        document.documentVersion === SESSION_AUTHORITY_DOCUMENT_VERSION) &&
      document.lifecycle === "ATTACHED" &&
      document.activeOperation === null &&
      document.launch === null &&
      document.attachment !== null &&
      document.lease !== null &&
      fencingEpoch === document.writerEpoch &&
      fencingEpoch === lease.fencingEpoch &&
      canonicalSerialize(attachment) ===
        canonicalSerialize(document.attachment) &&
      canonicalSerialize(lease) === canonicalSerialize(document.lease),
    code,
  );
  return canonicalJsonObject(
    {
      attachment,
      contractVersion: WRITER_LAUNCH_ATTEMPT_OPERATION_CONTRACT_VERSION,
      fencingEpoch,
      generation,
      lease,
      measuredImage,
      supervisor,
    },
    code,
  );
}

export function createWriterLaunchAttemptOperationRequest(options) {
  const normalized = exactPlainObject(
    options,
    ["expectedSession", "generation", "measuredImage", "supervisor"],
    "invalid_operation_request",
  );
  const expectedSession = expectedSnapshotFromValue(
    normalized.expectedSession,
    "invalid_operation_request",
  );
  const document = expectedSession.document;
  ensure(
    document.attachment !== null && document.lease !== null,
    "invalid_operation_request",
  );
  return writerLaunchAttemptOperationRequest(
    {
      attachment: document.attachment,
      contractVersion: WRITER_LAUNCH_ATTEMPT_OPERATION_CONTRACT_VERSION,
      fencingEpoch: document.writerEpoch,
      generation: writerLaunchGenerationReference(
        normalized.generation,
        expectedSession,
        "invalid_operation_request",
      ),
      lease: document.lease,
      measuredImage: normalized.measuredImage,
      supervisor: normalized.supervisor,
    },
    expectedSession,
    "invalid_operation_request",
  );
}

function writerLaunchStopOperationRequest(value, expectedSession, code) {
  const contractVersion = ownDataValue(value, "contractVersion", code);
  ensure(
    contractVersion === WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION ||
      contractVersion ===
        WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2,
    code,
  );
  const request = exactPlainObject(
    value,
    contractVersion === WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION
      ? WRITER_LAUNCH_STOP_OPERATION_REQUEST_KEYS
      : WRITER_LAUNCH_STOP_OPERATION_REQUEST_V2_KEYS,
    code,
  );
  const document = expectedSession.document;
  const launch = canonicalWriterLaunchPointer(
    request.launch,
    expectedSession.sessionId,
    code,
  );
  ensure(
    document.documentVersion === SESSION_AUTHORITY_DOCUMENT_VERSION &&
      document.lifecycle === "ATTACHED" &&
      document.activeOperation === null &&
      document.launch !== null &&
      canonicalSerialize(launch) === canonicalSerialize(document.launch),
    code,
  );
  if (contractVersion === WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2) {
    ensure(
      typeof request.dispatchClaimSha256 === "string" &&
        regexpTest(SHA256_PATTERN, request.dispatchClaimSha256),
      code,
    );
  }
  return canonicalJsonObject(
    {
      contractVersion,
      ...(contractVersion ===
      WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2
        ? { dispatchClaimSha256: request.dispatchClaimSha256 }
        : {}),
      launch,
    },
    code,
  );
}

export function createWriterLaunchStopOperationRequest(options) {
  const hasClaimToken = hasWriterLaunchStopClaimToken(options);
  const normalized = exactPlainObject(
    options,
    hasClaimToken
      ? ["claimToken", "expectedSession"]
      : ["expectedSession"],
    "invalid_operation_request",
  );
  const expectedSession = expectedSnapshotFromValue(
    normalized.expectedSession,
    "invalid_operation_request",
  );
  return writerLaunchStopOperationRequest(
    {
      contractVersion: hasClaimToken
        ? WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2
        : WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION,
      ...(hasClaimToken
        ? {
            dispatchClaimSha256: writerLaunchStopClaimSha256(
              normalized.claimToken,
              "invalid_operation_request",
            ),
          }
        : {}),
      launch: expectedSession.document.launch,
    },
    expectedSession,
    "invalid_operation_request",
  );
}

function canonicalOperationEnvelope(value, code) {
  const normalized = exactPlainObject(value, OPERATION_REQUEST_KEYS, code);
  ensure(
    normalized.requestVersion === OPERATION_REQUEST_VERSION &&
      normalized.conflictClass === SESSION_OPERATION_CONFLICT_CLASS,
    code,
  );
  return deepFreeze({
    requestVersion: OPERATION_REQUEST_VERSION,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: expectedSnapshotFromValue(
      normalized.expectedSession,
      code,
    ),
    payload: canonicalJsonObject(normalized.payload, code),
  });
}

function canonicalOperationBinding(options, keys = OPERATION_INPUT_KEYS) {
  const normalized = exactPlainObject(
    options,
    keys,
    "invalid_operation_request",
  );
  const expectedSession = expectedSnapshotFromValue(
    normalized.expectedSession,
  );
  ensure(
    expectedSession.document.activeOperation === null,
    "invalid_operation_request",
  );
  const operationId = canonicalOpaqueId(
    normalized.operationId,
    128,
    "invalid_operation_request",
  );
  const kind = canonicalOpaqueId(
    normalized.kind,
    64,
    "invalid_operation_request",
  );
  const request = canonicalJsonObject(normalized.request);
  const envelope = deepFreeze({
    requestVersion: OPERATION_REQUEST_VERSION,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession,
    payload: request,
  });
  const serializedEnvelope = canonicalSerialize(envelope);
  ensure(
    reflectApply(bufferByteLengthIntrinsic, BufferConstructor, [
      serializedEnvelope,
      "utf8",
    ]) <= MAX_OPERATION_ENVELOPE_JSON_BYTES,
    "invalid_operation_request",
  );
  const requestSha256 = sha256(serializedEnvelope);
  const reservationId = `reservation-${sha256(operationId)}`;
  const input = deepFreeze({
    envelope,
    expectedSession,
    kind,
    operationId,
    request,
    requestSha256,
    reservationId,
    serializedEnvelope,
  });
  return input;
}

function canonicalOperationInput(options, keys = OPERATION_INPUT_KEYS) {
  const input = canonicalOperationBinding(options, keys);
  validateTypedOperationInput(input);
  return input;
}

/**
 * Canonicalizes the shared identity binding for one session operation.
 * Consumers that validate persisted operation receipts can use this helper
 * without duplicating the authority's snapshot and envelope byte contract.
 */
export function assertSessionOperationBinding(...args) {
  ensure(args.length === 1, "invalid_operation_request");
  const input = canonicalOperationBinding(args[0]);
  return deepFreeze({
    expectedSession: input.expectedSession,
    kind: input.kind,
    operationId: input.operationId,
    request: input.request,
    requestSha256: input.requestSha256,
    reservationId: input.reservationId,
  });
}

/**
 * Canonicalizes and validates one complete session-authority snapshot,
 * including its active or terminal revision-state relation.
 */
export function assertSessionAuthoritySnapshot(...args) {
  ensure(args.length === 1, "invalid_operation_request");
  return expectedSnapshotFromValue(args[0]);
}

function canonicalLeaseDuration(value, code) {
  ensure(
    numberIsSafeInteger(value) &&
      value > 0 &&
      value <= MAX_WRITER_LEASE_DURATION_MILLISECONDS,
    code,
  );
  return value;
}

function canonicalWriterAttachmentTarget(value, code) {
  const target = exactPlainObject(
    value,
    ATTACH_MUTATION_TARGET_KEYS,
    code,
  );
  ensure(target.kind === "attachment", code);
  return deepFreeze({
    attachmentId: canonicalOpaqueId(target.attachmentId, 128, code),
    kind: "attachment",
  });
}

function writerAttachmentRequest(
  input,
  code = "invalid_operation_request",
  epochExhaustionCode = code,
) {
  ensure(
    input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
    code,
  );
  const request = exactPlainObject(
    input.request,
    WRITER_ATTACHMENT_REQUEST_KEYS,
    code,
  );
  ensure(
    request.contractVersion === WRITER_OPERATION_CONTRACT_VERSION &&
      input.expectedSession.document.lifecycle === "DETACHED" &&
      input.expectedSession.document.lease === null &&
      input.expectedSession.document.attachment === null,
    code,
  );
  ensure(
    BigIntConstructor(input.expectedSession.document.writerEpoch) <
      UINT64_MAX,
    epochExhaustionCode,
  );
  return deepFreeze({
    contractVersion: WRITER_OPERATION_CONTRACT_VERSION,
    holderId: canonicalOpaqueId(request.holderId, 128, code),
    leaseDurationMilliseconds: canonicalLeaseDuration(
      request.leaseDurationMilliseconds,
      code,
    ),
  });
}

function writerLeaseRenewalRequest(
  input,
  code = "invalid_operation_request",
) {
  ensure(input.kind === WRITER_LEASE_RENEW_OPERATION_KIND, code);
  const request = exactPlainObject(
    input.request,
    WRITER_LEASE_RENEWAL_REQUEST_KEYS,
    code,
  );
  ensure(
    request.contractVersion === WRITER_OPERATION_CONTRACT_VERSION &&
      input.expectedSession.document.lifecycle === "ATTACHED" &&
      input.expectedSession.document.lease !== null &&
      input.expectedSession.document.attachment !== null,
    code,
  );
  return deepFreeze({
    contractVersion: WRITER_OPERATION_CONTRACT_VERSION,
    leaseDurationMilliseconds: canonicalLeaseDuration(
      request.leaseDurationMilliseconds,
      code,
    ),
  });
}

function writerReleaseRequest(
  input,
  code = "invalid_operation_request",
) {
  ensure(input.kind === WRITER_RELEASE_OPERATION_KIND, code);
  const request = exactPlainObject(
    input.request,
    WRITER_LIFECYCLE_REQUEST_KEYS,
    code,
  );
  const document = input.expectedSession.document;
  const target = canonicalWriterAttachmentTarget(request.target, code);
  ensure(
    request.contractVersion === WRITER_OPERATION_CONTRACT_VERSION &&
      document.lifecycle === "ATTACHED" &&
      document.launch === null &&
      document.lease !== null &&
      document.attachment !== null &&
      target.attachmentId === document.attachment.attachmentId,
    code,
  );
  return deepFreeze({
    contractVersion: WRITER_OPERATION_CONTRACT_VERSION,
    target,
  });
}

function writerForceFenceOperationRequest(
  input,
  code = "invalid_operation_request",
  epochExhaustionCode = code,
) {
  ensure(input.kind === WRITER_FORCE_FENCE_OPERATION_KIND, code);
  const request = exactPlainObject(
    input.request,
    WRITER_LIFECYCLE_REQUEST_KEYS,
    code,
  );
  const document = input.expectedSession.document;
  const target = canonicalWriterAttachmentTarget(request.target, code);
  ensure(
    request.contractVersion === WRITER_OPERATION_CONTRACT_VERSION &&
      (document.lifecycle === "ATTACHED" ||
        document.lifecycle === "BLOCKED") &&
      document.lease !== null,
    code,
  );
  if (document.attachment !== null) {
    ensure(
      target.attachmentId === document.attachment.attachmentId,
      code,
    );
  } else if (
    document.lastOperation?.kind ===
    WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND
  ) {
    ensure(
      target.attachmentId ===
        attachmentIdForOperation(document.lastOperation.operationId),
      code,
    );
  }
  ensure(
    BigIntConstructor(document.writerEpoch) < UINT64_MAX,
    epochExhaustionCode,
  );
  return deepFreeze({
    contractVersion: WRITER_OPERATION_CONTRACT_VERSION,
    target,
  });
}

function validateTypedOperationInput(input) {
  if (input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND) {
    writerAttachmentRequest(
      input,
      "invalid_operation_request",
      "writer_epoch_exhausted",
    );
  } else if (input.kind === WRITER_LEASE_RENEW_OPERATION_KIND) {
    writerLeaseRenewalRequest(input);
  } else if (input.kind === WRITER_RELEASE_OPERATION_KIND) {
    writerReleaseRequest(input);
  } else if (input.kind === WRITER_FORCE_FENCE_OPERATION_KIND) {
    writerForceFenceOperationRequest(
      input,
      "invalid_operation_request",
      "writer_epoch_exhausted",
    );
  } else if (input.kind === CHECKPOINT_CAPTURE_OPERATION_KIND) {
    const request = checkpointCaptureOperationRequest(
      input.request,
      input.expectedSession,
      "invalid_operation_request",
    );
    ensure(
      input.operationId === request.admission.request.operationId,
      "invalid_operation_request",
    );
  } else if (
    input.kind === RESTORE_DESTINATION_GENERATION_OPERATION_KIND
  ) {
    const request = restoreGenerationOperationRequest(
      input.request,
      input.expectedSession,
      "invalid_operation_request",
    );
    ensure(
      input.operationId === request.admission.request.operationId,
      "invalid_operation_request",
    );
  } else if (
    input.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND
  ) {
    const request = restoreAttachmentActivationOperationRequest(
      input.request,
      input.expectedSession,
      "invalid_operation_request",
      "writer_epoch_exhausted",
    );
    ensure(
      input.operationId !== request.launchIntent.launchAttemptId,
      "invalid_operation_request",
    );
  } else if (input.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND) {
    writerLaunchAttemptOperationRequest(
      input.request,
      input.expectedSession,
      "invalid_operation_request",
    );
  } else if (input.kind === WRITER_LAUNCH_STOP_OPERATION_KIND) {
    writerLaunchStopOperationRequest(
      input.request,
      input.expectedSession,
      "invalid_operation_request",
    );
  }
}

function leaseIdForOperation(operationId) {
  return `lease-${sha256(`writer-lease:${operationId}`)}`;
}

function attachmentIdForOperation(operationId) {
  return `attachment-${sha256(`writer-attachment:${operationId}`)}`;
}

function timestampAfter(value, milliseconds, code) {
  const base = timestampMilliseconds(value);
  const result = base + milliseconds;
  ensure(numberIsFinite(base) && numberIsFinite(result), code);
  const date = new DateConstructor(result);
  return reflectApply(dateToISOStringIntrinsic, date, []);
}

function writerLeaseFor(input, authorityNow, fencingEpoch, code) {
  const request = writerAttachmentRequest(input, code);
  return canonicalLeaseGrant(
    {
      contractVersion: STORAGE_CONTRACT_VERSION,
      sessionId: input.expectedSession.sessionId,
      leaseId: leaseIdForOperation(input.operationId),
      holderId: request.holderId,
      fencingEpoch,
      expiresAt: timestampAfter(
        authorityNow,
        request.leaseDurationMilliseconds,
        code,
      ),
    },
    code,
  );
}

function canonicalAttachMutationRequest(value, code) {
  const normalized = exactPlainObject(
    value,
    ATTACH_MUTATION_REQUEST_KEYS,
    code,
  );
  const target = exactPlainObject(
    normalized.target,
    ATTACH_MUTATION_TARGET_KEYS,
    code,
  );
  let mutation;
  try {
    mutation = assertStorageMutationRequest(value);
  } catch {
    fail(code);
  }
  ensure(
    mutation.operation === "attach" && target.kind === "attachment",
    code,
  );
  return deepFreeze({
    contractVersion: STORAGE_CONTRACT_VERSION,
    backendId: canonicalOpaqueId(mutation.backendId, 128, code),
    storageId: canonicalOpaqueId(mutation.storageId, 128, code),
    sessionId: canonicalSessionId(mutation.sessionId, code),
    leaseId: canonicalOpaqueId(mutation.leaseId, 128, code),
    holderId: canonicalOpaqueId(mutation.holderId, 128, code),
    fencingEpoch: canonicalWriterEpoch(
      mutation.fencingEpoch,
      code,
      true,
    ),
    operation: "attach",
    operationId: canonicalOpaqueId(mutation.operationId, 128, code),
    target: deepFreeze({
      attachmentId: canonicalOpaqueId(
        mutation.target.attachmentId,
        128,
        code,
      ),
      kind: "attachment",
    }),
  });
}

function attachMutationRequestFor(input, lease, code) {
  return canonicalAttachMutationRequest(
    {
      contractVersion: STORAGE_CONTRACT_VERSION,
      backendId: input.expectedSession.document.storageRef.backendId,
      storageId: input.expectedSession.document.storageRef.storageId,
      sessionId: input.expectedSession.sessionId,
      leaseId: lease.leaseId,
      holderId: lease.holderId,
      fencingEpoch: lease.fencingEpoch,
      operation: "attach",
      operationId: input.operationId,
      target: {
        attachmentId: attachmentIdForOperation(input.operationId),
        kind: "attachment",
      },
    },
    code,
  );
}

function restoreAttachmentActivationLeaseFor(
  input,
  authorityNow,
  fencingEpoch,
  code,
) {
  const request = restoreAttachmentActivationOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  return canonicalLeaseGrant(
    {
      contractVersion: STORAGE_CONTRACT_VERSION,
      sessionId: input.expectedSession.sessionId,
      leaseId: leaseIdForOperation(input.operationId),
      holderId: request.holderId,
      fencingEpoch,
      expiresAt: timestampAfter(
        authorityNow,
        request.leaseDurationMilliseconds,
        code,
      ),
    },
    code,
  );
}

function restoreAttachmentPublicationFor(input, generation, code) {
  const request = restoreAttachmentActivationOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  ensure(
    generation.state === "committed" &&
      generation.document !== null &&
      generation.document.contractVersion ===
        RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
    code,
  );
  const materialization = generation.document.materialization;
  return canonicalJsonObject(
    {
      artifactManifestDigest: materialization.artifactManifestDigest,
      coordinatorBindingSha256: materialization.coordinatorBindingSha256,
      modeledDigest: materialization.modeledDigest,
      publicationId: materialization.publicationId,
      publicationKind: materialization.publicationKind,
      root: {
        filesystemId: materialization.stagedRoot.filesystemId,
        objectIdentityScheme:
          materialization.stagedRoot.objectIdentityScheme,
        objectId: materialization.stagedRoot.objectId,
        rootPath: request.destinationRootPath,
      },
      treeIdentityDigest: materialization.treeIdentityDigest,
    },
    code,
  );
}

function structurallyCanonicalRestoreAttachmentActivationRequest(
  value,
  input,
  code,
) {
  let activationRequest;
  try {
    activationRequest = assertRestoreAttachmentActivationRequest(value);
  } catch {
    fail(code);
  }
  const request = restoreAttachmentActivationOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  const expectedEpoch = nextWriterEpochForCode(
    input.expectedSession.document.writerEpoch,
    code,
  );
  ensure(
    canonicalSerialize(
      canonicalJsonObject(activationRequest.manifest, code),
    ) ===
        canonicalSerialize(
          canonicalJsonObject(
            input.expectedSession.document.manifest,
            code,
          ),
        ) &&
      canonicalSerialize(
        canonicalJsonObject(activationRequest.storageRef, code),
      ) ===
        canonicalSerialize(
          canonicalJsonObject(
            input.expectedSession.document.storageRef,
            code,
          ),
        ) &&
      activationRequest.lease.sessionId === input.expectedSession.sessionId &&
      activationRequest.lease.leaseId === leaseIdForOperation(input.operationId) &&
      activationRequest.lease.holderId === request.holderId &&
      activationRequest.lease.fencingEpoch === expectedEpoch &&
      timestampMilliseconds(activationRequest.lease.expiresAt) -
          request.leaseDurationMilliseconds >=
        timestampMilliseconds(input.expectedSession.updatedAt) &&
      activationRequest.mutationRequest.operationId === input.operationId &&
      activationRequest.mutationRequest.target.attachmentId ===
        attachmentIdForOperation(input.operationId) &&
      activationRequest.publication.root.rootPath ===
        request.destinationRootPath,
    code,
  );
  return canonicalJsonObject(activationRequest, code);
}

function restoreAttachmentActivationRequestFor(
  input,
  generation,
  lease,
  code,
) {
  let activationRequest;
  try {
    activationRequest = assertRestoreAttachmentActivationRequest({
      contractVersion:
        RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION,
      lease,
      manifest: input.expectedSession.document.manifest,
      mutationRequest: attachMutationRequestFor(input, lease, code),
      publication: restoreAttachmentPublicationFor(input, generation, code),
      storageRef: input.expectedSession.document.storageRef,
    });
  } catch {
    fail(code);
  }
  return structurallyCanonicalRestoreAttachmentActivationRequest(
    activationRequest,
    input,
    code,
  );
}

function canonicalRestoreAttachmentActivationProviderResult(
  value,
  activationRequest,
  code,
) {
  let result;
  try {
    result = assertRestoreAttachmentActivationResult(value, {
      request: activationRequest,
    });
  } catch {
    fail(code);
  }
  return canonicalJsonObject(result, code);
}

function canonicalAttachMutationResult(value, request, code) {
  const rootPath = attachmentRootPathFromPlainRecord(value, code);
  const normalized = exactPlainObject(
    value,
    WRITER_ATTACH_MUTATION_RESULT_KEYS,
    code,
  );
  exactPlainObject(
    normalized.target,
    ATTACH_MUTATION_TARGET_KEYS,
    code,
  );
  let result;
  try {
    result = assertStorageMutationResult(
      {
        backendId: normalized.backendId,
        contractVersion: normalized.contractVersion,
        fencingEpoch: normalized.fencingEpoch,
        holderId: normalized.holderId,
        leaseId: normalized.leaseId,
        operation: normalized.operation,
        operationId: normalized.operationId,
        proofId: normalized.proofId,
        sessionId: normalized.sessionId,
        status: normalized.status,
        storageId: normalized.storageId,
        target: normalized.target,
      },
      { request },
    );
  } catch {
    fail(code);
  }
  const actualRequest = canonicalAttachMutationRequest(
    {
      backendId: result.backendId,
      contractVersion: result.contractVersion,
      fencingEpoch: result.fencingEpoch,
      holderId: result.holderId,
      leaseId: result.leaseId,
      operation: result.operation,
      operationId: result.operationId,
      sessionId: result.sessionId,
      storageId: result.storageId,
      target: result.target,
    },
    code,
  );
  ensure(
    result.operation === "attach" &&
      result.status === "attached" &&
      canonicalSerialize(actualRequest) === canonicalSerialize(request),
    code,
  );
  return deepFreeze({
    contractVersion: STORAGE_CONTRACT_VERSION,
    backendId: canonicalOpaqueId(result.backendId, 128, code),
    storageId: canonicalOpaqueId(result.storageId, 128, code),
    sessionId: canonicalSessionId(result.sessionId, code),
    leaseId: canonicalOpaqueId(result.leaseId, 128, code),
    holderId: canonicalOpaqueId(result.holderId, 128, code),
    fencingEpoch: canonicalWriterEpoch(
      result.fencingEpoch,
      code,
      true,
    ),
    operation: "attach",
    operationId: canonicalOpaqueId(result.operationId, 128, code),
    target: deepFreeze({
      attachmentId: canonicalOpaqueId(
        result.target.attachmentId,
        128,
        code,
      ),
      kind: "attachment",
    }),
    status: "attached",
    proofId: canonicalOpaqueId(result.proofId, 128, code),
    rootPath,
  });
}

function structurallyCanonicalAttachMutationResult(value, code) {
  attachmentRootPathFromPlainRecord(value, code);
  const normalized = exactPlainObject(
    value,
    WRITER_ATTACH_MUTATION_RESULT_KEYS,
    code,
  );
  const request = canonicalAttachMutationRequest(
    {
      backendId: normalized.backendId,
      contractVersion: normalized.contractVersion,
      fencingEpoch: normalized.fencingEpoch,
      holderId: normalized.holderId,
      leaseId: normalized.leaseId,
      operation: normalized.operation,
      operationId: normalized.operationId,
      sessionId: normalized.sessionId,
      storageId: normalized.storageId,
      target: normalized.target,
    },
    code,
  );
  return canonicalAttachMutationResult(value, request, code);
}

function canonicalDetachMutationRequest(value, code) {
  const normalized = exactPlainObject(
    value,
    ATTACH_MUTATION_REQUEST_KEYS,
    code,
  );
  const target = canonicalWriterAttachmentTarget(normalized.target, code);
  let mutation;
  try {
    mutation = assertStorageMutationRequest(value);
  } catch {
    fail(code);
  }
  ensure(mutation.operation === "detach", code);
  return deepFreeze({
    contractVersion: STORAGE_CONTRACT_VERSION,
    backendId: canonicalOpaqueId(mutation.backendId, 128, code),
    storageId: canonicalOpaqueId(mutation.storageId, 128, code),
    sessionId: canonicalSessionId(mutation.sessionId, code),
    leaseId: canonicalOpaqueId(mutation.leaseId, 128, code),
    holderId: canonicalOpaqueId(mutation.holderId, 128, code),
    fencingEpoch: canonicalWriterEpoch(
      mutation.fencingEpoch,
      code,
      true,
    ),
    operation: "detach",
    operationId: canonicalOpaqueId(mutation.operationId, 128, code),
    target,
  });
}

function detachMutationRequestFor(input, lease, code) {
  const request = writerReleaseRequest(input, code);
  return canonicalDetachMutationRequest(
    {
      contractVersion: STORAGE_CONTRACT_VERSION,
      backendId: input.expectedSession.document.storageRef.backendId,
      storageId: input.expectedSession.document.storageRef.storageId,
      sessionId: input.expectedSession.sessionId,
      leaseId: lease.leaseId,
      holderId: lease.holderId,
      fencingEpoch: lease.fencingEpoch,
      operation: "detach",
      operationId: input.operationId,
      target: request.target,
    },
    code,
  );
}

function canonicalDetachMutationResult(value, request, code) {
  const normalized = exactPlainObject(
    value,
    DETACH_MUTATION_RESULT_KEYS,
    code,
  );
  canonicalWriterAttachmentTarget(normalized.target, code);
  let result;
  try {
    result = assertStorageMutationResult(value, { request });
  } catch {
    fail(code);
  }
  const actualRequest = canonicalDetachMutationRequest(
    {
      backendId: result.backendId,
      contractVersion: result.contractVersion,
      fencingEpoch: result.fencingEpoch,
      holderId: result.holderId,
      leaseId: result.leaseId,
      operation: result.operation,
      operationId: result.operationId,
      sessionId: result.sessionId,
      storageId: result.storageId,
      target: result.target,
    },
    code,
  );
  ensure(
    result.operation === "detach" &&
      result.status === "detached" &&
      canonicalSerialize(actualRequest) === canonicalSerialize(request),
    code,
  );
  return deepFreeze({
    ...actualRequest,
    proofId: canonicalOpaqueId(result.proofId, 128, code),
    status: "detached",
  });
}

function structurallyCanonicalDetachMutationResult(value, code) {
  const normalized = exactPlainObject(
    value,
    DETACH_MUTATION_RESULT_KEYS,
    code,
  );
  const request = canonicalDetachMutationRequest({
    backendId: normalized.backendId,
    contractVersion: normalized.contractVersion,
    fencingEpoch: normalized.fencingEpoch,
    holderId: normalized.holderId,
    leaseId: normalized.leaseId,
    operation: normalized.operation,
    operationId: normalized.operationId,
    sessionId: normalized.sessionId,
    storageId: normalized.storageId,
    target: normalized.target,
  }, code);
  return canonicalDetachMutationResult(value, request, code);
}

function canonicalForceFenceRequest(value, code) {
  let request;
  try {
    request = assertStorageForceFenceRequest(value);
  } catch {
    fail(code);
  }
  return deepFreeze({
    backendId: canonicalOpaqueId(request.backendId, 128, code),
    contractVersion: STORAGE_CONTRACT_VERSION,
    fencingEpoch: canonicalWriterEpoch(
      request.fencingEpoch,
      code,
      true,
    ),
    operationId: canonicalOpaqueId(request.operationId, 128, code),
    revokedFence: deepFreeze({
      fencingEpoch: canonicalWriterEpoch(
        request.revokedFence.fencingEpoch,
        code,
        true,
      ),
      holderId: canonicalOpaqueId(
        request.revokedFence.holderId,
        128,
        code,
      ),
      leaseId: canonicalOpaqueId(
        request.revokedFence.leaseId,
        128,
        code,
      ),
    }),
    sessionId: canonicalSessionId(request.sessionId, code),
    storageId: canonicalOpaqueId(request.storageId, 128, code),
    target: canonicalWriterAttachmentTarget(request.target, code),
  });
}

function forceFenceRequestFor(input, fencingEpoch, code) {
  const request = writerForceFenceOperationRequest(input, code);
  const document = input.expectedSession.document;
  const lease = document.lease;
  ensure(lease !== null, code);
  return canonicalForceFenceRequest(
    {
      backendId: document.storageRef.backendId,
      contractVersion: STORAGE_CONTRACT_VERSION,
      fencingEpoch,
      operationId: input.operationId,
      revokedFence: {
        fencingEpoch: lease.fencingEpoch,
        holderId: lease.holderId,
        leaseId: lease.leaseId,
      },
      sessionId: input.expectedSession.sessionId,
      storageId: document.storageRef.storageId,
      target: request.target,
    },
    code,
  );
}

function validateForceFenceTargetSource(input, terminal, code) {
  const document = input.expectedSession.document;
  const request = writerForceFenceOperationRequest(input, code);
  if (document.attachment !== null) {
    ensure(
      request.target.attachmentId ===
        document.attachment.attachmentId,
      code,
    );
    return;
  }
  ensure(
    document.lifecycle === "BLOCKED" &&
      terminal !== null &&
      terminal.operation.result?.outcome === "writer-blocked" &&
      canonicalSerialize(terminal.operation.result.fenceTarget) ===
        canonicalSerialize(request.target),
    code,
  );
}

function canonicalForceFenceResult(value, request, code) {
  let result;
  try {
    result = assertStorageForceFenceResult(value, { request });
  } catch {
    fail(code);
  }
  const actualRequest = canonicalForceFenceRequest({
    backendId: result.backendId,
    contractVersion: result.contractVersion,
    fencingEpoch: result.fencingEpoch,
    operationId: result.operationId,
    revokedFence: result.revokedFence,
    sessionId: result.sessionId,
    storageId: result.storageId,
    target: result.target,
  }, code);
  ensure(
    result.status === "fenced" &&
      canonicalSerialize(actualRequest) === canonicalSerialize(request),
    code,
  );
  return deepFreeze({
    ...actualRequest,
    proofId: canonicalOpaqueId(result.proofId, 128, code),
    status: "fenced",
  });
}

function structurallyCanonicalForceFenceResult(value, code) {
  const result = canonicalForceFenceResult(
    value,
    canonicalForceFenceRequest({
      backendId: ownDataValue(value, "backendId", code),
      contractVersion: ownDataValue(value, "contractVersion", code),
      fencingEpoch: ownDataValue(value, "fencingEpoch", code),
      operationId: ownDataValue(value, "operationId", code),
      revokedFence: ownDataValue(value, "revokedFence", code),
      sessionId: ownDataValue(value, "sessionId", code),
      storageId: ownDataValue(value, "storageId", code),
      target: ownDataValue(value, "target", code),
    }, code),
    code,
  );
  return result;
}

function operationInputWithExpectedRevision(options, expectedRevision) {
  const input = canonicalOperationInput(
    options,
    OPERATION_TRANSITION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    OPERATION_TRANSITION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === expectedRevision,
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    expectedOperationRevision,
  });
}

function checkpointCaptureInput(
  options,
  keys = OPERATION_INPUT_KEYS,
  code = "invalid_operation_request",
) {
  const input = canonicalOperationInput(options, keys);
  ensure(input.kind === CHECKPOINT_CAPTURE_OPERATION_KIND, code);
  const request = checkpointCaptureOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  ensure(
    input.operationId === request.admission.request.operationId,
    code,
  );
  return deepFreeze({ ...input, request });
}

function checkpointCaptureTransitionInput(options) {
  const input = checkpointCaptureInput(
    options,
    OPERATION_TRANSITION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    OPERATION_TRANSITION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "0",
    "invalid_operation_request",
  );
  return deepFreeze({ ...input, expectedOperationRevision });
}

function restoreGenerationInput(
  options,
  keys = OPERATION_INPUT_KEYS,
  code = "invalid_operation_request",
) {
  const input = canonicalOperationInput(options, keys);
  ensure(input.kind === RESTORE_DESTINATION_GENERATION_OPERATION_KIND, code);
  const request = restoreGenerationOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  ensure(input.operationId === request.admission.request.operationId, code);
  return deepFreeze({ ...input, request });
}

function restoreGenerationTransitionInput(options) {
  const input = restoreGenerationInput(
    options,
    RESTORE_GENERATION_CLAIM_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    RESTORE_GENERATION_CLAIM_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(expectedOperationRevision === "0", "invalid_operation_request");
  const destinationIsolationProofId = canonicalOpaqueId(
    normalized.destinationIsolationProofId,
    128,
    "invalid_operation_request",
  );
  const generationId = canonicalOpaqueId(
    normalized.generationId,
    128,
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    destinationIsolationProofId,
    expectedOperationRevision,
    generationId,
  });
}

function restoreAttachmentActivationInput(
  options,
  keys = OPERATION_INPUT_KEYS,
  code = "invalid_operation_request",
) {
  const input = canonicalOperationInput(options, keys);
  ensure(input.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND, code);
  const request = restoreAttachmentActivationOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  ensure(input.operationId !== request.launchIntent.launchAttemptId, code);
  return deepFreeze({ ...input, request });
}

function restoreAttachmentActivationTransitionInput(options) {
  const input = restoreAttachmentActivationInput(
    options,
    RESTORE_ATTACHMENT_ACTIVATION_CLAIM_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    RESTORE_ATTACHMENT_ACTIVATION_CLAIM_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(expectedOperationRevision === "0", "invalid_operation_request");
  return deepFreeze({ ...input, expectedOperationRevision });
}

function writerLaunchAttemptInput(
  options,
  keys = OPERATION_INPUT_KEYS,
  code = "invalid_operation_request",
) {
  const input = canonicalOperationInput(options, keys);
  ensure(input.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND, code);
  const request = writerLaunchAttemptOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  return deepFreeze({ ...input, request });
}

function writerLaunchAttemptTransitionInput(options) {
  const input = writerLaunchAttemptInput(
    options,
    OPERATION_TRANSITION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    OPERATION_TRANSITION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(expectedOperationRevision === "0", "invalid_operation_request");
  return deepFreeze({ ...input, expectedOperationRevision });
}

function writerLaunchStopInput(
  options,
  keys = OPERATION_INPUT_KEYS,
  code = "invalid_operation_request",
) {
  const input = canonicalOperationInput(options, keys);
  ensure(input.kind === WRITER_LAUNCH_STOP_OPERATION_KIND, code);
  const request = writerLaunchStopOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  return deepFreeze({ ...input, request });
}

function canonicalWriterLaunchStopClaimToken(value, code) {
  ensure(
    typeof value === "string" && regexpTest(UUID_PATTERN, value),
    code,
  );
  return value;
}

function hasWriterLaunchStopClaimToken(options) {
  return (
    options !== null &&
    typeof options === "object" &&
    !arrayIsArray(options) &&
    !isProxyValue(options) &&
    objectHasOwn(options, "claimToken")
  );
}

function writerLaunchStopClaimSha256(value, code) {
  const claimToken = canonicalWriterLaunchStopClaimToken(value, code);
  return sha256(`${WRITER_LAUNCH_STOP_CLAIM_DOMAIN}\0${claimToken}`);
}

function writerLaunchStopReconcileInput(options) {
  const hasClaimToken = hasWriterLaunchStopClaimToken(options);
  const input = writerLaunchStopInput(
    options,
    hasClaimToken
      ? WRITER_LAUNCH_STOP_RECONCILE_INPUT_KEYS
      : OPERATION_INPUT_KEYS,
  );
  ensure(
    (input.request.contractVersion ===
      WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2) === hasClaimToken,
    "invalid_operation_request",
  );
  if (!hasClaimToken) return input;
  const normalized = exactPlainObject(
    options,
    WRITER_LAUNCH_STOP_RECONCILE_INPUT_KEYS,
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    claimToken: canonicalWriterLaunchStopClaimToken(
      normalized.claimToken,
      "invalid_operation_request",
    ),
  });
}

function writerLaunchStopTransitionInput(options) {
  const hasClaimToken = hasWriterLaunchStopClaimToken(options);
  const input = writerLaunchStopInput(
    options,
    hasClaimToken
      ? WRITER_LAUNCH_STOP_TRANSITION_INPUT_KEYS
      : OPERATION_TRANSITION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    hasClaimToken
      ? WRITER_LAUNCH_STOP_TRANSITION_INPUT_KEYS
      : OPERATION_TRANSITION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(expectedOperationRevision === "0", "invalid_operation_request");
  ensure(
    (input.request.contractVersion ===
      WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2) === hasClaimToken,
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    ...(hasClaimToken
      ? {
          claimToken: canonicalWriterLaunchStopClaimToken(
            normalized.claimToken,
            "invalid_operation_request",
          ),
        }
      : {}),
    expectedOperationRevision,
  });
}

function canonicalWriterLaunchEvidence(value, input, statuses, code) {
  const evidence = exactPlainObject(
    value,
    WRITER_LAUNCH_EVIDENCE_KEYS,
    code,
  );
  const status = canonicalOpaqueId(evidence.status, 32, code);
  ensure(
    evidence.contractVersion ===
        WRITER_LAUNCH_SUPERVISOR_CONTRACT_VERSION &&
      reflectApply(arrayIncludesIntrinsic, statuses, [status]) &&
      evidence.launchAttemptId === input.operationId &&
      evidence.supervisorId === input.request.supervisor.supervisorId,
    code,
  );
  const processIncarnationId =
    evidence.processIncarnationId === null
      ? null
      : canonicalOpaqueId(evidence.processIncarnationId, 128, code);
  const writerIncarnationId =
    evidence.writerIncarnationId === null
      ? null
      : canonicalOpaqueId(evidence.writerIncarnationId, 128, code);
  ensure(
    (status === "not-started" &&
      processIncarnationId === null &&
      writerIncarnationId === null) ||
      ((status === "started" || status === "complete-stopped") &&
        processIncarnationId !== null &&
        writerIncarnationId !== null),
    code,
  );
  return deepFreeze({
    contractVersion: WRITER_LAUNCH_SUPERVISOR_CONTRACT_VERSION,
    launchAttemptId: input.operationId,
    processIncarnationId,
    proofId: canonicalOpaqueId(evidence.proofId, 128, code),
    status,
    supervisorId: input.request.supervisor.supervisorId,
    writerIncarnationId,
  });
}

function writerLaunchAttemptFinalizationInput(options, statuses) {
  const input = writerLaunchAttemptInput(
    options,
    WRITER_LAUNCH_FINALIZATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    WRITER_LAUNCH_FINALIZATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "1" ||
      expectedOperationRevision === "2",
    "invalid_operation_request",
  );
  const evidence = canonicalWriterLaunchEvidence(
    normalized.evidence,
    input,
    statuses,
    "invalid_operation_request",
  );
  return deepFreeze({ ...input, evidence, expectedOperationRevision });
}

function canonicalWriterLaunchStopEvidence(value, input, code) {
  const evidence = exactPlainObject(
    value,
    WRITER_LAUNCH_EVIDENCE_KEYS,
    code,
  );
  const launch = input.request.launch;
  ensure(
    evidence.contractVersion ===
        WRITER_LAUNCH_SUPERVISOR_CONTRACT_VERSION &&
      evidence.status === "complete-stopped" &&
      evidence.launchAttemptId === launch.launchAttemptId &&
      evidence.supervisorId === launch.supervisorId &&
      evidence.processIncarnationId === launch.processIncarnationId &&
      evidence.writerIncarnationId === launch.writerIncarnationId,
    code,
  );
  return deepFreeze({
    contractVersion: WRITER_LAUNCH_SUPERVISOR_CONTRACT_VERSION,
    launchAttemptId: launch.launchAttemptId,
    processIncarnationId: launch.processIncarnationId,
    proofId: canonicalOpaqueId(evidence.proofId, 128, code),
    status: "complete-stopped",
    supervisorId: launch.supervisorId,
    writerIncarnationId: launch.writerIncarnationId,
  });
}

function writerLaunchStopFinalizationInput(options) {
  const input = writerLaunchStopInput(
    options,
    WRITER_LAUNCH_STOP_FINALIZATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    WRITER_LAUNCH_STOP_FINALIZATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "1" ||
      expectedOperationRevision === "2",
    "invalid_operation_request",
  );
  const evidence = canonicalWriterLaunchStopEvidence(
    normalized.evidence,
    input,
    "invalid_operation_request",
  );
  return deepFreeze({ ...input, evidence, expectedOperationRevision });
}

function writerLaunchAttemptReadInput(options) {
  const normalized = exactPlainObject(
    options,
    WRITER_LAUNCH_READ_KEYS,
    "invalid_operation_request",
  );
  return deepFreeze({
    operationId: canonicalOpaqueId(
      normalized.operationId,
      128,
      "invalid_operation_request",
    ),
  });
}

function writerLaunchAttemptRecoveryListInput(options) {
  const normalized = exactPlainObject(
    options,
    WRITER_LAUNCH_RECOVERY_LIST_KEYS,
    "invalid_operation_request",
  );
  const afterSessionId =
    normalized.afterSessionId === null
      ? null
      : canonicalSessionId(
          normalized.afterSessionId,
          "invalid_operation_request",
        );
  ensure(
    numberIsSafeInteger(normalized.limit) &&
      normalized.limit >= 1 &&
      normalized.limit <= 100,
    "invalid_operation_request",
  );
  return deepFreeze({ afterSessionId, limit: normalized.limit });
}

function currentWriterLaunchRecoveryListInput(options) {
  const normalized = exactPlainObject(
    options,
    CURRENT_WRITER_LAUNCH_RECOVERY_LIST_KEYS,
    "invalid_operation_request",
  );
  const afterSessionId =
    normalized.afterSessionId === null
      ? null
      : canonicalSessionId(
          normalized.afterSessionId,
          "invalid_operation_request",
        );
  ensure(
    numberIsSafeInteger(normalized.limit) &&
      normalized.limit >= 1 &&
      normalized.limit <= 100,
    "invalid_operation_request",
  );
  return deepFreeze({ afterSessionId, limit: normalized.limit });
}

function canonicalCheckpointArtifactProof(value, operationId, code) {
  const proof = exactPlainObject(
    value,
    CHECKPOINT_ARTIFACT_PROOF_KEYS,
    code,
  );
  ensure(
    typeof proof.artifactManifestDigest === "string" &&
      regexpTest(SHA256_PATTERN, proof.artifactManifestDigest) &&
      canonicalOpaqueId(proof.captureOperationId, 128, code) ===
        operationId &&
      typeof proof.modeledDigest === "string" &&
      regexpTest(SHA256_PATTERN, proof.modeledDigest),
    code,
  );
  return deepFreeze({
    artifactManifestDigest: proof.artifactManifestDigest,
    captureOperationId: proof.captureOperationId,
    modeledDigest: proof.modeledDigest,
  });
}

function canonicalPublicationMaterialization(
  value,
  artifactProof,
  publicationKind,
  code,
  coordinatorBinding = undefined,
) {
  const restoreDestination = publicationKind === "restore-destination";
  const materialization = exactPlainObject(
    value,
    restoreDestination
      ? RESTORE_DESTINATION_MATERIALIZATION_KEYS
      : CHECKPOINT_MATERIALIZATION_KEYS,
    code,
  );
  const stagedRoot = exactPlainObject(
    materialization.stagedRoot,
    CHECKPOINT_STAGED_ROOT_KEYS,
    code,
  );
  assertLosslessString(stagedRoot.objectId, code);
  let expectedCoordinatorBindingSha256;
  if (restoreDestination) {
    try {
      expectedCoordinatorBindingSha256 =
        operationJournalBindingSha256(coordinatorBinding);
    } catch {
      fail(code);
    }
  }
  ensure(
    materialization.contractVersion ===
      (restoreDestination
        ? RESTORE_DESTINATION_MATERIALIZATION_CONTRACT_VERSION
        : CHECKPOINT_MATERIALIZATION_CONTRACT_VERSION) &&
      typeof materialization.artifactManifestDigest === "string" &&
      regexpTest(SHA256_PATTERN, materialization.artifactManifestDigest) &&
      materialization.artifactManifestDigest ===
        artifactProof.artifactManifestDigest &&
      typeof materialization.modeledDigest === "string" &&
      regexpTest(SHA256_PATTERN, materialization.modeledDigest) &&
      materialization.modeledDigest === artifactProof.modeledDigest &&
      canonicalOpaqueId(materialization.publicationId, 128, code) ===
        materialization.publicationId &&
      materialization.publicationKind === publicationKind &&
      (!restoreDestination ||
        (typeof materialization.coordinatorBindingSha256 === "string" &&
          regexpTest(
            SHA256_PATTERN,
            materialization.coordinatorBindingSha256,
          ) &&
          materialization.coordinatorBindingSha256 ===
            expectedCoordinatorBindingSha256)) &&
      canonicalOpaqueId(stagedRoot.filesystemId, 128, code) ===
        stagedRoot.filesystemId &&
      canonicalOpaqueId(stagedRoot.objectIdentityScheme, 128, code) ===
        stagedRoot.objectIdentityScheme &&
      regexpTest(PERSISTENT_OBJECT_ID_PATTERN, stagedRoot.objectId) &&
      typeof materialization.treeIdentityDigest === "string" &&
      regexpTest(SHA256_PATTERN, materialization.treeIdentityDigest),
    code,
  );
  return deepFreeze({
    artifactManifestDigest: materialization.artifactManifestDigest,
    ...(restoreDestination
      ? {
          coordinatorBindingSha256:
            materialization.coordinatorBindingSha256,
        }
      : {}),
    contractVersion: materialization.contractVersion,
    modeledDigest: materialization.modeledDigest,
    publicationId: materialization.publicationId,
    publicationKind,
    stagedRoot: deepFreeze({
      filesystemId: stagedRoot.filesystemId,
      objectIdentityScheme: stagedRoot.objectIdentityScheme,
      objectId: stagedRoot.objectId,
    }),
    treeIdentityDigest: materialization.treeIdentityDigest,
  });
}

function canonicalCheckpointMaterialization(value, artifactProof, code) {
  return canonicalPublicationMaterialization(
    value,
    artifactProof,
    "checkpoint-artifact",
    code,
  );
}

function canonicalCheckpointCatalogueDocument(value, input, code) {
  const document = exactPlainObject(
    value,
    CHECKPOINT_CATALOGUE_DOCUMENT_KEYS,
    code,
  );
  ensure(
    document.contractVersion === CHECKPOINT_CATALOGUE_CONTRACT_VERSION,
    code,
  );
  const artifactProof = canonicalCheckpointArtifactProof(
    document.artifactProof,
    input.operationId,
    code,
  );
  const materialization = canonicalCheckpointMaterialization(
    document.materialization,
    artifactProof,
    code,
  );
  const result = canonicalCheckpointCaptureResult(
    document.result,
    input.request.admission,
    code,
  );
  return deepFreeze({
    artifactProof,
    contractVersion: CHECKPOINT_CATALOGUE_CONTRACT_VERSION,
    materialization,
    result,
  });
}

function canonicalCheckpointCompletion(value, input, code) {
  const completion = exactPlainObject(
    value,
    CHECKPOINT_CAPTURE_COMPLETION_KEYS,
    code,
  );
  ensure(typeof completion.replayed === "boolean", code);
  const document = canonicalCheckpointCatalogueDocument(
    {
      artifactProof: completion.artifactProof,
      contractVersion: CHECKPOINT_CATALOGUE_CONTRACT_VERSION,
      materialization: completion.materialization,
      result: completion.result,
    },
    input,
    code,
  );
  return deepFreeze({
    document,
    replayed: completion.replayed,
  });
}

function checkpointCaptureFinalizationInput(options) {
  const input = checkpointCaptureInput(
    options,
    CHECKPOINT_CAPTURE_FINALIZATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    CHECKPOINT_CAPTURE_FINALIZATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "1" ||
      expectedOperationRevision === "2",
    "invalid_operation_request",
  );
  const completion = canonicalCheckpointCompletion(
    normalized.completion,
    input,
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    completion,
    expectedOperationRevision,
  });
}

function checkpointCaptureReadInput(options, keys = CHECKPOINT_CAPTURE_READ_KEYS) {
  const normalized = exactPlainObject(
    options,
    keys,
    "invalid_operation_request",
  );
  let checkpoint;
  try {
    checkpoint = assertCheckpointDescriptor(normalized.checkpoint);
  } catch {
    fail("invalid_operation_request");
  }
  checkpoint = canonicalJsonObject(
    checkpoint,
    "invalid_operation_request",
  );
  const request = canonicalCheckpointMutationRequest(
    normalized.request,
    "invalid_operation_request",
  );
  ensure(
    request.sessionId === checkpoint.sessionId &&
      request.backendId === checkpoint.backendId &&
      request.storageId === checkpoint.storageId &&
      request.operation === "checkpoint" &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId &&
      request.fencingEpoch === checkpoint.sourceFencingEpoch,
    "invalid_operation_request",
  );
  return deepFreeze({ checkpoint, request });
}

function checkpointCaptureRecoveryListInput(options) {
  const normalized = exactPlainObject(
    options,
    CHECKPOINT_CAPTURE_RECOVERY_LIST_KEYS,
    "invalid_operation_request",
  );
  const afterSessionId =
    normalized.afterSessionId === null
      ? null
      : canonicalSessionId(
          normalized.afterSessionId,
          "invalid_operation_request",
        );
  ensure(
    numberIsSafeInteger(normalized.limit) &&
      normalized.limit >= 1 &&
      normalized.limit <= 100,
    "invalid_operation_request",
  );
  return deepFreeze({ afterSessionId, limit: normalized.limit });
}

function restoreGenerationFinalizationInput(options) {
  const input = restoreGenerationInput(
    options,
    RESTORE_GENERATION_FINALIZATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    RESTORE_GENERATION_FINALIZATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "1" ||
      expectedOperationRevision === "2",
    "invalid_operation_request",
  );
  const completion = canonicalJsonObject(
    normalized.completion,
    "invalid_operation_request",
  );
  exactPlainObject(
    completion,
    RESTORE_GENERATION_COMPLETION_KEYS,
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    completion,
    expectedOperationRevision,
  });
}

function restoreGenerationLaunchHandoffInput(options) {
  const normalized = exactPlainObject(
    options,
    RESTORE_GENERATION_LAUNCH_HANDOFF_INPUT_KEYS,
    "invalid_operation_request",
  );
  const restore = restoreGenerationFinalizationInput(normalized.restore);
  ensure(
    restore.request.contractVersion ===
      RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2,
    "invalid_operation_request",
  );
  const launch = canonicalRestoreGenerationLaunchIntent(
    normalized.launch,
    restore.expectedSession,
    "invalid_operation_request",
  );
  ensure(
    canonicalSerialize(launch) ===
      canonicalSerialize(restore.request.launchIntent),
    "invalid_operation_request",
  );
  return deepFreeze({ launch, restore });
}

function restoreAttachmentActivationFinalizationInput(options) {
  const input = restoreAttachmentActivationInput(
    options,
    RESTORE_ATTACHMENT_ACTIVATION_FINALIZATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    RESTORE_ATTACHMENT_ACTIVATION_FINALIZATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "1" ||
      expectedOperationRevision === "2",
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    activationResult: canonicalJsonObject(
      normalized.activationResult,
      "invalid_operation_request",
    ),
    expectedOperationRevision,
  });
}

function restoreGenerationReadInput(options) {
  const normalized = exactPlainObject(
    options,
    RESTORE_GENERATION_READ_KEYS,
    "invalid_operation_request",
  );
  let checkpoint;
  try {
    checkpoint = assertCheckpointDescriptor(normalized.checkpoint);
  } catch {
    fail("invalid_operation_request");
  }
  checkpoint = canonicalJsonObject(checkpoint, "invalid_operation_request");
  const generationId = canonicalOpaqueId(
    normalized.generationId,
    128,
    "invalid_operation_request",
  );
  const request = canonicalRestoreMutationRequest(
    normalized.request,
    "invalid_operation_request",
  );
  ensure(
    request.sessionId === checkpoint.sessionId &&
      request.backendId === checkpoint.backendId &&
      request.operation === "restore" &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId &&
      BigIntConstructor(request.fencingEpoch) >
        BigIntConstructor(checkpoint.sourceFencingEpoch),
    "invalid_operation_request",
  );
  return deepFreeze({ checkpoint, generationId, request });
}

function restoreGenerationRecoveryListInput(options) {
  const normalized = exactPlainObject(
    options,
    RESTORE_GENERATION_RECOVERY_LIST_KEYS,
    "invalid_operation_request",
  );
  const afterSessionId =
    normalized.afterSessionId === null
      ? null
      : canonicalSessionId(
          normalized.afterSessionId,
          "invalid_operation_request",
        );
  ensure(
    numberIsSafeInteger(normalized.limit) &&
      normalized.limit >= 1 &&
      normalized.limit <= 100,
    "invalid_operation_request",
  );
  return deepFreeze({ afterSessionId, limit: normalized.limit });
}

function restoreAttachmentActivationRecoveryListInput(options) {
  const normalized = exactPlainObject(
    options,
    RESTORE_ATTACHMENT_ACTIVATION_RECOVERY_LIST_KEYS,
    "invalid_operation_request",
  );
  const afterSessionId =
    normalized.afterSessionId === null
      ? null
      : canonicalSessionId(
          normalized.afterSessionId,
          "invalid_operation_request",
        );
  ensure(
    numberIsSafeInteger(normalized.limit) &&
      normalized.limit >= 1 &&
      normalized.limit <= 100,
    "invalid_operation_request",
  );
  return deepFreeze({ afterSessionId, limit: normalized.limit });
}

function checkpointCatalogueReadInput(options) {
  const normalized = exactPlainObject(
    options,
    CHECKPOINT_CATALOGUE_READ_KEYS,
    "invalid_operation_request",
  );
  let checkpoint;
  try {
    checkpoint = assertCheckpointDescriptor(normalized.checkpoint);
  } catch {
    fail("invalid_operation_request");
  }
  return deepFreeze({
    checkpoint: canonicalJsonObject(
      checkpoint,
      "invalid_operation_request",
    ),
  });
}

function checkpointCaptureBinding(input) {
  const admission = input.request.admission;
  return deepFreeze({
    attachmentId: admission.attachment.attachmentId,
    attachmentOperationId: admission.attachment.operationId,
    attachmentProofId: admission.attachment.proofId,
    captureAttemptId: admission.captureAttemptId,
    checkpoint: admission.checkpoint,
    contractVersion: CHECKPOINT_CAPTURE_BINDING_CONTRACT_VERSION,
    processIncarnationId: admission.processIncarnationId,
    reservationId: input.reservationId,
    stopOperationId: admission.stopOperationId,
    writerIncarnationId: admission.writerIncarnationId,
  });
}

function canonicalCheckpointCaptureBinding(value, input, code) {
  const binding = exactPlainObject(
    value,
    CHECKPOINT_CAPTURE_BINDING_KEYS,
    code,
  );
  const expected = checkpointCaptureBinding(input);
  ensure(
    canonicalSerialize(canonicalJsonObject(binding, code)) ===
      canonicalSerialize(expected),
    code,
  );
  return expected;
}

function restoreGenerationBinding(
  input,
  source,
  { destinationIsolationProofId, generationId },
) {
  const admission = input.request.admission;
  const attachment = input.expectedSession.document.attachment;
  ensure(attachment !== null, "operation_state_invalid");
  return canonicalJsonObject(
    {
      attachment,
      captureAttemptId: source.attempt.captureAttemptId,
      captureOperationId: source.operation.operationId,
      catalogueSha256: sha256(
        canonicalSerialize(source.catalogue.document),
      ),
      checkpoint: admission.checkpoint,
      contractVersion:
        RESTORE_DESTINATION_GENERATION_BINDING_CONTRACT_VERSION,
      destinationIsolationProofId,
      destinationState: "detached",
      generationId,
      request: admission.request,
      reservationId: input.reservationId,
    },
    "operation_state_invalid",
  );
}

function canonicalRestoreGenerationBinding(
  value,
  input,
  source,
  generationId,
  code,
) {
  const binding = exactPlainObject(
    value,
    RESTORE_GENERATION_BINDING_KEYS,
    code,
  );
  const destinationIsolationProofId = canonicalOpaqueId(
    binding.destinationIsolationProofId,
    128,
    code,
  );
  const expected = restoreGenerationBinding(input, source, {
    destinationIsolationProofId,
    generationId,
  });
  ensure(
    canonicalSerialize(canonicalJsonObject(binding, code)) ===
      canonicalSerialize(expected),
    code,
  );
  return expected;
}

function canonicalRestoreGenerationDocument(
  value,
  input,
  source,
  generationBinding,
  code,
) {
  const document = exactPlainObject(
    value,
    RESTORE_GENERATION_DOCUMENT_KEYS,
    code,
  );
  ensure(
    document.contractVersion ===
      RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
    code,
  );
  const artifactProof = canonicalCheckpointArtifactProof(
    document.artifactProof,
    source.operation.operationId,
    code,
  );
  ensure(
    canonicalSerialize(artifactProof) ===
      canonicalSerialize(source.catalogue.document.artifactProof),
    code,
  );
  const materialization = canonicalPublicationMaterialization(
    document.materialization,
    artifactProof,
    "restore-destination",
    code,
    generationBinding,
  );
  const result = canonicalRestoreGenerationResult(
    document.result,
    input.request.admission,
    code,
  );
  return deepFreeze({
    artifactProof,
    contractVersion:
      RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
    materialization,
    result,
  });
}

function canonicalRestoreGenerationCompletion(
  value,
  input,
  source,
  generationBinding,
  code,
) {
  const completion = exactPlainObject(
    value,
    RESTORE_GENERATION_COMPLETION_KEYS,
    code,
  );
  ensure(typeof completion.replayed === "boolean", code);
  const document = canonicalRestoreGenerationDocument(
    {
      artifactProof: source.catalogue.document.artifactProof,
      contractVersion:
        RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
      materialization: completion.materialization,
      result: completion.result,
    },
    input,
    source,
    generationBinding,
    code,
  );
  return deepFreeze({
    document,
    replayed: completion.replayed,
  });
}

function writerAttachmentFinalizationInput(options) {
  const input = canonicalOperationInput(
    options,
    WRITER_ATTACHMENT_FINALIZATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    WRITER_ATTACHMENT_FINALIZATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "1" ||
      expectedOperationRevision === "2",
    "invalid_operation_request",
  );
  const attachment = canonicalSessionAttachment(
    normalized.attachment,
    "invalid_operation_request",
  );
  const mutationResult = structurallyCanonicalAttachMutationResult(
    normalized.mutationResult,
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    attachment,
    expectedOperationRevision,
    mutationResult,
  });
}

function writerReleaseFinalizationInput(options) {
  const input = canonicalOperationInput(
    options,
    WRITER_RELEASE_FINALIZATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    WRITER_RELEASE_FINALIZATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "1" ||
      expectedOperationRevision === "2",
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    expectedOperationRevision,
    mutationResult: structurallyCanonicalDetachMutationResult(
      normalized.mutationResult,
      "invalid_operation_request",
    ),
  });
}

function writerForceFenceFinalizationInput(options) {
  const input = canonicalOperationInput(
    options,
    WRITER_FORCE_FENCE_FINALIZATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    WRITER_FORCE_FENCE_FINALIZATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "1" ||
      expectedOperationRevision === "2",
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    expectedOperationRevision,
    fenceResult: structurallyCanonicalForceFenceResult(
      normalized.fenceResult,
      "invalid_operation_request",
    ),
  });
}

function writerBlockedFinalizationInput(options) {
  const input = canonicalOperationInput(
    options,
    WRITER_BLOCKED_FINALIZATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    WRITER_BLOCKED_FINALIZATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  const reason = canonicalOpaqueId(
    normalized.reason,
    64,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === "2" &&
      reflectApply(arrayIncludesIntrinsic, WRITER_BLOCKED_REASONS, [
        reason,
      ]) &&
      (input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND ||
        input.kind === WRITER_RELEASE_OPERATION_KIND ||
        input.kind === WRITER_FORCE_FENCE_OPERATION_KIND),
    "invalid_operation_request",
  );
  ensure(
    input.kind === WRITER_FORCE_FENCE_OPERATION_KIND ||
      reason === "provider-outcome-unresolved",
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    expectedOperationRevision,
    reason,
  });
}

function cancellationInput(options) {
  const input = canonicalOperationInput(
    options,
    OPERATION_CANCELLATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    OPERATION_CANCELLATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(expectedOperationRevision === "0", "invalid_operation_request");
  const reason = canonicalOpaqueId(
    normalized.reason,
    64,
    "invalid_operation_request",
  );
  const result = deepFreeze({
    resultVersion: OPERATION_RESULT_VERSION,
    outcome: "cancelled-before-dispatch",
    reason,
  });
  return deepFreeze({
    ...input,
    expectedOperationRevision,
    reason,
    result,
    serializedResult: canonicalSerialize(result),
  });
}

function canonicalNullableRowTimestamp(value, code) {
  return value === null ? null : canonicalTimestampForCode(value, code);
}

function canonicalTimestampForCode(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === datePrototype,
    code,
  );
  const milliseconds = reflectApply(dateGetTimeIntrinsic, value, []);
  ensure(numberIsFinite(milliseconds), code);
  return reflectApply(dateToISOStringIntrinsic, value, []);
}

function canonicalCancellationResult(value, code) {
  const result = exactPlainObject(value, CANCELLATION_RESULT_KEYS, code);
  ensure(
    result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "cancelled-before-dispatch",
    code,
  );
  return deepFreeze({
    resultVersion: OPERATION_RESULT_VERSION,
    outcome: "cancelled-before-dispatch",
    reason: canonicalOpaqueId(result.reason, 64, code),
  });
}

function operationInputFromEnvelope({
  envelope,
  kind,
  operationId,
  requestSha256,
}) {
  return deepFreeze({
    envelope,
    expectedSession: envelope.expectedSession,
    kind,
    operationId,
    request: envelope.payload,
    requestSha256,
    reservationId: `reservation-${sha256(operationId)}`,
    serializedEnvelope: canonicalSerialize(envelope),
  });
}

function canonicalWriterAttachmentResult(
  { attachment, input, lease, mutationResult },
  code,
) {
  const request = writerAttachmentRequest(input, code);
  const writerLease = canonicalLeaseGrant(lease, code);
  const mounted = canonicalSessionAttachment(attachment, code);
  const expectedEpoch = nextWriterEpochForCode(
    input.expectedSession.document.writerEpoch,
    code,
  );
  ensure(
    writerLease.sessionId === input.expectedSession.sessionId &&
      writerLease.leaseId === leaseIdForOperation(input.operationId) &&
      writerLease.holderId === request.holderId &&
      writerLease.fencingEpoch === expectedEpoch &&
      timestampMilliseconds(writerLease.expiresAt) -
        request.leaseDurationMilliseconds >=
        timestampMilliseconds(input.expectedSession.updatedAt),
    code,
  );
  const expectedMutation = attachMutationRequestFor(input, writerLease, code);
  const mutation = canonicalAttachMutationResult(
    mutationResult,
    expectedMutation,
    code,
  );
  canonicalLeaseAttachmentBinding({
    attachment: mounted,
    lease: writerLease,
    manifest: input.expectedSession.document.manifest,
    storageRef: input.expectedSession.document.storageRef,
    code,
  });
  ensure(
    mounted.operationId === input.operationId &&
      mounted.attachmentId === expectedMutation.target.attachmentId &&
      mounted.proofId === mutation.proofId &&
      mounted.rootPath === mutation.rootPath,
    code,
  );
  return deepFreeze({
    resultVersion: OPERATION_RESULT_VERSION,
    outcome: "writer-attached",
    lease: writerLease,
    attachment: mounted,
    mutationResult: mutation,
  });
}

function canonicalWriterAttachmentStoredResult(value, input, code) {
  const result = exactPlainObject(
    value,
    WRITER_ATTACHMENT_RESULT_KEYS,
    code,
  );
  ensure(
    result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "writer-attached",
    code,
  );
  return canonicalWriterAttachmentResult(
    {
      attachment: result.attachment,
      input,
      lease: result.lease,
      mutationResult: result.mutationResult,
    },
    code,
  );
}

function canonicalWriterLeaseRenewalResult(
  { attachment, input, lease },
  code,
) {
  const request = writerLeaseRenewalRequest(input, code);
  const previousLease = input.expectedSession.document.lease;
  const previousAttachment = input.expectedSession.document.attachment;
  const renewedLease = canonicalLeaseGrant(lease, code);
  const mounted = canonicalSessionAttachment(attachment, code);
  const decisionMilliseconds =
    timestampMilliseconds(renewedLease.expiresAt) -
    request.leaseDurationMilliseconds;
  ensure(
    previousLease !== null &&
      previousAttachment !== null &&
      renewedLease.sessionId === previousLease.sessionId &&
      renewedLease.leaseId === previousLease.leaseId &&
      renewedLease.holderId === previousLease.holderId &&
      renewedLease.fencingEpoch === previousLease.fencingEpoch &&
      timestampMilliseconds(previousLease.expiresAt) >
        decisionMilliseconds &&
      timestampMilliseconds(renewedLease.expiresAt) >
        timestampMilliseconds(previousLease.expiresAt) &&
      decisionMilliseconds >=
        timestampMilliseconds(input.expectedSession.updatedAt) &&
      canonicalSerialize(mounted) ===
        canonicalSerialize(previousAttachment),
    code,
  );
  canonicalLeaseAttachmentBinding({
    attachment: mounted,
    lease: renewedLease,
    manifest: input.expectedSession.document.manifest,
    storageRef: input.expectedSession.document.storageRef,
    code,
  });
  return deepFreeze({
    resultVersion: OPERATION_RESULT_VERSION,
    outcome: "writer-lease-renewed",
    lease: renewedLease,
    attachment: mounted,
  });
}

function canonicalWriterLeaseRenewalStoredResult(value, input, code) {
  const result = exactPlainObject(
    value,
    WRITER_LEASE_RENEWAL_RESULT_KEYS,
    code,
  );
  ensure(
    result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "writer-lease-renewed",
    code,
  );
  return canonicalWriterLeaseRenewalResult(
    {
      attachment: result.attachment,
      input,
      lease: result.lease,
    },
    code,
  );
}

function writerFenceTargetForInput(input, code) {
  if (input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND) {
    writerAttachmentRequest(input, code);
    return deepFreeze({
      attachmentId: attachmentIdForOperation(input.operationId),
      kind: "attachment",
    });
  }
  if (input.kind === WRITER_RELEASE_OPERATION_KIND) {
    return writerReleaseRequest(input, code).target;
  }
  if (input.kind === WRITER_FORCE_FENCE_OPERATION_KIND) {
    return writerForceFenceOperationRequest(input, code).target;
  }
  fail(code);
}

function canonicalWriterReleaseResult(
  { attachment, input, lease, mutationResult },
  code,
) {
  writerReleaseRequest(input, code);
  const expectedLease = input.expectedSession.document.lease;
  const expectedAttachment = input.expectedSession.document.attachment;
  const releasedLease = canonicalLeaseGrant(lease, code);
  const releasedAttachment = canonicalSessionAttachment(attachment, code);
  ensure(
    expectedLease !== null &&
      expectedAttachment !== null &&
      canonicalSerialize(releasedLease) ===
        canonicalSerialize(expectedLease) &&
      canonicalSerialize(releasedAttachment) ===
        canonicalSerialize(expectedAttachment),
    code,
  );
  const expectedMutation = detachMutationRequestFor(
    input,
    releasedLease,
    code,
  );
  const mutation = canonicalDetachMutationResult(
    mutationResult,
    expectedMutation,
    code,
  );
  ensure(
    releasedAttachment.attachmentId ===
      expectedMutation.target.attachmentId,
    code,
  );
  return deepFreeze({
    resultVersion: OPERATION_RESULT_VERSION,
    outcome: "writer-released",
    lease: releasedLease,
    attachment: releasedAttachment,
    mutationResult: mutation,
  });
}

function canonicalWriterReleaseStoredResult(value, input, code) {
  const result = exactPlainObject(
    value,
    WRITER_RELEASE_RESULT_KEYS,
    code,
  );
  ensure(
    result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "writer-released",
    code,
  );
  return canonicalWriterReleaseResult(
    {
      attachment: result.attachment,
      input,
      lease: result.lease,
      mutationResult: result.mutationResult,
    },
    code,
  );
}

function canonicalWriterForceFenceResult(
  { attachment, fenceResult, input, lease, writerEpoch },
  code,
) {
  const request = writerForceFenceOperationRequest(input, code);
  const document = input.expectedSession.document;
  const revokedLease = canonicalLeaseGrant(lease, code);
  const revokedAttachment =
    attachment === null
      ? null
      : canonicalSessionAttachment(attachment, code);
  const nextEpoch = nextWriterEpochForCode(document.writerEpoch, code);
  const canonicalEpoch = canonicalWriterEpoch(
    writerEpoch,
    code,
    true,
  );
  ensure(
    document.lease !== null &&
      document.backendCapabilities.fencing !== "manual" &&
      canonicalEpoch === nextEpoch &&
      canonicalSerialize(revokedLease) ===
        canonicalSerialize(document.lease) &&
      canonicalSerialize(revokedAttachment) ===
        canonicalSerialize(document.attachment),
    code,
  );
  const expectedFenceRequest = forceFenceRequestFor(
    input,
    canonicalEpoch,
    code,
  );
  const fenced = canonicalForceFenceResult(
    fenceResult,
    expectedFenceRequest,
    code,
  );
  ensure(
    canonicalSerialize(fenced.target) ===
      canonicalSerialize(request.target),
    code,
  );
  return deepFreeze({
    resultVersion: OPERATION_RESULT_VERSION,
    outcome: "writer-fenced",
    writerEpoch: canonicalEpoch,
    lease: revokedLease,
    attachment: revokedAttachment,
    fenceTarget: request.target,
    fenceResult: fenced,
  });
}

function canonicalWriterForceFenceStoredResult(value, input, code) {
  const result = exactPlainObject(
    value,
    WRITER_FORCE_FENCE_RESULT_KEYS,
    code,
  );
  ensure(
    result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "writer-fenced" &&
      canonicalSerialize(
        canonicalWriterAttachmentTarget(result.fenceTarget, code),
      ) ===
        canonicalSerialize(
          writerForceFenceOperationRequest(input, code).target,
        ),
    code,
  );
  return canonicalWriterForceFenceResult(
    {
      attachment: result.attachment,
      fenceResult: result.fenceResult,
      input,
      lease: result.lease,
      writerEpoch: result.writerEpoch,
    },
    code,
  );
}

function canonicalWriterBlockedResult(
  { attachment, input, lease, reason, writerEpoch },
  code,
) {
  const blockedReason = canonicalOpaqueId(reason, 64, code);
  ensure(
    reflectApply(arrayIncludesIntrinsic, WRITER_BLOCKED_REASONS, [
      blockedReason,
    ]),
    code,
  );
  const blockedLease = canonicalLeaseGrant(lease, code);
  const blockedAttachment =
    attachment === null
      ? null
      : canonicalSessionAttachment(attachment, code);
  const document = input.expectedSession.document;
  let expectedEpoch;
  if (input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND) {
    const request = writerAttachmentRequest(input, code);
    expectedEpoch = nextWriterEpochForCode(document.writerEpoch, code);
    ensure(
      blockedAttachment === null &&
        blockedLease.sessionId === input.expectedSession.sessionId &&
        blockedLease.leaseId === leaseIdForOperation(input.operationId) &&
        blockedLease.holderId === request.holderId &&
        blockedLease.fencingEpoch === expectedEpoch,
      code,
    );
  } else if (input.kind === WRITER_RELEASE_OPERATION_KIND) {
    writerReleaseRequest(input, code);
    expectedEpoch = document.writerEpoch;
    ensure(
      document.lease !== null &&
        document.attachment !== null &&
        canonicalSerialize(blockedLease) ===
          canonicalSerialize(document.lease) &&
        canonicalSerialize(blockedAttachment) ===
          canonicalSerialize(document.attachment),
      code,
    );
  } else if (input.kind === WRITER_FORCE_FENCE_OPERATION_KIND) {
    writerForceFenceOperationRequest(input, code);
    expectedEpoch = nextWriterEpochForCode(document.writerEpoch, code);
    ensure(
      document.lease !== null &&
        canonicalSerialize(blockedLease) ===
          canonicalSerialize(document.lease) &&
        canonicalSerialize(blockedAttachment) ===
          canonicalSerialize(document.attachment),
      code,
    );
  } else {
    fail(code);
  }
  const canonicalEpoch = canonicalWriterEpoch(
    writerEpoch,
    code,
    true,
  );
  ensure(canonicalEpoch === expectedEpoch, code);
  if (blockedAttachment !== null) {
    canonicalLeaseAttachmentBinding({
      attachment: blockedAttachment,
      lease: blockedLease,
      manifest: document.manifest,
      storageRef: document.storageRef,
      code,
    });
  }
  return deepFreeze({
    resultVersion: OPERATION_RESULT_VERSION,
    outcome: "writer-blocked",
    reason: blockedReason,
    writerEpoch: canonicalEpoch,
    lease: blockedLease,
    attachment: blockedAttachment,
    fenceTarget: writerFenceTargetForInput(input, code),
  });
}

function canonicalWriterBlockedStoredResult(value, input, code) {
  const result = exactPlainObject(
    value,
    WRITER_BLOCKED_RESULT_KEYS,
    code,
  );
  ensure(
    result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "writer-blocked" &&
      canonicalSerialize(
        canonicalWriterAttachmentTarget(result.fenceTarget, code),
      ) ===
        canonicalSerialize(writerFenceTargetForInput(input, code)),
    code,
  );
  return canonicalWriterBlockedResult(
    {
      attachment: result.attachment,
      input,
      lease: result.lease,
      reason: result.reason,
      writerEpoch: result.writerEpoch,
    },
    code,
  );
}

function writerLaunchOutcomeForStatus(status, code) {
  if (status === "started") return "writer-launch-started";
  if (status === "not-started") return "writer-launch-not-started";
  if (status === "complete-stopped") {
    return "writer-launch-complete-stopped";
  }
  fail(code);
}

function canonicalWriterLaunchTerminalResult(value, input, code) {
  const result = exactPlainObject(
    value,
    WRITER_LAUNCH_TERMINAL_RESULT_KEYS,
    code,
  );
  ensure(result.resultVersion === OPERATION_RESULT_VERSION, code);
  const evidence = canonicalWriterLaunchEvidence(
    result.evidence,
    input,
    ["started", "not-started", "complete-stopped"],
    code,
  );
  const outcome = writerLaunchOutcomeForStatus(evidence.status, code);
  ensure(result.outcome === outcome, code);
  return deepFreeze({
    evidence,
    outcome,
    resultVersion: OPERATION_RESULT_VERSION,
  });
}

function writerLaunchTerminalResult(input, evidence, code) {
  return canonicalWriterLaunchTerminalResult(
    {
      evidence,
      outcome: writerLaunchOutcomeForStatus(evidence.status, code),
      resultVersion: OPERATION_RESULT_VERSION,
    },
    input,
    code,
  );
}

function canonicalWriterLaunchStopTerminalResult(value, input, code) {
  const request = writerLaunchStopOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  const result = exactPlainObject(
    value,
    WRITER_LAUNCH_STOP_TERMINAL_RESULT_KEYS,
    code,
  );
  ensure(
    result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "writer-launch-stopped",
    code,
  );
  const evidence = canonicalWriterLaunchStopEvidence(
    result.evidence,
    { ...input, request },
    code,
  );
  return deepFreeze({
    evidence,
    outcome: "writer-launch-stopped",
    resultVersion: OPERATION_RESULT_VERSION,
  });
}

function writerLaunchStopTerminalResult(input, evidence, code) {
  return canonicalWriterLaunchStopTerminalResult(
    {
      evidence,
      outcome: "writer-launch-stopped",
      resultVersion: OPERATION_RESULT_VERSION,
    },
    input,
    code,
  );
}

function writerLaunchPointerFor(input, result, startedAt, code) {
  ensure(
    result.outcome === "writer-launch-started" &&
      result.evidence.processIncarnationId !== null &&
      result.evidence.writerIncarnationId !== null,
    code,
  );
  return canonicalWriterLaunchPointer(
    {
      attachmentId: input.request.attachment.attachmentId,
      attachmentSha256: sha256(
        canonicalSerialize(input.request.attachment),
      ),
      contractVersion: WRITER_LAUNCH_POINTER_CONTRACT_VERSION,
      fencingEpoch: input.request.fencingEpoch,
      generation: input.request.generation,
      launchAttemptId: input.operationId,
      launchResultSha256: sha256(canonicalSerialize(result)),
      leaseId: input.request.lease.leaseId,
      leaseSha256: sha256(canonicalSerialize(input.request.lease)),
      measuredImageSha256: sha256(
        canonicalSerialize(input.request.measuredImage),
      ),
      processIncarnationId: result.evidence.processIncarnationId,
      startedAt,
      supervisorId: input.request.supervisor.supervisorId,
      supervisorProofId: result.evidence.proofId,
      writerIncarnationId: result.evidence.writerIncarnationId,
    },
    input.expectedSession.sessionId,
    code,
  );
}

function canonicalCheckpointCaptureStoredResult(value, input, code) {
  const result = exactPlainObject(
    value,
    CHECKPOINT_CAPTURE_TERMINAL_RESULT_KEYS,
    code,
  );
  const request = checkpointCaptureOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  const captureAttemptId = canonicalSessionId(
    result.captureAttemptId,
    code,
  );
  const checkpointId = canonicalOpaqueId(result.checkpointId, 128, code);
  ensure(
    input.kind === CHECKPOINT_CAPTURE_OPERATION_KIND &&
      result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "checkpoint-captured" &&
      captureAttemptId === request.admission.captureAttemptId &&
      checkpointId === request.admission.checkpoint.checkpointId &&
      typeof result.catalogueSha256 === "string" &&
      regexpTest(SHA256_PATTERN, result.catalogueSha256),
    code,
  );
  return deepFreeze({
    captureAttemptId,
    catalogueSha256: result.catalogueSha256,
    checkpointId,
    outcome: "checkpoint-captured",
    resultVersion: OPERATION_RESULT_VERSION,
  });
}

function canonicalRestoreGenerationStoredResult(value, input, code) {
  const result = exactPlainObject(
    value,
    RESTORE_GENERATION_TERMINAL_RESULT_KEYS,
    code,
  );
  const request = restoreGenerationOperationRequest(
    input.request,
    input.expectedSession,
    code,
  );
  const checkpointId = canonicalOpaqueId(result.checkpointId, 128, code);
  const generationId = canonicalOpaqueId(result.generationId, 128, code);
  ensure(
    input.kind === RESTORE_DESTINATION_GENERATION_OPERATION_KIND &&
      result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "restore-generation-committed" &&
      checkpointId === request.admission.checkpoint.checkpointId &&
      typeof result.catalogueSha256 === "string" &&
      regexpTest(SHA256_PATTERN, result.catalogueSha256) &&
      typeof result.generationDocumentSha256 === "string" &&
      regexpTest(SHA256_PATTERN, result.generationDocumentSha256),
    code,
  );
  return deepFreeze({
    catalogueSha256: result.catalogueSha256,
    checkpointId,
    generationDocumentSha256: result.generationDocumentSha256,
    generationId,
    outcome: "restore-generation-committed",
    resultVersion: OPERATION_RESULT_VERSION,
  });
}

function canonicalRestoreAttachmentActivationStoredResult(
  value,
  input,
  code,
) {
  const result = exactPlainObject(
    value,
    RESTORE_ATTACHMENT_ACTIVATION_TERMINAL_RESULT_KEYS,
    code,
  );
  ensure(
    input.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND &&
      result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "restore-attachment-activated",
    code,
  );
  const activationRequest =
    structurallyCanonicalRestoreAttachmentActivationRequest(
      result.activationRequest,
      input,
      code,
    );
  const activationResult =
    canonicalRestoreAttachmentActivationProviderResult(
      result.activationResult,
      activationRequest,
      code,
    );
  return deepFreeze({
    activationRequest,
    activationResult,
    outcome: "restore-attachment-activated",
    resultVersion: OPERATION_RESULT_VERSION,
  });
}

function restoreAttachmentActivationTerminalResult(
  input,
  activationRequest,
  activationResult,
  code,
) {
  return canonicalRestoreAttachmentActivationStoredResult(
    {
      activationRequest,
      activationResult,
      outcome: "restore-attachment-activated",
      resultVersion: OPERATION_RESULT_VERSION,
    },
    input,
    code,
  );
}

function canonicalCommittedResult(value, input, revision, code) {
  const outcome = ownDataValue(value, "outcome", code);
  if (outcome === "cancelled-before-dispatch") {
    ensure(revision === "1", code);
    return canonicalCancellationResult(value, code);
  }
  if (outcome === "writer-attached") {
    ensure(
      input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND &&
        (revision === "2" || revision === "3"),
      code,
    );
    return canonicalWriterAttachmentStoredResult(value, input, code);
  }
  if (outcome === "writer-lease-renewed") {
    ensure(
      input.kind === WRITER_LEASE_RENEW_OPERATION_KIND &&
        revision === "0",
      code,
    );
    return canonicalWriterLeaseRenewalStoredResult(value, input, code);
  }
  if (outcome === "writer-released") {
    ensure(
      input.kind === WRITER_RELEASE_OPERATION_KIND &&
        (revision === "2" || revision === "3"),
      code,
    );
    return canonicalWriterReleaseStoredResult(value, input, code);
  }
  if (outcome === "writer-fenced") {
    ensure(
      input.kind === WRITER_FORCE_FENCE_OPERATION_KIND &&
        (revision === "2" || revision === "3"),
      code,
    );
    return canonicalWriterForceFenceStoredResult(value, input, code);
  }
  if (outcome === "writer-blocked") {
    ensure(
      revision === "3" &&
        (input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND ||
          input.kind === WRITER_RELEASE_OPERATION_KIND ||
          input.kind === WRITER_FORCE_FENCE_OPERATION_KIND),
      code,
    );
    return canonicalWriterBlockedStoredResult(value, input, code);
  }
  if (outcome === "checkpoint-captured") {
    ensure(
      input.kind === CHECKPOINT_CAPTURE_OPERATION_KIND &&
        (revision === "2" || revision === "3"),
      code,
    );
    return canonicalCheckpointCaptureStoredResult(value, input, code);
  }
  if (outcome === "restore-generation-committed") {
    ensure(
      input.kind === RESTORE_DESTINATION_GENERATION_OPERATION_KIND &&
        (revision === "2" || revision === "3"),
      code,
    );
    return canonicalRestoreGenerationStoredResult(value, input, code);
  }
  if (outcome === "restore-attachment-activated") {
    ensure(
      input.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND &&
        (revision === "2" || revision === "3"),
      code,
    );
    return canonicalRestoreAttachmentActivationStoredResult(
      value,
      input,
      code,
    );
  }
  if (
    outcome === "writer-launch-started" ||
    outcome === "writer-launch-not-started" ||
    outcome === "writer-launch-complete-stopped"
  ) {
    ensure(
      input.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND &&
        (revision === "2" || revision === "3"),
      code,
    );
    return canonicalWriterLaunchTerminalResult(value, input, code);
  }
  if (outcome === "writer-launch-stopped") {
    ensure(
      input.kind === WRITER_LAUNCH_STOP_OPERATION_KIND &&
        (revision === "2" || revision === "3"),
      code,
    );
    return canonicalWriterLaunchStopTerminalResult(value, input, code);
  }
  fail(code);
}

function operationSnapshotFromRow(row) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(row, OPERATION_ROW_KEYS, code);
  const operationId = canonicalOpaqueId(
    normalized.operation_id,
    128,
    code,
  );
  const sessionId = canonicalSessionId(normalized.session_id, code);
  const kind = canonicalOpaqueId(normalized.kind, 64, code);
  const envelope = canonicalOperationEnvelope(normalized.request, code);
  ensure(
    envelope.expectedSession.sessionId === sessionId &&
      envelope.expectedSession.document.activeOperation === null,
    code,
  );
  const state = canonicalOpaqueId(normalized.state, 32, code);
  ensure(
    reflectApply(arrayIncludesIntrinsic, ACTIVE_OPERATION_STATES, [state]) ||
      state === "committed",
    code,
  );
  const revision = canonicalRevisionForCode(normalized.revision, code);
  const createdAt = canonicalTimestampForCode(normalized.created_at, code);
  const updatedAt = canonicalTimestampForCode(normalized.updated_at, code);
  const retiredAt = canonicalNullableRowTimestamp(
    normalized.retired_at,
    code,
  );
  ensure(
    timestampMilliseconds(updatedAt) >= timestampMilliseconds(createdAt),
    code,
  );
  const requestSha256 = sha256(canonicalSerialize(envelope));
  const input = operationInputFromEnvelope({
    envelope,
    kind,
    operationId,
    requestSha256,
  });
  let result = null;
  if (state === "prepared") {
    ensure(
      revision === "0" &&
        normalized.result === null &&
        retiredAt === null &&
        createdAt === updatedAt,
      code,
    );
  } else if (state === "starting") {
    ensure(
      revision === "1" &&
        normalized.result === null &&
        retiredAt === null,
      code,
    );
  } else if (state === "uncertain") {
    ensure(
      revision === "2" &&
        normalized.result === null &&
        retiredAt === null,
      code,
    );
  } else {
    ensure(retiredAt === updatedAt, code);
    result = canonicalCommittedResult(
      normalized.result,
      input,
      revision,
      code,
    );
    if (revision === "0") {
      ensure(createdAt === updatedAt, code);
    }
    if (result.outcome === "writer-attached") {
      ensure(
        timestampMilliseconds(result.lease.expiresAt) -
          writerAttachmentRequest(input, code)
            .leaseDurationMilliseconds >=
          timestampMilliseconds(createdAt),
        code,
      );
    } else if (
      result.outcome === "writer-blocked" &&
      input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND
    ) {
      ensure(
        timestampMilliseconds(result.lease.expiresAt) -
          writerAttachmentRequest(input, code)
            .leaseDurationMilliseconds >=
          timestampMilliseconds(createdAt),
        code,
      );
    } else if (result.outcome === "writer-lease-renewed") {
      ensure(
        timestampMilliseconds(result.lease.expiresAt) -
          writerLeaseRenewalRequest(input, code)
            .leaseDurationMilliseconds >=
          timestampMilliseconds(createdAt),
        code,
      );
    }
  }
  return deepFreeze({
    operationId,
    sessionId,
    kind,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: envelope.expectedSession,
    request: envelope.payload,
    requestSha256,
    state,
    revision,
    result,
    createdAt,
    updatedAt,
    retiredAt,
  });
}

function operationIdClaimSnapshotFromRow(row) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(
    row,
    OPERATION_ID_CLAIM_ROW_KEYS,
    code,
  );
  const operationId = canonicalOpaqueId(
    normalized.operation_id,
    128,
    code,
  );
  const sessionId = canonicalSessionId(normalized.session_id, code);
  const claimType = canonicalOpaqueId(normalized.claim_type, 64, code);
  const claimantOperationId =
    normalized.claimant_operation_id === null
      ? null
      : canonicalOpaqueId(
          normalized.claimant_operation_id,
          128,
          code,
        );
  const binding =
    normalized.binding === null
      ? null
      : canonicalJsonObject(normalized.binding, code);
  const claimedAt = canonicalTimestampForCode(normalized.claimed_at, code);
  const materializedAt = canonicalNullableRowTimestamp(
    normalized.materialized_at,
    code,
  );
  if (claimType === DIRECT_OPERATION_ID_CLAIM_TYPE) {
    ensure(
      claimantOperationId === null &&
        binding === null &&
        materializedAt === claimedAt,
      code,
    );
  } else {
    ensure(
      (claimType === RESTORE_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE ||
        claimType ===
          RESTORE_ACTIVATION_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE) &&
        claimantOperationId !== null &&
        claimantOperationId !== operationId &&
        binding !== null &&
        (materializedAt === null ||
          timestampMilliseconds(materializedAt) >=
            timestampMilliseconds(claimedAt)),
      code,
    );
  }
  return deepFreeze({
    binding,
    claimedAt,
    claimantOperationId,
    claimType,
    materializedAt,
    operationId,
    sessionId,
  });
}

function validateRestoreLaunchIdClaim(claim, input, materialization) {
  ensure(
    input.request.contractVersion ===
        RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2 &&
      claim.operationId === input.request.launchIntent.launchAttemptId &&
      claim.sessionId === input.expectedSession.sessionId &&
      claim.claimType ===
        RESTORE_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE &&
      claim.claimantOperationId === input.operationId &&
      canonicalSerialize(claim.binding) ===
        canonicalSerialize(input.request.launchIntent) &&
      (materialization === "either" ||
        (materialization === "reserved" &&
          claim.materializedAt === null) ||
        (materialization === "materialized" &&
          claim.materializedAt !== null)),
    "operation_state_invalid",
  );
  return claim;
}

function validateRestoreAttachmentActivationLaunchIdClaim(
  claim,
  input,
  materialization,
) {
  ensure(
    claim.operationId === input.request.launchIntent.launchAttemptId &&
      claim.sessionId === input.expectedSession.sessionId &&
      claim.claimType ===
        RESTORE_ACTIVATION_LAUNCH_INTENT_OPERATION_ID_CLAIM_TYPE &&
      claim.claimantOperationId === input.operationId &&
      canonicalSerialize(claim.binding) ===
        canonicalSerialize(input.request.launchIntent) &&
      (materialization === "either" ||
        (materialization === "reserved" &&
          claim.materializedAt === null) ||
        (materialization === "materialized" &&
          claim.materializedAt !== null)),
    "operation_state_invalid",
  );
  return claim;
}

function canonicalReservationPayload(value, code) {
  const payload = exactPlainObject(value, RESERVATION_PAYLOAD_KEYS, code);
  ensure(
    payload.reservationVersion === RESERVATION_PAYLOAD_VERSION &&
      payload.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      typeof payload.requestSha256 === "string" &&
      regexpTest(SHA256_PATTERN, payload.requestSha256),
    code,
  );
  return deepFreeze({
    reservationVersion: RESERVATION_PAYLOAD_VERSION,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    requestSha256: payload.requestSha256,
  });
}

function reservationSnapshotFromRow(row) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(row, RESERVATION_ROW_KEYS, code);
  const reservationId = canonicalOpaqueId(
    normalized.reservation_id,
    128,
    code,
  );
  const operationId = canonicalOpaqueId(
    normalized.operation_id,
    128,
    code,
  );
  const sessionId = canonicalSessionId(normalized.session_id, code);
  const kind = canonicalOpaqueId(normalized.kind, 64, code);
  const expectedSessionRevision = canonicalRevisionForCode(
    normalized.expected_session_revision,
    code,
  );
  const state = canonicalOpaqueId(normalized.state, 32, code);
  ensure(
    reflectApply(arrayIncludesIntrinsic, ACTIVE_OPERATION_STATES, [state]) ||
      state === "released",
    code,
  );
  const payload = canonicalReservationPayload(normalized.payload, code);
  const createdAt = canonicalTimestampForCode(normalized.created_at, code);
  const updatedAt = canonicalTimestampForCode(normalized.updated_at, code);
  const expiresAt = canonicalNullableRowTimestamp(
    normalized.expires_at,
    code,
  );
  const releasedAt = canonicalNullableRowTimestamp(
    normalized.released_at,
    code,
  );
  ensure(
    expiresAt === null &&
      timestampMilliseconds(updatedAt) >= timestampMilliseconds(createdAt),
    code,
  );
  if (state === "prepared") {
    ensure(createdAt === updatedAt && releasedAt === null, code);
  } else if (state === "released") {
    ensure(releasedAt === updatedAt, code);
  } else {
    ensure(releasedAt === null, code);
  }
  return deepFreeze({
    reservationId,
    operationId,
    sessionId,
    kind,
    expectedSessionRevision,
    state,
    conflictClass: payload.conflictClass,
    requestSha256: payload.requestSha256,
    createdAt,
    updatedAt,
    expiresAt,
    releasedAt,
  });
}

function captureAttemptIdentityFromRow(row) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(row, CAPTURE_ATTEMPT_ROW_KEYS, code);
  return deepFreeze({
    captureAttemptId: canonicalSessionId(
      normalized.capture_attempt_id,
      code,
    ),
    operationId: canonicalOpaqueId(
      normalized.operation_id,
      128,
      code,
    ),
    sessionId: canonicalSessionId(normalized.session_id, code),
  });
}

function checkpointCatalogueIdentityFromRow(row) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(
    row,
    CHECKPOINT_CATALOGUE_ROW_KEYS,
    code,
  );
  return deepFreeze({
    captureAttemptId: canonicalSessionId(
      normalized.capture_attempt_id,
      code,
    ),
    checkpointId: canonicalOpaqueId(
      normalized.checkpoint_id,
      128,
      code,
    ),
    sessionId: canonicalSessionId(normalized.session_id, code),
  });
}

function captureAttemptSnapshotFromRow(row, input) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(
    row,
    CAPTURE_ATTEMPT_ROW_KEYS,
    code,
  );
  const captureAttemptId = canonicalSessionId(
    normalized.capture_attempt_id,
    code,
  );
  const operationId = canonicalOpaqueId(
    normalized.operation_id,
    128,
    code,
  );
  const sessionId = canonicalSessionId(normalized.session_id, code);
  const claimedAt = canonicalTimestampForCode(
    normalized.claimed_at,
    code,
  );
  ensure(
    operationId === input.operationId &&
      sessionId === input.expectedSession.sessionId &&
      captureAttemptId === input.request.admission.captureAttemptId,
    code,
  );
  const binding = canonicalCheckpointCaptureBinding(
    normalized.binding,
    input,
    code,
  );
  return deepFreeze({
    binding,
    captureAttemptId,
    claimedAt,
    operationId,
    sessionId,
  });
}

function captureAttemptTombstoneSnapshotFromRow(row, input) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(
    row,
    CAPTURE_ATTEMPT_TOMBSTONE_ROW_KEYS,
    code,
  );
  const captureAttemptId = canonicalSessionId(
    normalized.capture_attempt_id,
    code,
  );
  const operationId = canonicalOpaqueId(
    normalized.operation_id,
    128,
    code,
  );
  const sessionId = canonicalSessionId(normalized.session_id, code);
  const retiredAt = canonicalTimestampForCode(
    normalized.retired_at,
    code,
  );
  const tombstone = canonicalJsonObject(normalized.tombstone, code);
  ensure(
    captureAttemptId === input.request.admission.captureAttemptId &&
      operationId === input.operationId &&
      sessionId === input.expectedSession.sessionId,
    code,
  );
  return deepFreeze({
    captureAttemptId,
    operationId,
    retiredAt,
    sessionId,
    tombstone,
  });
}

function checkpointCatalogueSnapshotFromRow(row, input) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(
    row,
    CHECKPOINT_CATALOGUE_ROW_KEYS,
    code,
  );
  const checkpointId = canonicalOpaqueId(
    normalized.checkpoint_id,
    128,
    code,
  );
  const sessionId = canonicalSessionId(normalized.session_id, code);
  const captureAttemptId = canonicalSessionId(
    normalized.capture_attempt_id,
    code,
  );
  const committedAt = canonicalTimestampForCode(
    normalized.committed_at,
    code,
  );
  const document = canonicalCheckpointCatalogueDocument(
    normalized.document,
    input,
    code,
  );
  ensure(
    checkpointId === input.request.admission.checkpoint.checkpointId &&
      sessionId === input.expectedSession.sessionId &&
      captureAttemptId === input.request.admission.captureAttemptId,
    code,
  );
  return deepFreeze({
    captureAttemptId,
    checkpointId,
    committedAt,
    document,
    sessionId,
  });
}

function restoreGenerationIdentityFromRow(row) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(
    row,
    RESTORE_GENERATION_ROW_KEYS,
    code,
  );
  return deepFreeze({
    checkpointId: canonicalOpaqueId(
      normalized.checkpoint_id,
      128,
      code,
    ),
    generationId: canonicalOpaqueId(
      normalized.generation_id,
      128,
      code,
    ),
    operationId: canonicalOpaqueId(
      normalized.operation_id,
      128,
      code,
    ),
    sessionId: canonicalSessionId(normalized.session_id, code),
  });
}

function restoreGenerationSnapshotFromRow(row, input, source) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(
    row,
    RESTORE_GENERATION_ROW_KEYS,
    code,
  );
  const identity = restoreGenerationIdentityFromRow(row);
  ensure(
    identity.operationId === input.operationId &&
      identity.sessionId === input.expectedSession.sessionId &&
      identity.checkpointId ===
        input.request.admission.checkpoint.checkpointId,
    code,
  );
  const state = canonicalOpaqueId(normalized.state, 32, code);
  ensure(state === "authorized" || state === "committed", code);
  const claimedAt = canonicalTimestampForCode(normalized.claimed_at, code);
  const committedAt = canonicalNullableRowTimestamp(
    normalized.committed_at,
    code,
  );
  const binding = canonicalRestoreGenerationBinding(
    normalized.binding,
    input,
    source,
    identity.generationId,
    code,
  );
  let document = null;
  if (state === "authorized") {
    ensure(normalized.document === null && committedAt === null, code);
  } else {
    ensure(normalized.document !== null && committedAt !== null, code);
    document = canonicalRestoreGenerationDocument(
      normalized.document,
      input,
      source,
      binding,
      code,
    );
    ensure(
      timestampMilliseconds(committedAt) >=
        timestampMilliseconds(claimedAt),
      code,
    );
  }
  return deepFreeze({
    binding,
    checkpointId: identity.checkpointId,
    claimedAt,
    committedAt,
    document,
    generationId: identity.generationId,
    operationId: identity.operationId,
    sessionId: identity.sessionId,
    state,
  });
}

function checkpointCaptureAttemptRecord(input, attempt, catalogue) {
  return deepFreeze({
    binding: attempt.binding,
    captureAttemptId: attempt.captureAttemptId,
    contractVersion: CHECKPOINT_CAPTURE_ATTEMPT_CONTRACT_VERSION,
    operationId: attempt.operationId,
    request: input.request.admission.request,
    result: input.request.predeterminedResult,
    state: catalogue === null ? "authorized" : "committed",
  });
}

function checkpointCaptureTerminalResult(input, catalogueDocument) {
  return deepFreeze({
    captureAttemptId: input.request.admission.captureAttemptId,
    catalogueSha256: sha256(canonicalSerialize(catalogueDocument)),
    checkpointId: input.request.admission.checkpoint.checkpointId,
    outcome: "checkpoint-captured",
    resultVersion: OPERATION_RESULT_VERSION,
  });
}

function restoreGenerationTerminalResult(input, source, generation) {
  ensure(
    generation.state === "committed" && generation.document !== null,
    "operation_state_invalid",
  );
  return deepFreeze({
    catalogueSha256: sha256(
      canonicalSerialize(source.catalogue.document),
    ),
    checkpointId: input.request.admission.checkpoint.checkpointId,
    generationDocumentSha256: sha256(
      canonicalSerialize(generation.document),
    ),
    generationId: generation.generationId,
    outcome: "restore-generation-committed",
    resultVersion: OPERATION_RESULT_VERSION,
  });
}

function writerLaunchAttemptRecord(input, operation) {
  return deepFreeze({
    contractVersion: WRITER_LAUNCH_ATTEMPT_OPERATION_CONTRACT_VERSION,
    launchAttemptId: operation.operationId,
    request: input.request,
    result: operation.result,
    state: operation.state,
  });
}

function writerLaunchStopRecord(input, operation) {
  return deepFreeze({
    contractVersion: input.request.contractVersion,
    launchAttemptId: input.request.launch.launchAttemptId,
    request: input.request,
    result: operation.result,
    state: operation.state,
    stopOperationId: operation.operationId,
  });
}

function currentWriterLaunchForAttempt(session, launchAttemptId) {
  const launch = session.document.launch;
  return launch?.launchAttemptId === launchAttemptId ? launch : null;
}

function validateOperationIdentity(operation, input) {
  ensure(
    operation.operationId === input.operationId &&
      operation.sessionId === input.expectedSession.sessionId &&
      operation.kind === input.kind &&
      operation.requestSha256 === input.requestSha256 &&
      canonicalSnapshotBytes(operation.expectedSession) ===
        canonicalSnapshotBytes(input.expectedSession) &&
      canonicalSerialize(operation.request) === canonicalSerialize(input.request),
    "operation_identity_conflict",
  );
}

function validateCommittedOperationHistory(operation, session) {
  ensure(
    operation.state === "committed" &&
      operation.sessionId === session.sessionId &&
      canonicalIdentityBytes(session.document) ===
        canonicalIdentityBytes(operation.expectedSession.document) &&
      session.createdAt === operation.expectedSession.createdAt &&
      BigIntConstructor(session.revision) >=
        BigIntConstructor(
          revisionAfter(
            operation.expectedSession.revision,
            BigIntConstructor(operation.revision) + 1n,
            "operation_state_invalid",
          ),
        ),
    "operation_state_invalid",
  );
}

function validateOperationReservation(operation, reservation, input) {
  const expectedReservationState =
    operation.state === "committed" ? "released" : operation.state;
  ensure(
    reservation.reservationId === input.reservationId &&
      reservation.operationId === operation.operationId &&
      reservation.sessionId === operation.sessionId &&
      reservation.kind === operation.kind &&
      reservation.expectedSessionRevision ===
        operation.expectedSession.revision &&
      reservation.state === expectedReservationState &&
      reservation.requestSha256 === operation.requestSha256 &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt &&
      (operation.state !== "committed" ||
        reservation.releasedAt === operation.retiredAt),
    "operation_state_invalid",
  );
}

function inputForOperation(operation) {
  return deepFreeze({
    expectedSession: operation.expectedSession,
    kind: operation.kind,
    operationId: operation.operationId,
    request: operation.request,
  });
}

function validateActiveBusinessState(session, operation) {
  const expectedDocument = operation.expectedSession.document;
  if (
    operation.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND &&
    operation.state !== "prepared"
  ) {
    const input = restoreAttachmentActivationInput(
      inputForOperation(operation),
      OPERATION_INPUT_KEYS,
      "operation_state_invalid",
    );
    const request = input.request;
    const lease = session.document.lease;
    const expectedEpoch = nextWriterEpochForCode(
      expectedDocument.writerEpoch,
      "operation_state_invalid",
    );
    ensure(
      session.document.lifecycle === "ATTACHING" &&
        session.document.writerEpoch === expectedEpoch &&
        lease !== null &&
        session.document.attachment === null &&
        session.document.launch === null &&
        lease.sessionId === operation.sessionId &&
        lease.leaseId === leaseIdForOperation(operation.operationId) &&
        lease.holderId === request.holderId &&
        lease.fencingEpoch === expectedEpoch &&
        timestampMilliseconds(lease.expiresAt) -
            request.leaseDurationMilliseconds >=
          timestampMilliseconds(
            operation.state === "starting"
              ? operation.updatedAt
              : operation.createdAt,
          ),
      "operation_state_invalid",
    );
    return;
  }
  const typedWriterOperation =
    operation.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND ||
    operation.kind === WRITER_RELEASE_OPERATION_KIND ||
    operation.kind === WRITER_FORCE_FENCE_OPERATION_KIND;
  if (!typedWriterOperation || operation.state === "prepared") {
    ensure(
      canonicalBusinessBytes(session.document) ===
        canonicalBusinessBytes(expectedDocument),
      "operation_state_invalid",
    );
    return;
  }
  const input = inputForOperation(operation);
  if (operation.kind === WRITER_RELEASE_OPERATION_KIND) {
    writerReleaseRequest(input, "operation_state_invalid");
    ensure(
      session.document.lifecycle === "RELEASING" &&
        session.document.writerEpoch === expectedDocument.writerEpoch &&
        canonicalSerialize(session.document.lease) ===
          canonicalSerialize(expectedDocument.lease) &&
        canonicalSerialize(session.document.attachment) ===
          canonicalSerialize(expectedDocument.attachment),
      "operation_state_invalid",
    );
    return;
  }
  if (operation.kind === WRITER_FORCE_FENCE_OPERATION_KIND) {
    writerForceFenceOperationRequest(input, "operation_state_invalid");
    ensure(
      session.document.lifecycle === "FENCING" &&
        session.document.writerEpoch ===
          nextWriterEpochForCode(
            expectedDocument.writerEpoch,
            "operation_state_invalid",
          ) &&
        canonicalSerialize(session.document.lease) ===
        canonicalSerialize(expectedDocument.lease) &&
        canonicalSerialize(session.document.attachment) ===
          canonicalSerialize(expectedDocument.attachment) &&
        canonicalSerialize(session.document.launch) ===
          canonicalSerialize(expectedDocument.launch),
      "operation_state_invalid",
    );
    return;
  }
  const request = writerAttachmentRequest(input, "operation_state_invalid");
  const lease = session.document.lease;
  const expectedEpoch = nextWriterEpochForCode(
    expectedDocument.writerEpoch,
    "operation_state_invalid",
  );
  ensure(
    session.document.lifecycle === "ATTACHING" &&
      session.document.writerEpoch === expectedEpoch &&
      lease !== null &&
      session.document.attachment === null &&
      lease.sessionId === operation.sessionId &&
      lease.leaseId === leaseIdForOperation(operation.operationId) &&
      lease.holderId === request.holderId &&
      lease.fencingEpoch === expectedEpoch &&
      timestampMilliseconds(lease.expiresAt) -
        request.leaseDurationMilliseconds >=
        timestampMilliseconds(
          operation.state === "starting"
            ? operation.updatedAt
            : operation.createdAt,
        ),
    "operation_state_invalid",
  );
}

function validateTerminalBusinessState(terminalBase, operation) {
  const result = operation.result;
  ensure(result !== null, "operation_state_invalid");
  if (result.outcome === "cancelled-before-dispatch") {
    ensure(
      operation.expectedSession.document.lifecycle !== "BLOCKED" &&
      canonicalBusinessBytes(terminalBase.document) ===
        canonicalBusinessBytes(operation.expectedSession.document),
      "operation_state_invalid",
    );
    return;
  }
  if (
    result.outcome === "writer-attached" ||
    result.outcome === "writer-lease-renewed"
  ) {
    ensure(
      terminalBase.document.lifecycle === "ATTACHED" &&
        terminalBase.document.writerEpoch === result.lease.fencingEpoch &&
        canonicalSerialize(terminalBase.document.lease) ===
          canonicalSerialize(result.lease) &&
        canonicalSerialize(terminalBase.document.attachment) ===
          canonicalSerialize(result.attachment) &&
        canonicalSerialize(terminalBase.document.launch) ===
          canonicalSerialize(
            operation.expectedSession.document.launch,
          ),
      "operation_state_invalid",
    );
    return;
  }
  if (result.outcome === "writer-released") {
    ensure(
      terminalBase.document.lifecycle === "DETACHED" &&
        terminalBase.document.writerEpoch === result.lease.fencingEpoch &&
        terminalBase.document.lease === null &&
        terminalBase.document.attachment === null,
      "operation_state_invalid",
    );
    return;
  }
  if (result.outcome === "writer-fenced") {
    ensure(
      terminalBase.document.lifecycle === "DETACHED" &&
        terminalBase.document.writerEpoch === result.writerEpoch &&
        terminalBase.document.lease === null &&
        terminalBase.document.attachment === null,
      "operation_state_invalid",
    );
    return;
  }
  if (result.outcome === "checkpoint-captured") {
    ensure(
      operation.kind === CHECKPOINT_CAPTURE_OPERATION_KIND &&
        operation.expectedSession.document.lifecycle === "ATTACHED" &&
        canonicalBusinessBytes(terminalBase.document) ===
          canonicalBusinessBytes(operation.expectedSession.document),
      "operation_state_invalid",
    );
    return;
  }
  if (result.outcome === "restore-generation-committed") {
    ensure(
      operation.kind === RESTORE_DESTINATION_GENERATION_OPERATION_KIND &&
        operation.expectedSession.document.lifecycle === "ATTACHED" &&
        canonicalBusinessBytes(terminalBase.document) ===
          canonicalBusinessBytes(operation.expectedSession.document),
      "operation_state_invalid",
    );
    return;
  }
  if (result.outcome === "restore-attachment-activated") {
    const activationResult = result.activationResult;
    const activationRequest = result.activationRequest;
    ensure(
      operation.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND &&
        terminalBase.document.lifecycle === "ATTACHED" &&
        terminalBase.document.writerEpoch ===
          activationRequest.lease.fencingEpoch &&
        canonicalSerialize(
          canonicalJsonObject(
            terminalBase.document.lease,
            "operation_state_invalid",
          ),
        ) ===
          canonicalSerialize(
            canonicalJsonObject(
              activationRequest.lease,
              "operation_state_invalid",
            ),
          ) &&
        canonicalSerialize(
          canonicalJsonObject(
            terminalBase.document.attachment,
            "operation_state_invalid",
          ),
        ) ===
          canonicalSerialize(
            canonicalJsonObject(
              activationResult.attachment,
              "operation_state_invalid",
            ),
          ) &&
        terminalBase.document.launch === null,
      "operation_state_invalid",
    );
    return;
  }
  if (result.outcome === "writer-launch-started") {
    const input = writerLaunchAttemptInput(
      inputForOperation(operation),
      OPERATION_INPUT_KEYS,
      "operation_state_invalid",
    );
    const expectedLaunch = writerLaunchPointerFor(
      input,
      result,
      operation.updatedAt,
      "operation_state_invalid",
    );
    ensure(
      terminalBase.document.lifecycle === "ATTACHED" &&
        canonicalSerialize(
          canonicalJsonObject(
            terminalBase.document.lease,
            "operation_state_invalid",
          ),
        ) ===
          canonicalSerialize(input.request.lease) &&
        canonicalSerialize(
          canonicalJsonObject(
            terminalBase.document.attachment,
            "operation_state_invalid",
          ),
        ) ===
          canonicalSerialize(input.request.attachment) &&
        canonicalSerialize(terminalBase.document.launch) ===
          canonicalSerialize(expectedLaunch),
      "operation_state_invalid",
    );
    return;
  }
  if (
    result.outcome === "writer-launch-not-started" ||
    result.outcome === "writer-launch-complete-stopped"
  ) {
    ensure(
      canonicalBusinessBytes(terminalBase.document) ===
        canonicalBusinessBytes(operation.expectedSession.document),
      "operation_state_invalid",
    );
    return;
  }
  if (result.outcome === "writer-launch-stopped") {
    const input = writerLaunchStopInput(
      inputForOperation(operation),
      OPERATION_INPUT_KEYS,
      "operation_state_invalid",
    );
    const expectedDocument = documentWithAuthorityState(
      input.expectedSession.document,
      { launch: null },
    );
    ensure(
      terminalBase.document.lifecycle === "ATTACHED" &&
        terminalBase.document.launch === null &&
        canonicalBusinessBytes(terminalBase.document) ===
          canonicalBusinessBytes(expectedDocument),
      "operation_state_invalid",
    );
    return;
  }
  ensure(
    result.outcome === "writer-blocked" &&
      terminalBase.document.lifecycle === "BLOCKED" &&
      terminalBase.document.writerEpoch === result.writerEpoch &&
      canonicalSerialize(terminalBase.document.lease) ===
        canonicalSerialize(result.lease) &&
      canonicalSerialize(terminalBase.document.attachment) ===
        canonicalSerialize(result.attachment) &&
      canonicalSerialize(terminalBase.document.launch) ===
        canonicalSerialize(operation.expectedSession.document.launch),
    "operation_state_invalid",
  );
}

function validateActivePointer(session, operation, reservation) {
  const active = session.document.activeOperation;
  ensure(
    active !== null &&
      operation.state !== "committed" &&
      active.operationId === operation.operationId &&
      active.reservationId === reservation.reservationId &&
      active.kind === operation.kind &&
      active.state === operation.state &&
      active.expectedSessionRevision ===
        operation.expectedSession.revision &&
      active.operationRevision === operation.revision &&
      active.requestSha256 === operation.requestSha256 &&
      active.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      canonicalIdentityBytes(session.document) ===
        canonicalIdentityBytes(operation.expectedSession.document) &&
      session.createdAt === operation.expectedSession.createdAt &&
      session.updatedAt === operation.updatedAt,
    "operation_state_invalid",
  );
  validateActiveBusinessState(session, operation);
}

function validateLastOperationPointer(
  terminalBase,
  operation,
  reservation,
) {
  const last = documentLastOperation(terminalBase.document);
  ensure(
    last !== null &&
      terminalBase.document.activeOperation === null &&
      operation.state === "committed" &&
      last.operationId === operation.operationId &&
      last.reservationId === reservation.reservationId &&
      last.kind === operation.kind &&
      last.state === operation.state &&
      last.expectedSessionRevision === operation.expectedSession.revision &&
      last.operationRevision === operation.revision &&
      last.requestSha256 === operation.requestSha256 &&
      last.resultSha256 === sha256(canonicalSerialize(operation.result)) &&
      last.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      terminalBase.sessionId === operation.sessionId &&
      canonicalIdentityBytes(terminalBase.document) ===
        canonicalIdentityBytes(operation.expectedSession.document) &&
      terminalBase.createdAt === operation.expectedSession.createdAt &&
      terminalBase.updatedAt === operation.updatedAt &&
      operation.retiredAt === operation.updatedAt &&
      reservation.releasedAt === operation.updatedAt &&
      reservation.updatedAt === operation.updatedAt,
    "operation_state_invalid",
  );
  validateTerminalBusinessState(terminalBase, operation);
  validateOperationReservation(operation, reservation, {
    reservationId: last.reservationId,
  });
}

function canonicalCommittedWriterLaunchStopOperation(value, code) {
  const normalized = exactPlainObject(
    value,
    COMMITTED_WRITER_LAUNCH_STOP_OPERATION_KEYS,
    code,
  );
  let input;
  try {
    input = canonicalOperationInput({
      expectedSession: normalized.expectedSession,
      kind: normalized.kind,
      operationId: normalized.operationId,
      request: normalized.request,
    });
  } catch {
    fail(code);
  }
  const revision = canonicalRevisionForCode(normalized.revision, code);
  const createdAt = canonicalTimestampString(normalized.createdAt, code);
  const updatedAt = canonicalTimestampString(normalized.updatedAt, code);
  const retiredAt = canonicalTimestampString(normalized.retiredAt, code);
  ensure(
    input.kind === WRITER_LAUNCH_STOP_OPERATION_KIND &&
      normalized.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      normalized.sessionId === input.expectedSession.sessionId &&
      normalized.state === "committed" &&
      normalized.requestSha256 === input.requestSha256 &&
      retiredAt === updatedAt &&
      timestampMilliseconds(updatedAt) >= timestampMilliseconds(createdAt),
    code,
  );
  const result = canonicalCommittedResult(
    normalized.result,
    input,
    revision,
    code,
  );
  ensure(
    result.outcome === "writer-launch-stopped" &&
      canonicalSerialize(normalized.result) === canonicalSerialize(result),
    code,
  );
  const operation = deepFreeze({
    operationId: input.operationId,
    sessionId: input.expectedSession.sessionId,
    kind: input.kind,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: input.expectedSession,
    request: input.request,
    requestSha256: input.requestSha256,
    state: "committed",
    revision,
    result,
    createdAt,
    updatedAt,
    retiredAt,
  });
  validateOperationIdentity(operation, input);
  return deepFreeze({ input, operation });
}

function canonicalCommittedWriterLaunchStopReservation(value, code) {
  const reservation = exactPlainObject(
    value,
    COMMITTED_WRITER_LAUNCH_STOP_RESERVATION_KEYS,
    code,
  );
  const createdAt = canonicalTimestampString(reservation.createdAt, code);
  const updatedAt = canonicalTimestampString(reservation.updatedAt, code);
  const releasedAt = canonicalTimestampString(reservation.releasedAt, code);
  ensure(
    reservation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      reservation.state === "released" &&
      reservation.expiresAt === null &&
      typeof reservation.requestSha256 === "string" &&
      regexpTest(SHA256_PATTERN, reservation.requestSha256) &&
      releasedAt === updatedAt &&
      timestampMilliseconds(updatedAt) >= timestampMilliseconds(createdAt),
    code,
  );
  return deepFreeze({
    reservationId: canonicalOpaqueId(
      reservation.reservationId,
      128,
      code,
    ),
    operationId: canonicalOpaqueId(reservation.operationId, 128, code),
    sessionId: canonicalSessionId(reservation.sessionId, code),
    kind: canonicalOpaqueId(reservation.kind, 64, code),
    expectedSessionRevision: canonicalRevisionForCode(
      reservation.expectedSessionRevision,
      code,
    ),
    state: "released",
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    requestSha256: reservation.requestSha256,
    createdAt,
    updatedAt,
    expiresAt: null,
    releasedAt,
  });
}

/**
 * Validates one committed writer-launch stop transition, including the exact
 * typed request/result relation and released reservation linkage.
 */
export function assertCommittedWriterLaunchStopTransitionProof(...args) {
  const code = "operation_state_invalid";
  ensure(args.length === 1, code);
  const proof = exactPlainObject(
    args[0],
    COMMITTED_WRITER_LAUNCH_STOP_PROOF_KEYS,
    code,
  );
  const before = expectedSnapshotFromValue(proof.before, code);
  const after = expectedSnapshotFromValue(proof.after, code);
  const normalizedOperation = canonicalCommittedWriterLaunchStopOperation(
    proof.operation,
    code,
  );
  const operation = normalizedOperation.operation;
  const reservation = canonicalCommittedWriterLaunchStopReservation(
    proof.reservation,
    code,
  );
  ensure(
    canonicalSnapshotBytes(operation.expectedSession) ===
        canonicalSnapshotBytes(before) &&
      after.sessionId === before.sessionId &&
      after.document.documentVersion === SESSION_AUTHORITY_DOCUMENT_VERSION &&
      after.document.activeOperation === null &&
      BigIntConstructor(after.revision) ===
        BigIntConstructor(before.revision) +
          BigIntConstructor(operation.revision) +
          1n,
    code,
  );
  validateOperationReservation(
    operation,
    reservation,
    normalizedOperation.input,
  );
  validateLastOperationPointer(after, operation, reservation);
  return deepFreeze({
    after,
    before,
    operation,
    reservation,
  });
}

function activePointerFor(input, state, operationRevision) {
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: input.expectedSession.revision,
    kind: input.kind,
    operationId: input.operationId,
    operationRevision,
    requestSha256: input.requestSha256,
    reservationId: input.reservationId,
    state,
  });
}

function lastPointerFor(operation, reservation) {
  ensure(
    operation.state === "committed" &&
      operation.result !== null &&
      reservation.state === "released",
    "operation_state_invalid",
  );
  return canonicalLastOperation(
    {
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      expectedSessionRevision: operation.expectedSession.revision,
      kind: operation.kind,
      operationId: operation.operationId,
      operationRevision: operation.revision,
      requestSha256: operation.requestSha256,
      reservationId: reservation.reservationId,
      resultSha256: sha256(canonicalSerialize(operation.result)),
      state: operation.state,
    },
    "operation_state_invalid",
  );
}

function documentWithAuthorityState(
  document,
  {
    activeOperation = document.activeOperation,
    attachment = document.attachment,
    lastOperation = documentLastOperation(document),
    launch = document.launch,
    lease = document.lease,
    lifecycle = document.lifecycle,
    writerEpoch = document.writerEpoch,
  },
) {
  return deepFreeze({
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: document.manifest,
    storageRef: document.storageRef,
    backendCapabilities: document.backendCapabilities,
    lifecycle,
    writerEpoch,
    lease,
    attachment,
    activeOperation,
    lastOperation,
    recovery: document.recovery,
    launch,
  });
}

function documentWithActiveOperation(
  document,
  activeOperation,
  lastOperation = documentLastOperation(document),
) {
  return documentWithAuthorityState(document, {
    activeOperation,
    lastOperation,
  });
}

function operationReceipt({
  operation,
  reservation,
  session,
  status = operation?.state ?? "absent",
  ...flags
}) {
  return deepFreeze({
    status,
    session,
    operation,
    reservation,
    ...flags,
  });
}

function restoreTerminalRevisionForLaunchHandoff(input, operation, code) {
  return revisionAfter(
    input.expectedSession.revision,
    BigIntConstructor(operation.revision) + 1n,
    code,
  );
}

function restoreTerminalSessionForLaunchHandoff(
  input,
  operation,
  reservation,
) {
  ensure(
    operation.state === "committed" &&
      operation.result?.outcome === "restore-generation-committed",
    "operation_state_invalid",
  );
  const session = expectedSnapshotFromValue(
    {
      createdAt: input.expectedSession.createdAt,
      document: documentWithAuthorityState(
        input.expectedSession.document,
        {
          activeOperation: null,
          lastOperation: lastPointerFor(operation, reservation),
        },
      ),
      revision: restoreTerminalRevisionForLaunchHandoff(
        input,
        operation,
        "operation_state_invalid",
      ),
      sessionId: input.expectedSession.sessionId,
      updatedAt: operation.updatedAt,
    },
    "operation_state_invalid",
  );
  validateLastOperationPointer(session, operation, reservation);
  return session;
}

function writerLaunchAttemptInputForRestoreHandoff(
  restoreInput,
  generation,
  expectedSession,
  launch,
) {
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession,
    generation,
    measuredImage: launch.measuredImage,
    supervisor: launch.supervisor,
  });
  const input = writerLaunchAttemptInput({
    expectedSession,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: launch.launchAttemptId,
    request,
  });
  ensure(
    restoreInput.request.contractVersion ===
        RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2 &&
      canonicalSerialize(launch) ===
        canonicalSerialize(restoreInput.request.launchIntent),
    "operation_state_invalid",
  );
  return input;
}

function restoreAttachmentActivationTerminalSessionForLaunchHandoff(
  input,
  operation,
  reservation,
) {
  ensure(
    operation.state === "committed" &&
      operation.result?.outcome === "restore-attachment-activated",
    "operation_state_invalid",
  );
  const session = expectedSnapshotFromValue(
    {
      createdAt: input.expectedSession.createdAt,
      document: documentWithAuthorityState(
        input.expectedSession.document,
        {
          activeOperation: null,
          attachment: operation.result.activationResult.attachment,
          lastOperation: lastPointerFor(operation, reservation),
          lease: operation.result.activationRequest.lease,
          lifecycle: "ATTACHED",
          writerEpoch:
            operation.result.activationRequest.lease.fencingEpoch,
        },
      ),
      revision: revisionAfter(
        input.expectedSession.revision,
        BigIntConstructor(operation.revision) + 1n,
        "operation_state_invalid",
      ),
      sessionId: input.expectedSession.sessionId,
      updatedAt: operation.updatedAt,
    },
    "operation_state_invalid",
  );
  validateLastOperationPointer(session, operation, reservation);
  return session;
}

function writerLaunchAttemptInputForRestoreActivationHandoff(
  activationInput,
  generation,
  expectedSession,
) {
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession,
    generation,
    measuredImage: activationInput.request.launchIntent.measuredImage,
    supervisor: activationInput.request.launchIntent.supervisor,
  });
  return writerLaunchAttemptInput({
    expectedSession,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: activationInput.request.launchIntent.launchAttemptId,
    request,
  });
}

function restoreAttachmentActivationHandoffReceipt({
  activationFinalized,
  activationInput,
  activationOperation,
  activationReservation,
  generation,
  launchInput,
  launchOperation,
  launchReservation,
  session,
}) {
  ensure(
    launchOperation.createdAt === activationOperation.updatedAt &&
      launchReservation.createdAt === activationOperation.updatedAt,
    "operation_state_invalid",
  );
  validateOperationIdentity(activationOperation, activationInput);
  validateOperationReservation(
    activationOperation,
    activationReservation,
    activationInput,
  );
  validateOperationIdentity(launchOperation, launchInput);
  validateOperationReservation(
    launchOperation,
    launchReservation,
    launchInput,
  );
  validateLastOperationPointer(
    launchInput.expectedSession,
    activationOperation,
    activationReservation,
  );
  if (launchOperation.state === "committed") {
    validateCommittedOperationHistory(launchOperation, session);
  } else {
    validateActivePointer(session, launchOperation, launchReservation);
  }
  return deepFreeze({
    activation: deepFreeze({
      finalized: activationFinalized,
      operation: activationOperation,
      reservation: activationReservation,
    }),
    generation,
    launch: deepFreeze({
      attempt: writerLaunchAttemptRecord(launchInput, launchOperation),
      operation: launchOperation,
      reservation: launchReservation,
    }),
    session,
    status: launchOperation.state,
  });
}

function restoreLaunchHandoffReceipt({
  generation,
  launchInput,
  launchOperation,
  launchReservation,
  restoreCatalogue,
  restoreFinalized,
  restoreInput,
  restoreOperation,
  restoreReservation,
  session,
}) {
  ensure(
    generation.state === "committed" &&
      generation.document !== null &&
      generation.committedAt === restoreOperation.updatedAt &&
      restoreOperation.result?.generationId === generation.generationId &&
      restoreOperation.result?.checkpointId === generation.checkpointId &&
      restoreOperation.result?.catalogueSha256 ===
        sha256(canonicalSerialize(restoreCatalogue.document)) &&
      restoreOperation.result?.generationDocumentSha256 ===
        sha256(canonicalSerialize(generation.document)) &&
      launchOperation.createdAt === restoreOperation.updatedAt &&
      launchReservation.createdAt === restoreOperation.updatedAt,
    "operation_state_invalid",
  );
  validateOperationIdentity(restoreOperation, restoreInput);
  validateOperationReservation(
    restoreOperation,
    restoreReservation,
    restoreInput,
  );
  validateOperationIdentity(launchOperation, launchInput);
  validateOperationReservation(
    launchOperation,
    launchReservation,
    launchInput,
  );
  validateLastOperationPointer(
    launchInput.expectedSession,
    restoreOperation,
    restoreReservation,
  );
  if (launchOperation.state === "committed") {
    validateCommittedOperationHistory(launchOperation, session);
  } else {
    validateActivePointer(session, launchOperation, launchReservation);
  }
  return deepFreeze({
    generation,
    launch: deepFreeze({
      attempt: writerLaunchAttemptRecord(launchInput, launchOperation),
      operation: launchOperation,
      reservation: launchReservation,
    }),
    restore: deepFreeze({
      catalogue: restoreCatalogue,
      finalized: restoreFinalized,
      operation: restoreOperation,
      reservation: restoreReservation,
    }),
    session,
    status: launchOperation.state,
  });
}

function isAtomicRestoreLaunchHandoff(session, observed) {
  const active = observed.active;
  if (
    active === null ||
    active.operation.kind !== WRITER_LAUNCH_ATTEMPT_OPERATION_KIND
  ) {
    return false;
  }
  const terminal = observed.terminal;
  const restoreRelation = terminal?.generation;
  if (
    restoreRelation?.input.request.contractVersion !==
    RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2
  ) {
    return false;
  }
  ensure(
    active.operation.operationId === observed.operation?.operationId &&
      active.reservation.reservationId ===
        observed.reservation?.reservationId &&
      active.launchAttempt !== null &&
      terminal !== null &&
      terminal.operation.state === "committed" &&
      restoreRelation.generation !== null &&
      restoreRelation.source !== null,
    "operation_state_invalid",
  );

  const restoreTerminalSession = restoreTerminalSessionForLaunchHandoff(
    restoreRelation.input,
    terminal.operation,
    terminal.reservation,
  );
  const launchInput = writerLaunchAttemptInputForRestoreHandoff(
    restoreRelation.input,
    restoreRelation.generation,
    restoreTerminalSession,
    restoreRelation.input.request.launchIntent,
  );
  const launchIdClaim = restoreRelation.launchIdClaim;
  ensure(launchIdClaim !== null, "operation_state_invalid");
  ensure(
    canonicalSerialize(active.launchAttempt.input) ===
        canonicalSerialize(launchInput) &&
      active.operation.createdAt === terminal.operation.updatedAt &&
      active.reservation.createdAt === terminal.operation.updatedAt &&
      (active.operation.state !== "prepared" ||
        (active.operation.updatedAt === terminal.operation.updatedAt &&
          active.reservation.updatedAt === terminal.operation.updatedAt)) &&
      launchIdClaim.claimedAt === restoreRelation.generation.claimedAt &&
      launchIdClaim.materializedAt === active.operation.createdAt &&
      launchIdClaim.materializedAt === terminal.operation.updatedAt,
    "operation_state_invalid",
  );

  // Standalone restore finalization accepts only V1, so a committed V2
  // terminal relation identifies the atomic producer. The reconstructed input
  // binds its launch intent, committed generation, measured image, and
  // supervisor; the shared durable timestamps corroborate that producer but
  // are not the provenance root. Receipt validation additionally proves both
  // durable identities and the active/terminal session relation.
  restoreLaunchHandoffReceipt({
    generation: restoreRelation.generation,
    launchInput,
    launchOperation: active.operation,
    launchReservation: active.reservation,
    restoreCatalogue: restoreRelation.source.catalogue,
    restoreFinalized: false,
    restoreInput: restoreRelation.input,
    restoreOperation: terminal.operation,
    restoreReservation: terminal.reservation,
    session,
  });
  return true;
}

function isAtomicRestoreAttachmentActivationLaunchHandoff(
  session,
  observed,
) {
  const active = observed.active;
  if (
    active === null ||
    active.operation.kind !== WRITER_LAUNCH_ATTEMPT_OPERATION_KIND
  ) {
    return false;
  }
  const terminal = observed.terminal;
  const activation = terminal?.activation;
  if (
    activation === null ||
    activation === undefined ||
    terminal.operation.kind !== RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND
  ) {
    return false;
  }
  ensure(
    activation.generation !== null &&
      activation.launchIdClaim !== null &&
      terminal.operation.state === "committed" &&
      terminal.operation.result?.outcome === "restore-attachment-activated" &&
      active.launchAttempt !== null,
    "operation_state_invalid",
  );
  const terminalSession =
    restoreAttachmentActivationTerminalSessionForLaunchHandoff(
      activation.input,
      terminal.operation,
      terminal.reservation,
    );
  const launchInput = writerLaunchAttemptInputForRestoreActivationHandoff(
    activation.input,
    activation.generation.generation,
    terminalSession,
  );
  ensure(
    canonicalSerialize(active.launchAttempt.input) ===
        canonicalSerialize(launchInput) &&
      active.operation.createdAt === terminal.operation.updatedAt &&
      active.reservation.createdAt === terminal.operation.updatedAt &&
      (active.operation.state !== "prepared" ||
        (active.operation.updatedAt === terminal.operation.updatedAt &&
          active.reservation.updatedAt === terminal.operation.updatedAt)) &&
      activation.launchIdClaim.materializedAt ===
        active.operation.createdAt &&
      activation.launchIdClaim.materializedAt ===
        terminal.operation.updatedAt,
    "operation_state_invalid",
  );
  restoreAttachmentActivationHandoffReceipt({
    activationFinalized: false,
    activationInput: activation.input,
    activationOperation: terminal.operation,
    activationReservation: terminal.reservation,
    generation: activation.generation.generation,
    launchInput,
    launchOperation: active.operation,
    launchReservation: active.reservation,
    session,
  });
  return true;
}

async function readRestoreLaunchHandoffReplay(
  transaction,
  session,
  input,
  observed,
) {
  const restoreOperation = observed.operation;
  const restoreReservation = observed.reservation;
  const relation = observed.generation;
  ensure(
    restoreOperation !== null &&
      restoreReservation !== null &&
      relation?.generation !== null &&
      relation?.generation !== undefined &&
      relation.source !== null &&
      restoreOperation.state === "committed" &&
      restoreOperation.result?.outcome === "restore-generation-committed" &&
      relation.generation.state === "committed",
    "operation_transition_conflict",
  );
  const restoreTerminalSession = restoreTerminalSessionForLaunchHandoff(
    input.restore,
    restoreOperation,
    restoreReservation,
  );
  const launchInput = writerLaunchAttemptInputForRestoreHandoff(
    input.restore,
    relation.generation,
    restoreTerminalSession,
    input.launch,
  );
  const launchIdClaim = relation.launchIdClaim;
  ensure(launchIdClaim !== null, "operation_state_invalid");
  const launchObserved = await readRequestedOperation(
    transaction,
    session,
    launchInput,
    true,
  );
  const cancelledAfterLeaseExpiry =
    launchObserved.operation?.state === "committed" &&
    launchObserved.operation.result?.outcome ===
      "cancelled-before-dispatch";
  ensure(
    launchObserved.operation !== null &&
      launchObserved.reservation !== null &&
      launchObserved.launchAttempt !== null,
    "operation_transition_conflict",
  );
  if (cancelledAfterLeaseExpiry) {
    ensure(
      launchObserved.operation.result.reason ===
          WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON &&
        timestampMilliseconds(launchObserved.operation.updatedAt) >=
          timestampMilliseconds(launchInput.request.lease.expiresAt),
      "operation_transition_conflict",
    );
  }
  ensure(
    launchIdClaim.claimedAt === relation.generation.claimedAt &&
      launchIdClaim.materializedAt === launchObserved.operation.createdAt &&
      launchIdClaim.materializedAt === restoreOperation.updatedAt,
    "operation_state_invalid",
  );
  return restoreLaunchHandoffReceipt({
    generation: relation.generation,
    launchInput,
    launchOperation: launchObserved.operation,
    launchReservation: launchObserved.reservation,
    restoreCatalogue: relation.source.catalogue,
    restoreFinalized: false,
    restoreInput: input.restore,
    restoreOperation,
    restoreReservation,
    session,
  });
}

async function readRestoreAttachmentActivationHandoffReplay(
  transaction,
  session,
  input,
  observed,
) {
  const operation = observed.operation;
  const reservation = observed.reservation;
  const relation = observed.activation;
  ensure(
    operation !== null &&
      reservation !== null &&
      relation !== null &&
      relation.generation !== null &&
      relation.launchIdClaim !== null &&
      operation.state === "committed" &&
      operation.result?.outcome === "restore-attachment-activated",
    "operation_transition_conflict",
  );
  const terminalSession =
    restoreAttachmentActivationTerminalSessionForLaunchHandoff(
      input,
      operation,
      reservation,
    );
  const launchInput = writerLaunchAttemptInputForRestoreActivationHandoff(
    input,
    relation.generation.generation,
    terminalSession,
  );
  const launchObserved = await readRequestedOperation(
    transaction,
    session,
    launchInput,
    true,
  );
  ensure(
    launchObserved.operation !== null &&
      launchObserved.reservation !== null &&
      launchObserved.launchAttempt !== null &&
      relation.launchIdClaim.materializedAt ===
        launchObserved.operation.createdAt &&
      relation.launchIdClaim.materializedAt === operation.updatedAt,
    "operation_transition_conflict",
  );
  return restoreAttachmentActivationHandoffReceipt({
    activationFinalized: false,
    activationInput: input,
    activationOperation: operation,
    activationReservation: reservation,
    generation: relation.generation.generation,
    launchInput,
    launchOperation: launchObserved.operation,
    launchReservation: launchObserved.reservation,
    session,
  });
}

async function readSessionSnapshot(transaction, sessionId, forUpdate) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_SESSION_FOR_UPDATE_QUERY.text
        : READ_SESSION_QUERY.text,
      [sessionId],
    ),
  );
  ensure(rows.length === 1, "session_not_found");
  return snapshotFromRow(rows[0], sessionId);
}

async function readAuthorityClock(transaction) {
  const rows = rowsFromResult(
    await transaction.query(READ_AUTHORITY_CLOCK_QUERY.text),
    "session_state_invalid",
  );
  ensure(rows.length === 1, "session_state_invalid");
  const row = exactPlainObject(
    rows[0],
    ["authority_now"],
    "session_state_invalid",
  );
  return canonicalTimestampForCode(
    row.authority_now,
    "session_state_invalid",
  );
}

async function readOperationSnapshot(transaction, operationId, forUpdate) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_OPERATION_FOR_UPDATE_QUERY.text
        : READ_OPERATION_QUERY.text,
      [operationId],
    ),
    "operation_state_invalid",
  );
  return rows.length === 0 ? null : operationSnapshotFromRow(rows[0]);
}

async function readOperationIdClaim(transaction, operationId, forUpdate) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY.text
        : READ_OPERATION_ID_CLAIM_QUERY.text,
      [operationId],
    ),
    "operation_state_invalid",
  );
  return rows.length === 0
    ? null
    : operationIdClaimSnapshotFromRow(rows[0]);
}

async function readReservationSnapshot(
  transaction,
  operationId,
  forUpdate,
) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_RESERVATION_BY_OPERATION_FOR_UPDATE_QUERY.text
        : READ_RESERVATION_BY_OPERATION_QUERY.text,
      [operationId],
    ),
    "operation_state_invalid",
  );
  return rows.length === 0 ? null : reservationSnapshotFromRow(rows[0]);
}

async function readCaptureAttemptSnapshot(transaction, input, forUpdate) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_CAPTURE_ATTEMPT_BY_OPERATION_FOR_UPDATE_QUERY.text
        : READ_CAPTURE_ATTEMPT_BY_OPERATION_QUERY.text,
      [input.operationId],
    ),
    "operation_state_invalid",
  );
  return rows.length === 0
    ? null
    : captureAttemptSnapshotFromRow(rows[0], input);
}

async function readCaptureAttemptTombstone(
  transaction,
  input,
  forUpdate,
) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_CAPTURE_ATTEMPT_TOMBSTONE_FOR_UPDATE_QUERY.text
        : READ_CAPTURE_ATTEMPT_TOMBSTONE_QUERY.text,
      [input.operationId],
    ),
    "operation_state_invalid",
  );
  return rows.length === 0
    ? null
    : captureAttemptTombstoneSnapshotFromRow(rows[0], input);
}

async function readCheckpointCatalogueByAttempt(transaction, input) {
  const rows = rowsFromResult(
    await transaction.query(
      READ_CHECKPOINT_CATALOGUE_BY_ATTEMPT_QUERY.text,
      [input.request.admission.captureAttemptId],
    ),
    "operation_state_invalid",
  );
  return rows.length === 0
    ? null
    : checkpointCatalogueSnapshotFromRow(rows[0], input);
}

async function validateCheckpointCaptureRelations(
  transaction,
  operation,
  forUpdate,
) {
  if (operation.kind !== CHECKPOINT_CAPTURE_OPERATION_KIND) return null;
  const input = checkpointCaptureInput(inputForOperation(operation));
  const attempt = await readCaptureAttemptSnapshot(
    transaction,
    input,
    forUpdate,
  );
  const tombstone = await readCaptureAttemptTombstone(
    transaction,
    input,
    forUpdate,
  );
  const catalogue =
    attempt === null
      ? null
      : await readCheckpointCatalogueByAttempt(transaction, input);
  if (operation.state === "prepared") {
    ensure(
      attempt === null && tombstone === null && catalogue === null,
      "operation_state_invalid",
    );
    return deepFreeze({ attempt, catalogue, input });
  }
  if (
    operation.state === "committed" &&
    operation.result?.outcome === "cancelled-before-dispatch"
  ) {
    ensure(
      attempt === null && tombstone === null && catalogue === null,
      "operation_state_invalid",
    );
    return deepFreeze({ attempt, catalogue, input });
  }
  if (tombstone !== null) {
    fail("checkpoint_capture_not_authorized");
  }
  ensure(
    attempt !== null &&
      timestampMilliseconds(attempt.claimedAt) >=
        timestampMilliseconds(operation.createdAt) &&
      timestampMilliseconds(attempt.claimedAt) <=
        timestampMilliseconds(operation.updatedAt) &&
      (operation.state !== "starting" ||
        attempt.claimedAt === operation.updatedAt),
    "operation_state_invalid",
  );
  if (operation.state === "starting" || operation.state === "uncertain") {
    ensure(catalogue === null, "operation_state_invalid");
  } else {
    ensure(
      operation.state === "committed" &&
        catalogue !== null &&
        operation.result?.outcome === "checkpoint-captured" &&
        operation.result.catalogueSha256 ===
          sha256(canonicalSerialize(catalogue.document)) &&
        catalogue.committedAt === operation.updatedAt,
      "operation_state_invalid",
    );
  }
  return deepFreeze({ attempt, catalogue, input });
}

async function readRestoreGenerationRow(
  transaction,
  operationId,
  forUpdate,
) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_RESTORE_GENERATION_BY_OPERATION_FOR_UPDATE_QUERY.text
        : READ_RESTORE_GENERATION_BY_OPERATION_QUERY.text,
      [operationId],
    ),
    "operation_state_invalid",
  );
  ensure(rows.length <= 1, "operation_state_invalid");
  return rows.length === 0 ? null : rows[0];
}

async function readCommittedCheckpointSource(
  transaction,
  checkpoint,
  currentSession,
  forUpdate,
) {
  const catalogueRows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_CHECKPOINT_CATALOGUE_BY_ID_FOR_UPDATE_QUERY.text
        : READ_CHECKPOINT_CATALOGUE_BY_ID_QUERY.text,
      [checkpoint.checkpointId],
    ),
    "operation_state_invalid",
  );
  ensure(catalogueRows.length === 1, "checkpoint_catalogue_not_found");
  const catalogueIdentity = checkpointCatalogueIdentityFromRow(
    catalogueRows[0],
  );
  ensure(
    catalogueIdentity.checkpointId === checkpoint.checkpointId &&
      catalogueIdentity.sessionId === currentSession.sessionId,
    "operation_state_invalid",
  );

  const attemptRows = rowsFromResult(
    await transaction.query(READ_CAPTURE_ATTEMPT_BY_ID_QUERY.text, [
      catalogueIdentity.captureAttemptId,
    ]),
    "operation_state_invalid",
  );
  ensure(attemptRows.length === 1, "operation_state_invalid");
  const attemptIdentity = captureAttemptIdentityFromRow(attemptRows[0]);
  ensure(
    attemptIdentity.captureAttemptId ===
        catalogueIdentity.captureAttemptId &&
      attemptIdentity.sessionId === catalogueIdentity.sessionId,
    "operation_state_invalid",
  );

  const operation = await readOperationSnapshot(
    transaction,
    attemptIdentity.operationId,
    false,
  );
  ensure(
    operation !== null &&
      operation.kind === CHECKPOINT_CAPTURE_OPERATION_KIND &&
      operation.state === "committed" &&
      operation.result?.outcome === "checkpoint-captured",
    "operation_state_invalid",
  );
  const input = checkpointCaptureInput(
    inputForOperation(operation),
    OPERATION_INPUT_KEYS,
    "operation_state_invalid",
  );
  const sourceCheckpoint = input.request.admission.checkpoint;
  const sourceStorageRef = operation.expectedSession.document.storageRef;
  ensure(
    canonicalSerialize(sourceCheckpoint) ===
        canonicalSerialize(checkpoint) &&
      sourceStorageRef.sessionId === sourceCheckpoint.sessionId &&
      sourceStorageRef.backendId === sourceCheckpoint.backendId &&
      sourceStorageRef.storageId === sourceCheckpoint.storageId &&
      canonicalRestoreIdentityBytes(operation.expectedSession.document) ===
        canonicalRestoreIdentityBytes(currentSession.document) &&
      operation.expectedSession.createdAt === currentSession.createdAt &&
      BigIntConstructor(currentSession.revision) >=
        BigIntConstructor(
          revisionAfter(
            operation.expectedSession.revision,
            BigIntConstructor(operation.revision) + 1n,
            "operation_state_invalid",
          ),
        ),
    "operation_state_invalid",
  );
  const reservation = await readReservationSnapshot(
    transaction,
    operation.operationId,
    false,
  );
  ensure(reservation !== null, "operation_state_invalid");
  validateOperationReservation(operation, reservation, input);
  const relation = await validateCheckpointCaptureRelations(
    transaction,
    operation,
    false,
  );
  ensure(
    relation !== null &&
      relation.attempt !== null &&
      relation.catalogue !== null &&
      canonicalSerialize(relation.catalogue) ===
        canonicalSerialize(
          checkpointCatalogueSnapshotFromRow(catalogueRows[0], input),
        ),
    "operation_state_invalid",
  );
  return deepFreeze({
    attempt: relation.attempt,
    catalogue: relation.catalogue,
    operation,
    reservation,
  });
}

async function readCommittedRestoreGenerationForLaunch(
  transaction,
  reference,
  currentSession,
  forUpdate,
) {
  const generationRows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_RESTORE_GENERATION_BY_ID_FOR_UPDATE_QUERY.text
        : READ_RESTORE_GENERATION_BY_ID_QUERY.text,
      [reference.generationId],
    ),
    "operation_state_invalid",
  );
  ensure(generationRows.length === 1, "operation_state_invalid");
  const identity = restoreGenerationIdentityFromRow(generationRows[0]);
  ensure(
    identity.generationId === reference.generationId &&
      identity.operationId === reference.operationId &&
      identity.sessionId === reference.sessionId &&
      identity.checkpointId === reference.checkpointId,
    "operation_state_invalid",
  );
  const operation = await readOperationSnapshot(
    transaction,
    identity.operationId,
    false,
  );
  ensure(
    operation !== null &&
      operation.kind ===
        RESTORE_DESTINATION_GENERATION_OPERATION_KIND &&
      operation.state === "committed" &&
      operation.result?.outcome === "restore-generation-committed" &&
      operation.result.generationId === identity.generationId &&
      operation.result.checkpointId === identity.checkpointId,
    "operation_state_invalid",
  );
  validateCommittedOperationHistory(operation, currentSession);
  const input = restoreGenerationInput(
    inputForOperation(operation),
    OPERATION_INPUT_KEYS,
    "operation_state_invalid",
  );
  const source = await readCommittedCheckpointSource(
    transaction,
    input.request.admission.checkpoint,
    currentSession,
    forUpdate,
  );
  const generation = restoreGenerationSnapshotFromRow(
    generationRows[0],
    input,
    source,
  );
  const reservation = await readReservationSnapshot(
    transaction,
    operation.operationId,
    false,
  );
  ensure(reservation !== null, "operation_state_invalid");
  validateOperationReservation(operation, reservation, input);
  ensure(
    generation.state === "committed" &&
      generation.document !== null &&
      generation.committedAt !== null &&
      generation.committedAt === operation.updatedAt &&
      timestampMilliseconds(generation.claimedAt) >=
        timestampMilliseconds(operation.createdAt) &&
      timestampMilliseconds(generation.claimedAt) <=
        timestampMilliseconds(operation.updatedAt),
    "operation_state_invalid",
  );
  ensure(
    operation.result.catalogueSha256 ===
        sha256(canonicalSerialize(source.catalogue.document)) &&
      operation.result.generationDocumentSha256 ===
        sha256(canonicalSerialize(generation.document)),
    "operation_state_invalid",
  );
  const observedReference = writerLaunchGenerationReference(
    generation,
    currentSession,
    "operation_state_invalid",
  );
  ensure(
    observedReference.bindingSha256 === reference.bindingSha256,
    "operation_state_invalid",
  );
  ensure(
    observedReference.checkpointId === reference.checkpointId,
    "operation_state_invalid",
  );
  ensure(
    observedReference.claimedAt === reference.claimedAt &&
      observedReference.committedAt === reference.committedAt,
    "operation_state_invalid",
  );
  ensure(
    observedReference.documentSha256 === reference.documentSha256,
    "operation_state_invalid",
  );
  ensure(
    observedReference.generationId === reference.generationId &&
      observedReference.operationId === reference.operationId &&
      observedReference.sessionId === reference.sessionId &&
      observedReference.state === reference.state,
    "operation_state_invalid",
  );
  return deepFreeze({ generation, input, operation, reservation, source });
}

async function readRestoreAttachmentActivationPredecessorV1(
  transaction,
  input,
  currentSession,
  forUpdate,
) {
  const predecessor = input.request.predecessor;
  const detachOperation = await readOperationSnapshot(
    transaction,
    predecessor.detachOperationId,
    forUpdate,
  );
  ensure(
    detachOperation !== null &&
      detachOperation.state === "committed" &&
      (detachOperation.kind === WRITER_RELEASE_OPERATION_KIND ||
        detachOperation.kind === WRITER_FORCE_FENCE_OPERATION_KIND) &&
      detachOperation.result !== null &&
      (detachOperation.result.outcome === "writer-released" ||
        detachOperation.result.outcome === "writer-fenced"),
    "operation_state_invalid",
  );
  const detachReservation = await readReservationSnapshot(
    transaction,
    detachOperation.operationId,
    forUpdate,
  );
  ensure(detachReservation !== null, "operation_state_invalid");
  validateLastOperationPointer(
    input.expectedSession,
    detachOperation,
    detachReservation,
  );
  validateCommittedOperationHistory(detachOperation, currentSession);
  const stopPointer = detachOperation.expectedSession.document.lastOperation;
  ensure(
    stopPointer !== null &&
      stopPointer.kind === WRITER_LAUNCH_STOP_OPERATION_KIND &&
      stopPointer.operationId === predecessor.stopOperationId &&
      detachOperation.expectedSession.document.lifecycle === "ATTACHED" &&
      detachOperation.expectedSession.document.launch === null &&
      detachOperation.expectedSession.document.attachment !== null &&
      detachOperation.expectedSession.document.attachment.attachmentId ===
        predecessor.attachmentId,
    "operation_state_invalid",
  );
  const stopOperation = await readOperationSnapshot(
    transaction,
    predecessor.stopOperationId,
    forUpdate,
  );
  ensure(
    stopOperation !== null &&
      stopOperation.kind === WRITER_LAUNCH_STOP_OPERATION_KIND &&
      stopOperation.state === "committed" &&
      stopOperation.result?.outcome === "writer-launch-stopped",
    "operation_state_invalid",
  );
  const stopReservation = await readReservationSnapshot(
    transaction,
    stopOperation.operationId,
    forUpdate,
  );
  ensure(stopReservation !== null, "operation_state_invalid");
  validateLastOperationPointer(
    detachOperation.expectedSession,
    stopOperation,
    stopReservation,
  );
  const stopRelation = await validateWriterLaunchStopRelations(
    transaction,
    stopOperation,
    detachOperation.expectedSession,
    forUpdate,
  );
  ensure(
    stopRelation !== null &&
      stopRelation.input.request.launch.attachmentId ===
        predecessor.attachmentId &&
      canonicalSerialize(stopRelation.input.request.launch.generation) ===
        canonicalSerialize(input.request.generation),
    "operation_state_invalid",
  );
  return deepFreeze({
    detachOperation,
    detachReservation,
    stopOperation,
    stopRelation,
    stopReservation,
  });
}

async function readRestoreAttachmentActivationPredecessorV2(
  transaction,
  input,
  currentSession,
  forUpdate,
) {
  const predecessor = input.request.predecessor;
  const detachOperation = await readOperationSnapshot(
    transaction,
    predecessor.detachOperationId,
    forUpdate,
  );
  ensure(
    detachOperation !== null &&
      detachOperation.state === "committed" &&
      (detachOperation.kind === WRITER_RELEASE_OPERATION_KIND ||
        detachOperation.kind === WRITER_FORCE_FENCE_OPERATION_KIND) &&
      detachOperation.result !== null &&
      (detachOperation.result.outcome === "writer-released" ||
        detachOperation.result.outcome === "writer-fenced"),
    "operation_state_invalid",
  );
  const detachReservation = await readReservationSnapshot(
    transaction,
    detachOperation.operationId,
    forUpdate,
  );
  ensure(detachReservation !== null, "operation_state_invalid");
  validateLastOperationPointer(
    input.expectedSession,
    detachOperation,
    detachReservation,
  );
  validateCommittedOperationHistory(detachOperation, currentSession);

  const capturePointer =
    detachOperation.expectedSession.document.lastOperation;
  ensure(
    capturePointer !== null &&
      capturePointer.kind === CHECKPOINT_CAPTURE_OPERATION_KIND &&
      capturePointer.operationId === predecessor.captureOperationId &&
      detachOperation.expectedSession.document.lifecycle === "ATTACHED" &&
      detachOperation.expectedSession.document.launch === null &&
      detachOperation.expectedSession.document.attachment !== null &&
      detachOperation.expectedSession.document.attachment.attachmentId ===
        predecessor.attachmentId,
    "operation_state_invalid",
  );
  const captureOperation = await readOperationSnapshot(
    transaction,
    predecessor.captureOperationId,
    forUpdate,
  );
  ensure(
    captureOperation !== null &&
      captureOperation.kind === CHECKPOINT_CAPTURE_OPERATION_KIND &&
      captureOperation.state === "committed" &&
      captureOperation.result?.outcome === "checkpoint-captured",
    "operation_state_invalid",
  );
  const captureReservation = await readReservationSnapshot(
    transaction,
    captureOperation.operationId,
    forUpdate,
  );
  ensure(captureReservation !== null, "operation_state_invalid");
  validateLastOperationPointer(
    detachOperation.expectedSession,
    captureOperation,
    captureReservation,
  );
  validateCommittedOperationHistory(captureOperation, currentSession);
  const captureRelation = await validateCheckpointCaptureRelations(
    transaction,
    captureOperation,
    forUpdate,
  );
  ensure(
    captureRelation !== null &&
      captureRelation.attempt !== null &&
      captureRelation.catalogue !== null,
    "operation_state_invalid",
  );
  const captureAdmission = captureRelation.input.request.admission;
  ensure(
    captureAdmission.checkpoint.checkpointClass === "clean" &&
      captureAdmission.stopOperationId === predecessor.stopOperationId &&
      captureAdmission.attachment.attachmentId ===
        predecessor.attachmentId &&
      canonicalSerialize(
        canonicalJsonObject(
          captureAdmission.attachment,
          "operation_state_invalid",
        ),
      ) ===
        canonicalSerialize(
          canonicalJsonObject(
            detachOperation.expectedSession.document.attachment,
            "operation_state_invalid",
          ),
        ),
    "operation_state_invalid",
  );
  ensure(
    captureRelation.attempt.operationId ===
        predecessor.captureOperationId &&
      captureRelation.attempt.captureAttemptId ===
        captureAdmission.captureAttemptId &&
      captureRelation.attempt.binding.attachmentId ===
        predecessor.attachmentId &&
      captureRelation.attempt.binding.captureAttemptId ===
        captureAdmission.captureAttemptId &&
      captureRelation.attempt.binding.stopOperationId ===
        predecessor.stopOperationId &&
      captureRelation.attempt.binding.processIncarnationId ===
        captureAdmission.processIncarnationId &&
      captureRelation.attempt.binding.writerIncarnationId ===
        captureAdmission.writerIncarnationId,
    "operation_state_invalid",
  );
  ensure(
    captureRelation.catalogue.captureAttemptId ===
        captureAdmission.captureAttemptId &&
      captureRelation.catalogue.checkpointId ===
        captureAdmission.checkpoint.checkpointId &&
      captureOperation.result.captureAttemptId ===
        captureAdmission.captureAttemptId &&
      captureOperation.result.checkpointId ===
        captureAdmission.checkpoint.checkpointId &&
      captureOperation.result.catalogueSha256 ===
        sha256(canonicalSerialize(captureRelation.catalogue.document)),
    "operation_state_invalid",
  );

  const stopPointer = captureOperation.expectedSession.document.lastOperation;
  ensure(
    stopPointer !== null &&
      stopPointer.kind === WRITER_LAUNCH_STOP_OPERATION_KIND &&
      stopPointer.operationId === predecessor.stopOperationId &&
      captureOperation.expectedSession.document.lifecycle === "ATTACHED" &&
      captureOperation.expectedSession.document.launch === null &&
      captureOperation.expectedSession.document.attachment !== null &&
      captureOperation.expectedSession.document.attachment.attachmentId ===
        predecessor.attachmentId,
    "operation_state_invalid",
  );
  const stopOperation = await readOperationSnapshot(
    transaction,
    predecessor.stopOperationId,
    forUpdate,
  );
  ensure(
    stopOperation !== null &&
      stopOperation.kind === WRITER_LAUNCH_STOP_OPERATION_KIND &&
      stopOperation.state === "committed" &&
      stopOperation.result?.outcome === "writer-launch-stopped",
    "operation_state_invalid",
  );
  const stopReservation = await readReservationSnapshot(
    transaction,
    stopOperation.operationId,
    forUpdate,
  );
  ensure(stopReservation !== null, "operation_state_invalid");
  validateLastOperationPointer(
    captureOperation.expectedSession,
    stopOperation,
    stopReservation,
  );
  const stopRelation = await validateWriterLaunchStopRelations(
    transaction,
    stopOperation,
    captureOperation.expectedSession,
    forUpdate,
  );
  ensure(
    stopRelation !== null &&
      stopRelation.input.request.launch.attachmentId ===
        predecessor.attachmentId &&
      captureAdmission.processIncarnationId ===
        stopRelation.input.request.launch.processIncarnationId &&
      captureAdmission.writerIncarnationId ===
        stopRelation.input.request.launch.writerIncarnationId,
    "operation_state_invalid",
  );
  return deepFreeze({
    captureOperation,
    captureRelation,
    captureReservation,
    detachOperation,
    detachReservation,
    stopOperation,
    stopRelation,
    stopReservation,
  });
}

async function readRestoreAttachmentActivationPredecessor(
  transaction,
  input,
  currentSession,
  forUpdate,
) {
  return input.request.contractVersion ===
    RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION
    ? readRestoreAttachmentActivationPredecessorV1(
        transaction,
        input,
        currentSession,
        forUpdate,
      )
    : readRestoreAttachmentActivationPredecessorV2(
        transaction,
        input,
        currentSession,
        forUpdate,
      );
}

async function readRestoreAttachmentActivationCoreRelations(
  transaction,
  input,
  currentSession,
  forUpdate,
) {
  const generation = await readCommittedRestoreGenerationForLaunch(
    transaction,
    input.request.generation,
    currentSession,
    forUpdate,
  );
  const predecessor = await readRestoreAttachmentActivationPredecessor(
    transaction,
    input,
    currentSession,
    forUpdate,
  );
  ensure(
    canonicalSerialize(
      canonicalJsonObject(
        generation.generation.binding.attachment,
        "operation_state_invalid",
      ),
    ) ===
      canonicalSerialize(
        canonicalJsonObject(
          predecessor.detachOperation.expectedSession.document.attachment,
          "operation_state_invalid",
        ),
      ) &&
      generation.generation.binding.attachment.attachmentId ===
        input.request.predecessor.attachmentId,
    "operation_state_invalid",
  );
  return deepFreeze({ generation, predecessor });
}

async function validateRestoreAttachmentActivationRelations(
  transaction,
  operation,
  currentSession,
  forUpdate,
) {
  if (operation.kind !== RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND) {
    return null;
  }
  const input = restoreAttachmentActivationInput(
    inputForOperation(operation),
    OPERATION_INPUT_KEYS,
    "operation_state_invalid",
  );
  if (
    operation.state === "committed" &&
    operation.result?.outcome === "cancelled-before-dispatch"
  ) {
    return deepFreeze({
      activationRequest: null,
      generation: null,
      input,
      launchIdClaim: null,
      predecessor: null,
    });
  }
  const relations = await readRestoreAttachmentActivationCoreRelations(
    transaction,
    input,
    currentSession,
    forUpdate,
  );
  const { generation, predecessor } = relations;
  if (operation.state === "prepared") {
    return deepFreeze({
      activationRequest: null,
      generation,
      input,
      launchIdClaim: null,
      predecessor,
    });
  }
  const launchIdClaim = await readOperationIdClaim(
    transaction,
    input.request.launchIntent.launchAttemptId,
    forUpdate,
  );
  ensure(launchIdClaim !== null, "operation_state_invalid");
  validateRestoreAttachmentActivationLaunchIdClaim(
    launchIdClaim,
    input,
    operation.state === "committed" ? "materialized" : "reserved",
  );
  ensure(
    timestampMilliseconds(launchIdClaim.claimedAt) >=
        timestampMilliseconds(operation.createdAt) &&
      timestampMilliseconds(launchIdClaim.claimedAt) <=
        timestampMilliseconds(operation.updatedAt) &&
      (operation.state !== "starting" ||
        launchIdClaim.claimedAt === operation.updatedAt) &&
      (operation.state !== "committed" ||
        launchIdClaim.materializedAt === operation.updatedAt),
    "operation_state_invalid",
  );
  let activationRequest;
  if (operation.state === "starting" || operation.state === "uncertain") {
    ensure(
      currentSession.document.lease !== null,
      "operation_state_invalid",
    );
    activationRequest = restoreAttachmentActivationRequestFor(
      input,
      generation.generation,
      currentSession.document.lease,
      "operation_state_invalid",
    );
  } else {
    ensure(
      operation.state === "committed" &&
        operation.result?.outcome === "restore-attachment-activated",
      "operation_state_invalid",
    );
    activationRequest = operation.result.activationRequest;
    const expectedRequest = restoreAttachmentActivationRequestFor(
      input,
      generation.generation,
      activationRequest.lease,
      "operation_state_invalid",
    );
    ensure(
      canonicalSerialize(activationRequest) ===
        canonicalSerialize(expectedRequest),
      "operation_state_invalid",
    );
  }
  return deepFreeze({
    activationRequest,
    generation,
    input,
    launchIdClaim,
    predecessor,
  });
}

async function validateRestoreGenerationRelations(
  transaction,
  operation,
  currentSession,
  forUpdate,
) {
  if (
    operation.kind !== RESTORE_DESTINATION_GENERATION_OPERATION_KIND
  ) {
    return null;
  }
  const input = restoreGenerationInput(
    inputForOperation(operation),
    OPERATION_INPUT_KEYS,
    "operation_state_invalid",
  );
  const generationRow = await readRestoreGenerationRow(
    transaction,
    operation.operationId,
    forUpdate,
  );
  if (
    operation.state === "prepared" ||
    (operation.state === "committed" &&
      operation.result?.outcome === "cancelled-before-dispatch")
  ) {
    ensure(generationRow === null, "operation_state_invalid");
    return deepFreeze({
      generation: null,
      input,
      launchIdClaim: null,
      source: null,
    });
  }
  const source = await readCommittedCheckpointSource(
    transaction,
    input.request.admission.checkpoint,
    currentSession,
    forUpdate,
  );
  ensure(generationRow !== null, "restore_generation_not_authorized");
  const generation = restoreGenerationSnapshotFromRow(
    generationRow,
    input,
    source,
  );
  ensure(
    timestampMilliseconds(generation.claimedAt) >=
      timestampMilliseconds(operation.createdAt) &&
      timestampMilliseconds(generation.claimedAt) <=
        timestampMilliseconds(operation.updatedAt) &&
      (operation.state !== "starting" ||
        generation.claimedAt === operation.updatedAt),
    "operation_state_invalid",
  );
  if (operation.state === "starting" || operation.state === "uncertain") {
    ensure(
      generation.state === "authorized" &&
        generation.document === null &&
        generation.committedAt === null,
      "operation_state_invalid",
    );
  } else {
    ensure(
      operation.state === "committed" &&
        operation.result?.outcome === "restore-generation-committed" &&
        generation.state === "committed" &&
        generation.document !== null &&
        generation.committedAt === operation.updatedAt &&
        operation.result.generationId === generation.generationId &&
        operation.result.checkpointId === generation.checkpointId &&
        operation.result.catalogueSha256 ===
          sha256(canonicalSerialize(source.catalogue.document)) &&
        operation.result.generationDocumentSha256 ===
          sha256(canonicalSerialize(generation.document)),
      "operation_state_invalid",
    );
  }
  let launchIdClaim = null;
  if (
    input.request.contractVersion ===
    RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2
  ) {
    launchIdClaim = await readOperationIdClaim(
      transaction,
      input.request.launchIntent.launchAttemptId,
      forUpdate,
    );
    ensure(launchIdClaim !== null, "operation_state_invalid");
    validateRestoreLaunchIdClaim(
      launchIdClaim,
      input,
      operation.state === "committed" ? "materialized" : "reserved",
    );
    ensure(
      launchIdClaim.claimedAt === generation.claimedAt &&
        (operation.state !== "committed" ||
          launchIdClaim.materializedAt === operation.updatedAt),
      "operation_state_invalid",
    );
  }
  return deepFreeze({ generation, input, launchIdClaim, source });
}

async function validateWriterLaunchGenerationAttachmentRelation(
  transaction,
  input,
  generation,
  currentSession,
  forUpdate,
) {
  const generationAttachmentMatches =
    canonicalSerialize(
      canonicalJsonObject(
        generation.generation.binding.attachment,
        "operation_state_invalid",
      ),
    ) === canonicalSerialize(input.request.attachment);
  let restoreActivation = null;
  if (!generationAttachmentMatches) {
    const activationOperation = await readOperationSnapshot(
      transaction,
      input.request.attachment.operationId,
      forUpdate,
    );
    const activationReservation = await readReservationSnapshot(
      transaction,
      input.request.attachment.operationId,
      forUpdate,
    );
    ensure(
      activationOperation !== null &&
        activationOperation.kind ===
          RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND &&
        activationOperation.state === "committed" &&
        activationOperation.result?.outcome ===
          "restore-attachment-activated" &&
        activationReservation !== null,
      "operation_state_invalid",
    );
    const activationInput = restoreAttachmentActivationInput(
      inputForOperation(activationOperation),
      OPERATION_INPUT_KEYS,
      "operation_state_invalid",
    );
    validateOperationReservation(
      activationOperation,
      activationReservation,
      activationInput,
    );
    validateCommittedOperationHistory(
      activationOperation,
      currentSession,
    );
    restoreActivation =
      await validateRestoreAttachmentActivationRelations(
        transaction,
        activationOperation,
        currentSession,
        forUpdate,
      );
    ensure(
      restoreActivation !== null &&
        restoreActivation.generation !== null &&
        canonicalSerialize(restoreActivation.input.request.generation) ===
          canonicalSerialize(input.request.generation) &&
        canonicalSerialize(
          activationOperation.result?.activationResult.attachment,
        ) === canonicalSerialize(input.request.attachment),
      "operation_state_invalid",
    );
  }
  return restoreActivation;
}

async function validateWriterLaunchAttemptRelations(
  transaction,
  operation,
  currentSession,
  forUpdate,
) {
  if (operation.kind !== WRITER_LAUNCH_ATTEMPT_OPERATION_KIND) {
    return null;
  }
  const input = writerLaunchAttemptInput(
    inputForOperation(operation),
    OPERATION_INPUT_KEYS,
    "operation_state_invalid",
  );
  if (
    operation.state === "prepared" ||
    (operation.state === "committed" &&
      operation.result?.outcome === "cancelled-before-dispatch")
  ) {
    // Prepared is a cancellable intent, not generation authority. The fresh
    // claim performs the first locked generation read; later active and
    // terminal phases continuously revalidate that durable relation.
    return deepFreeze({
      generation: null,
      input,
      launch: null,
      restoreActivation: null,
    });
  }
  const generation = await readCommittedRestoreGenerationForLaunch(
    transaction,
    input.request.generation,
    currentSession,
    forUpdate,
  );
  const restoreActivation =
    await validateWriterLaunchGenerationAttachmentRelation(
      transaction,
      input,
      generation,
      currentSession,
      forUpdate,
    );
  let launch = null;
  if (operation.state === "committed") {
    ensure(operation.result !== null, "operation_state_invalid");
    if (operation.result.outcome === "writer-launch-started") {
      launch = writerLaunchPointerFor(
        input,
        operation.result,
        operation.updatedAt,
        "operation_state_invalid",
      );
    } else {
      ensure(
        operation.result.outcome === "writer-launch-not-started" ||
          operation.result.outcome ===
            "writer-launch-complete-stopped" ||
          operation.result.outcome === "cancelled-before-dispatch",
        "operation_state_invalid",
      );
    }
  }
  return deepFreeze({ generation, input, launch, restoreActivation });
}

async function validateWriterLaunchPointerRelations(
  transaction,
  launch,
  session,
  forUpdate,
) {
  const operation = await readOperationSnapshot(
    transaction,
    launch.launchAttemptId,
    false,
  );
  ensure(
    operation !== null &&
      operation.sessionId === session.sessionId &&
      operation.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND &&
      operation.state === "committed" &&
      operation.result?.outcome === "writer-launch-started",
    "operation_state_invalid",
  );
  validateCommittedOperationHistory(operation, session);
  const reservation = await readReservationSnapshot(
    transaction,
    operation.operationId,
    false,
  );
  ensure(reservation !== null, "operation_state_invalid");
  validateOperationReservation(operation, reservation, {
    reservationId: `reservation-${sha256(operation.operationId)}`,
  });
  const relation = await validateWriterLaunchAttemptRelations(
    transaction,
    operation,
    session,
    forUpdate,
  );
  ensure(
    relation !== null &&
      relation.launch !== null &&
      canonicalSerialize(relation.launch) === canonicalSerialize(launch),
    "operation_state_invalid",
  );
  return deepFreeze({ operation, relation, reservation });
}

async function validateWriterLaunchStopRelations(
  transaction,
  operation,
  currentSession,
  forUpdate,
) {
  if (operation.kind !== WRITER_LAUNCH_STOP_OPERATION_KIND) {
    return null;
  }
  const input = writerLaunchStopInput(
    inputForOperation(operation),
    OPERATION_INPUT_KEYS,
    "operation_state_invalid",
  );
  const launch = input.request.launch;
  ensure(
    input.expectedSession.document.launch !== null &&
      canonicalSerialize(input.expectedSession.document.launch) ===
        canonicalSerialize(launch),
    "operation_state_invalid",
  );
  const originalLaunch = await validateCurrentWriterLaunchRelation(
    transaction,
    input.expectedSession,
    forUpdate,
  );
  ensure(originalLaunch !== null, "operation_state_invalid");
  if (operation.state === "committed") {
    ensure(
      operation.result?.outcome === "writer-launch-stopped" ||
        operation.result?.outcome === "cancelled-before-dispatch",
      "operation_state_invalid",
    );
  }
  if (operation.state !== "committed") {
    ensure(
      currentSession.document.launch !== null &&
        canonicalSerialize(currentSession.document.launch) ===
          canonicalSerialize(launch),
      "operation_state_invalid",
    );
  }
  return deepFreeze({ input, originalLaunch });
}

async function validateCurrentWriterLaunchRelation(
  transaction,
  session,
  forUpdate,
) {
  const launch = session.document.launch;
  if (launch === null) return null;
  const current = await validateWriterLaunchPointerRelations(
    transaction,
    launch,
    session,
    forUpdate,
  );
  const { operation, relation, reservation } = current;
  ensure(
    session.document.lease !== null &&
      session.document.attachment !== null &&
      session.document.lease.sessionId === operation.sessionId &&
      session.document.lease.leaseId === relation.input.request.lease.leaseId &&
      session.document.lease.holderId ===
        relation.input.request.lease.holderId &&
      session.document.lease.fencingEpoch ===
        relation.input.request.lease.fencingEpoch &&
      timestampMilliseconds(session.document.lease.expiresAt) >=
        timestampMilliseconds(relation.input.request.lease.expiresAt) &&
      canonicalSerialize(
        canonicalJsonObject(
          session.document.attachment,
          "operation_state_invalid",
        ),
      ) ===
        canonicalSerialize(relation.input.request.attachment),
    "operation_state_invalid",
  );
  return deepFreeze({ operation, relation, reservation });
}

async function ensureNoActiveRows(transaction, sessionId) {
  const rows = rowsFromResult(
    await transaction.query(READ_ACTIVE_COUNTS_QUERY.text, [sessionId]),
    "operation_state_invalid",
  );
  ensure(rows.length === 1, "operation_state_invalid");
  const counts = exactPlainObject(
    rows[0],
    ["operation_count", "reservation_count"],
    "operation_state_invalid",
  );
  ensure(
    counts.operation_count === 0 && counts.reservation_count === 0,
    "operation_state_invalid",
  );
}

async function validateSessionRelations(transaction, session, forUpdate) {
  const activePointer = session.document.activeOperation;
  let active = null;
  if (activePointer === null) {
    await ensureNoActiveRows(transaction, session.sessionId);
  } else {
    const operation = await readOperationSnapshot(
      transaction,
      activePointer.operationId,
      forUpdate,
    );
    ensure(operation !== null, "operation_state_invalid");
    const reservation = await readReservationSnapshot(
      transaction,
      activePointer.operationId,
      forUpdate,
    );
    ensure(reservation !== null, "operation_state_invalid");
    validateOperationReservation(operation, reservation, {
      reservationId: activePointer.reservationId,
    });
    validateActivePointer(session, operation, reservation);
    const checkpoint = await validateCheckpointCaptureRelations(
      transaction,
      operation,
      forUpdate,
    );
    const generation = await validateRestoreGenerationRelations(
      transaction,
      operation,
      session,
      forUpdate,
    );
    const activation =
      await validateRestoreAttachmentActivationRelations(
        transaction,
        operation,
        session,
        forUpdate,
      );
    const launchAttempt = await validateWriterLaunchAttemptRelations(
      transaction,
      operation,
      session,
      forUpdate,
    );
    const launchStop = await validateWriterLaunchStopRelations(
      transaction,
      operation,
      session,
      forUpdate,
    );
    active = deepFreeze({
      activation,
      checkpoint,
      generation,
      launchAttempt,
      launchStop,
      operation,
      reservation,
    });
  }

  const currentLast = documentLastOperation(session.document);
  const terminalBase =
    active === null ? session : active.operation.expectedSession;
  const expectedLast = documentLastOperation(terminalBase.document);
  ensure(
    sameLastOperation(currentLast, expectedLast),
    "operation_state_invalid",
  );

  let terminal = null;
  if (expectedLast !== null) {
    const operation = await readOperationSnapshot(
      transaction,
      expectedLast.operationId,
      false,
    );
    ensure(operation !== null, "operation_state_invalid");
    const reservation = await readReservationSnapshot(
      transaction,
      expectedLast.operationId,
      false,
    );
    ensure(reservation !== null, "operation_state_invalid");
    validateLastOperationPointer(terminalBase, operation, reservation);
    const checkpoint = await validateCheckpointCaptureRelations(
      transaction,
      operation,
      false,
    );
    const generation = await validateRestoreGenerationRelations(
      transaction,
      operation,
      terminalBase,
      false,
    );
    const activation =
      await validateRestoreAttachmentActivationRelations(
        transaction,
        operation,
        terminalBase,
        false,
      );
    const launchAttempt = await validateWriterLaunchAttemptRelations(
      transaction,
      operation,
      terminalBase,
      false,
    );
    const launchStop = await validateWriterLaunchStopRelations(
      transaction,
      operation,
      session,
      false,
    );
    terminal = deepFreeze({
      activation,
      checkpoint,
      generation,
      launchAttempt,
      launchStop,
      operation,
      reservation,
    });
  }
  if (
    active?.operation.kind === WRITER_FORCE_FENCE_OPERATION_KIND &&
    active.operation.expectedSession.document.attachment === null
  ) {
    validateForceFenceTargetSource(
      inputForOperation(active.operation),
      terminal,
      "operation_state_invalid",
    );
  }
  const currentLaunch = await validateCurrentWriterLaunchRelation(
    transaction,
    session,
    forUpdate,
  );
  return deepFreeze({ active, currentLaunch, terminal });
}

async function readRequestedOperation(
  transaction,
  session,
  input,
  forUpdate,
) {
  const relations = await validateSessionRelations(
    transaction,
    session,
    forUpdate,
  );
  const requestedIsActive =
    relations.active?.operation.operationId === input.operationId;
  const requestedIsTerminal =
    relations.terminal?.operation.operationId === input.operationId;
  const current =
    requestedIsActive
      ? relations.active
      : requestedIsTerminal
        ? relations.terminal
        : null;
  // Mutations already hold this session row and its active relation locks.
  // A foreign or retired operation is identity evidence only; locking it
  // would allow crossed foreign IDs to create an avoidable lock cycle.
  const operation =
    current === null
      ? await readOperationSnapshot(
          transaction,
          input.operationId,
          false,
        )
      : current.operation;
  if (operation === null) {
    return deepFreeze({
      active: relations.active,
      activation: null,
      checkpoint: null,
      generation: null,
      launchAttempt: null,
      launchStop: null,
      terminal: relations.terminal,
      operation: null,
      reservation: null,
    });
  }
  validateOperationIdentity(operation, input);
  if (current === null && operation.state === "committed") {
    validateCommittedOperationHistory(operation, session);
  }
  const reservation =
    current === null
      ? await readReservationSnapshot(
          transaction,
          input.operationId,
          false,
        )
      : current.reservation;
  ensure(reservation !== null, "operation_state_invalid");
  validateOperationReservation(operation, reservation, input);
  const checkpoint =
    current === null
      ? await validateCheckpointCaptureRelations(
          transaction,
          operation,
          false,
        )
      : current.checkpoint;
  const activation =
    current === null
      ? await validateRestoreAttachmentActivationRelations(
          transaction,
          operation,
          session,
          false,
        )
      : current.activation;
  const generation =
    current === null
      ? await validateRestoreGenerationRelations(
          transaction,
          operation,
          session,
          false,
        )
      : current.generation;
  const launchAttempt =
    current === null
      ? await validateWriterLaunchAttemptRelations(
          transaction,
          operation,
          session,
          false,
        )
      : current.launchAttempt;
  const launchStop =
    current === null
      ? await validateWriterLaunchStopRelations(
          transaction,
          operation,
          session,
          false,
        )
      : current.launchStop;
  if (operation.state !== "committed") {
    validateActivePointer(session, operation, reservation);
  }
  return deepFreeze({
    active: relations.active,
    activation,
    checkpoint,
    generation,
    launchAttempt,
    launchStop,
    terminal: relations.terminal,
    operation,
    reservation,
  });
}

function ensureExactExpectedSession(session, expected) {
  if (
    canonicalIdentityBytes(session.document) !==
    canonicalIdentityBytes(expected.document)
  ) {
    fail("session_identity_conflict");
  }
  ensure(
    canonicalSnapshotBytes(session) === canonicalSnapshotBytes(expected),
    "session_revision_conflict",
  );
}

function revisionAfter(
  value,
  increments,
  code = "session_revision_exhausted",
) {
  const revision = BigIntConstructor(value);
  const delta = BigIntConstructor(increments);
  ensure(delta > 0n && revision <= MAX_POSTGRES_BIGINT - delta, code);
  return reflectApply(bigIntToStringIntrinsic, revision + delta, []);
}

function nextRevision(value, code = "session_revision_exhausted") {
  return revisionAfter(value, 1, code);
}

function reservationPayload(input) {
  return deepFreeze({
    reservationVersion: RESERVATION_PAYLOAD_VERSION,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    requestSha256: input.requestSha256,
  });
}

async function updateSessionPhase(
  transaction,
  session,
  input,
  activeOperation,
  lastOperation = documentLastOperation(session.document),
  updatedAt = transaction.now,
) {
  const nextDocument = documentWithActiveOperation(
    session.document,
    activeOperation,
    lastOperation,
  );
  return updateSessionDocument(
    transaction,
    session,
    input,
    nextDocument,
    updatedAt,
  );
}

async function updateSessionDocument(
  transaction,
  session,
  input,
  document,
  updatedAt = transaction.now,
) {
  const nextDocument = canonicalDocument(
    document,
    "session_state_invalid",
  );
  const rows = rowsFromResult(
    await transaction.query(UPDATE_SESSION_QUERY.text, [
      session.sessionId,
      session.revision,
      canonicalSerialize(nextDocument),
      updatedAt,
    ]),
  );
  ensure(rows.length === 1, "session_revision_conflict");
  const updated = snapshotFromRow(rows[0], session.sessionId);
  ensure(
    updated.revision === nextRevision(session.revision) &&
      updated.updatedAt === updatedAt &&
      canonicalSerialize(updated.document) ===
        canonicalSerialize(nextDocument) &&
      canonicalIdentityBytes(updated.document) ===
        canonicalIdentityBytes(input.expectedSession.document),
    "session_state_invalid",
  );
  return updated;
}

function runSerializable(store, callback) {
  return reflectApply(runSerializableIntrinsic, store, [callback]);
}

async function finalizeWriterLaunchAttempt(
  store,
  options,
  acceptedStatuses,
) {
  const input = writerLaunchAttemptFinalizationInput(
    options,
    acceptedStatuses,
  );
  return runSerializable(store, async (transaction) => {
    const session = await readSessionSnapshot(
      transaction,
      input.expectedSession.sessionId,
      true,
    );
    const observed = await readRequestedOperation(
      transaction,
      session,
      input,
      true,
    );
    ensure(
      observed.operation !== null &&
        observed.reservation !== null &&
        observed.launchAttempt !== null,
      "operation_transition_conflict",
    );
    const result = writerLaunchTerminalResult(
      input,
      input.evidence,
      "invalid_operation_request",
    );
    if (observed.operation.state === "committed") {
      ensure(
        BigIntConstructor(input.expectedOperationRevision) + 1n ===
          BigIntConstructor(observed.operation.revision),
        "operation_transition_conflict",
      );
      ensure(
        canonicalSerialize(observed.operation.result) ===
          canonicalSerialize(result),
        "operation_result_conflict",
      );
      const currentLaunch =
        session.document.launch?.launchAttemptId === input.operationId
          ? session.document.launch
          : null;
      return operationReceipt({
        attempt: writerLaunchAttemptRecord(input, observed.operation),
        finalized: false,
        launch: currentLaunch,
        operation: observed.operation,
        reservation: observed.reservation,
        session,
      });
    }
    ensure(
      (observed.operation.state === "starting" ||
        observed.operation.state === "uncertain") &&
        observed.operation.revision === input.expectedOperationRevision,
      "operation_transition_conflict",
    );
    ensure(
      session.document.lifecycle === "ATTACHED" &&
        session.document.launch === null &&
        session.document.lease !== null &&
        session.document.attachment !== null,
      "operation_transition_conflict",
    );
    ensure(
      canonicalSerialize(
        canonicalJsonObject(
          session.document.lease,
          "operation_transition_conflict",
        ),
      ) ===
        canonicalSerialize(input.request.lease) &&
        canonicalSerialize(
          canonicalJsonObject(
            session.document.attachment,
            "operation_transition_conflict",
          ),
        ) ===
          canonicalSerialize(input.request.attachment),
      "operation_transition_conflict",
    );
    nextRevision(session.revision);
    const serializedResult = canonicalSerialize(result);
    const predecessorState = observed.operation.state;
    const operationRows = rowsFromResult(
      await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
        input.operationId,
        input.expectedOperationRevision,
        serializedResult,
        transaction.now,
        predecessorState,
      ]),
      "operation_state_invalid",
    );
    ensure(operationRows.length === 1, "operation_transition_conflict");
    const reservationRows = rowsFromResult(
      await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
        input.operationId,
        transaction.now,
        predecessorState,
      ]),
      "operation_state_invalid",
    );
    ensure(
      reservationRows.length === 1,
      "operation_transition_conflict",
    );
    const operation = operationSnapshotFromRow(operationRows[0]);
    const reservation = reservationSnapshotFromRow(reservationRows[0]);
    validateOperationIdentity(operation, input);
    validateOperationReservation(operation, reservation, input);
    ensure(
      canonicalSerialize(operation.result) === serializedResult,
      "operation_result_conflict",
    );
    const launch =
      result.outcome === "writer-launch-started"
        ? writerLaunchPointerFor(
            input,
            result,
            operation.updatedAt,
            "operation_state_invalid",
          )
        : null;
    const nextDocument = documentWithAuthorityState(session.document, {
      activeOperation: null,
      lastOperation: lastPointerFor(operation, reservation),
      launch,
    });
    const updatedSession = await updateSessionDocument(
      transaction,
      session,
      input,
      nextDocument,
    );
    validateLastOperationPointer(updatedSession, operation, reservation);
    return operationReceipt({
      attempt: writerLaunchAttemptRecord(input, operation),
      finalized: true,
      launch,
      operation,
      reservation,
      session: updatedSession,
    });
  });
}

async function finalizeWriterLaunchStop(store, options) {
  const input = writerLaunchStopFinalizationInput(options);
  return runSerializable(store, async (transaction) => {
    const session = await readSessionSnapshot(
      transaction,
      input.expectedSession.sessionId,
      true,
    );
    const observed = await readRequestedOperation(
      transaction,
      session,
      input,
      true,
    );
    ensure(
      observed.operation !== null &&
        observed.reservation !== null &&
        observed.launchStop !== null,
      "operation_transition_conflict",
    );
    const result = writerLaunchStopTerminalResult(
      input,
      input.evidence,
      "invalid_operation_request",
    );
    if (observed.operation.state === "committed") {
      ensure(
        BigIntConstructor(input.expectedOperationRevision) + 1n ===
          BigIntConstructor(observed.operation.revision),
        "operation_transition_conflict",
      );
      ensure(
        canonicalSerialize(observed.operation.result) ===
          canonicalSerialize(result),
        "operation_result_conflict",
      );
      return operationReceipt({
        finalized: false,
        launch: currentWriterLaunchForAttempt(
          session,
          input.request.launch.launchAttemptId,
        ),
        operation: observed.operation,
        reservation: observed.reservation,
        session,
        stop: writerLaunchStopRecord(input, observed.operation),
      });
    }
    ensure(
      (observed.operation.state === "starting" ||
        observed.operation.state === "uncertain") &&
        observed.operation.revision === input.expectedOperationRevision &&
        session.document.lifecycle === "ATTACHED" &&
        session.document.launch !== null &&
        canonicalSerialize(session.document.launch) ===
          canonicalSerialize(input.request.launch),
      "operation_transition_conflict",
    );
    nextRevision(session.revision);
    const serializedResult = canonicalSerialize(result);
    const predecessorState = observed.operation.state;
    const operationRows = rowsFromResult(
      await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
        input.operationId,
        input.expectedOperationRevision,
        serializedResult,
        transaction.now,
        predecessorState,
      ]),
      "operation_state_invalid",
    );
    ensure(operationRows.length === 1, "operation_transition_conflict");
    const reservationRows = rowsFromResult(
      await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
        input.operationId,
        transaction.now,
        predecessorState,
      ]),
      "operation_state_invalid",
    );
    ensure(reservationRows.length === 1, "operation_transition_conflict");
    const operation = operationSnapshotFromRow(operationRows[0]);
    const reservation = reservationSnapshotFromRow(reservationRows[0]);
    validateOperationIdentity(operation, input);
    validateOperationReservation(operation, reservation, input);
    ensure(
      canonicalSerialize(operation.result) === serializedResult,
      "operation_result_conflict",
    );
    const nextDocument = documentWithAuthorityState(session.document, {
      activeOperation: null,
      lastOperation: lastPointerFor(operation, reservation),
      launch: null,
    });
    const updatedSession = await updateSessionDocument(
      transaction,
      session,
      input,
      nextDocument,
    );
    validateLastOperationPointer(updatedSession, operation, reservation);
    return operationReceipt({
      finalized: true,
      launch: null,
      operation,
      reservation,
      session: updatedSession,
      stop: writerLaunchStopRecord(input, operation),
    });
  });
}

export class PostgresSessionAuthority {
  #restoreAttachmentActivationV2FleetCompatible;

  #restoreGenerationV2FleetCompatible;

  #store;

  constructor(options) {
    if (
      options === null ||
      typeof options !== "object" ||
      isProxyValue(options) ||
      arrayIsArray(options)
    ) {
      fail("invalid_authority_options");
    }
    let optionKeys;
    try {
      optionKeys = reflectOwnKeys(options);
    } catch {
      fail("invalid_authority_options");
    }
    const hasRestoreAttachmentActivationV2FleetCompatible = reflectApply(
      arrayIncludesIntrinsic,
      optionKeys,
      ["restoreAttachmentActivationV2FleetCompatible"],
    );
    const hasRestoreGenerationV2FleetCompatible = reflectApply(
      arrayIncludesIntrinsic,
      optionKeys,
      ["restoreGenerationV2FleetCompatible"],
    );
    const expectedOptionKeys =
      hasRestoreAttachmentActivationV2FleetCompatible
        ? hasRestoreGenerationV2FleetCompatible
          ? [
              "restoreAttachmentActivationV2FleetCompatible",
              "restoreGenerationV2FleetCompatible",
              "store",
            ]
          : ["restoreAttachmentActivationV2FleetCompatible", "store"]
        : hasRestoreGenerationV2FleetCompatible
          ? ["restoreGenerationV2FleetCompatible", "store"]
          : ["store"];
    const normalized = exactPlainObject(
      options,
      expectedOptionKeys,
      "invalid_authority_options",
    );
    let prototype;
    let ownKeys;
    try {
      prototype = objectGetPrototypeOf(normalized.store);
      ownKeys = reflectOwnKeys(normalized.store);
    } catch {
      fail("invalid_authority_options");
    }
    ensure(
      normalized.store !== null &&
        typeof normalized.store === "object" &&
        !isProxyValue(normalized.store) &&
        prototype === PostgresSerializableStore.prototype &&
        ownKeys.length === 0 &&
        objectIsFrozen(normalized.store),
      "invalid_authority_options",
    );
    ensure(
      (!hasRestoreAttachmentActivationV2FleetCompatible ||
        typeof normalized.restoreAttachmentActivationV2FleetCompatible ===
          "boolean") &&
        (!hasRestoreGenerationV2FleetCompatible ||
          typeof normalized.restoreGenerationV2FleetCompatible ===
            "boolean"),
      "invalid_authority_options",
    );
    this.#restoreAttachmentActivationV2FleetCompatible =
      normalized.restoreAttachmentActivationV2FleetCompatible === true;
    this.#restoreGenerationV2FleetCompatible =
      normalized.restoreGenerationV2FleetCompatible === true;
    this.#store = normalized.store;
    objectFreeze(this);
  }

  async registerSession(options) {
    const document = registrationDocument(options);
    const sessionId = document.manifest.sessionId;
    const serializedDocument = canonicalSerialize(document);
    return runSerializable(this.#store, async (transaction) => {
      const inserted = rowsFromResult(
        await transaction.query(INSERT_SESSION_QUERY.text, [
          sessionId,
          serializedDocument,
          transaction.now,
        ]),
      );
      if (inserted.length === 1) {
        const snapshot = snapshotFromRow(inserted[0], sessionId);
        ensure(
          snapshot.revision === "0" &&
            snapshot.createdAt === transaction.now &&
            snapshot.updatedAt === transaction.now &&
            canonicalSerialize(snapshot.document) === serializedDocument,
          "session_state_invalid",
        );
        return snapshot;
      }
      const existing = rowsFromResult(
        await transaction.query(READ_SESSION_FOR_UPDATE_QUERY.text, [
          sessionId,
        ]),
      );
      ensure(existing.length === 1, "session_state_invalid");
      const snapshot = snapshotFromRow(existing[0], sessionId);
      ensure(
        canonicalIdentityBytes(snapshot.document) ===
          canonicalIdentityBytes(document),
        "session_identity_conflict",
      );
      await validateSessionRelations(transaction, snapshot, true);
      return snapshot;
    });
  }

  async readSession(options) {
    const normalized = exactPlainObject(
      options,
      ["sessionId"],
      "invalid_session_read",
    );
    const sessionId = canonicalSessionId(
      normalized.sessionId,
      "invalid_session_read",
    );
    return runSerializable(this.#store, async (transaction) => {
      const rows = rowsFromResult(
        await transaction.query(READ_SESSION_QUERY.text, [sessionId]),
      );
      ensure(rows.length === 1, "session_not_found");
      const snapshot = snapshotFromRow(rows[0], sessionId);
      await validateSessionRelations(transaction, snapshot, false);
      return snapshot;
    });
  }

  async listCheckpointCaptureRecoveryCandidates(options) {
    const listInput = checkpointCaptureRecoveryListInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const maximumRows = listInput.limit + 1;
      const afterCursor = listInput.afterSessionId;
      const query =
        afterCursor === null
          ? LIST_CHECKPOINT_CAPTURE_RECOVERY_FIRST_PAGE_QUERY
          : LIST_CHECKPOINT_CAPTURE_RECOVERY_AFTER_QUERY;
      const values =
        afterCursor === null
          ? [maximumRows]
          : [afterCursor, maximumRows];
      const operationRows = pageRowsFromResult(
        await transaction.query(query.text, values),
        maximumRows,
        "operation_state_invalid",
      );
      const candidateCount =
        operationRows.length > listInput.limit
          ? listInput.limit
          : operationRows.length;
      const candidates = new ArrayConstructor(candidateCount);
      objectSetPrototypeOf(candidates, null);
      let previousSessionId = afterCursor;
      let lastCandidateSessionId = null;

      for (let index = 0; index < operationRows.length; index += 1) {
        const operation = operationSnapshotFromRow(
          ownDataValue(
            operationRows,
            reflectApply(StringConstructor, undefined, [index]),
            "operation_state_invalid",
          ),
        );
        ensure(
          operation.kind === CHECKPOINT_CAPTURE_OPERATION_KIND &&
            (operation.state === "starting" ||
              operation.state === "uncertain") &&
            operation.retiredAt === null &&
            (previousSessionId === null ||
              operation.sessionId > previousSessionId),
          "operation_state_invalid",
        );
        const input = checkpointCaptureInput(
          inputForOperation(operation),
          OPERATION_INPUT_KEYS,
          "operation_state_invalid",
        );
        const durableRequest = input.request.admission.request;
        ensure(
          durableRequest.sessionId === operation.sessionId,
          "operation_state_invalid",
        );
        const session = await readSessionSnapshot(
          transaction,
          operation.sessionId,
          false,
        );
        let observed;
        try {
          observed = await readRequestedOperation(
            transaction,
            session,
            input,
            false,
          );
        } catch (error) {
          if (
            error instanceof PostgresSessionAuthorityError &&
            error.code === "checkpoint_capture_not_authorized"
          ) {
            fail("operation_state_invalid");
          }
          throw error;
        }
        ensure(
          observed.active !== null &&
            observed.active.operation.operationId === operation.operationId &&
            observed.operation !== null &&
            observed.reservation !== null &&
            observed.checkpoint?.attempt !== null &&
            observed.checkpoint?.attempt !== undefined &&
            observed.checkpoint.catalogue === null &&
            (observed.operation.state === "starting" ||
              observed.operation.state === "uncertain") &&
            canonicalSerialize(observed.operation) ===
              canonicalSerialize(operation),
          "operation_state_invalid",
        );
        if (index < candidateCount) {
          candidates[index] = deepFreeze({
            checkpoint: input.request.admission.checkpoint,
            request: durableRequest,
          });
          lastCandidateSessionId = durableRequest.sessionId;
        }
        previousSessionId = durableRequest.sessionId;
      }
      objectSetPrototypeOf(candidates, arrayPrototype);

      return deepFreeze({
        candidates,
        nextAfterSessionId:
          operationRows.length > listInput.limit
            ? lastCandidateSessionId
            : null,
      });
    });
  }

  async listRestoreDestinationGenerationRecoveryCandidates(options) {
    const listInput = restoreGenerationRecoveryListInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const maximumRows = listInput.limit + 1;
      const afterCursor = listInput.afterSessionId;
      const query =
        afterCursor === null
          ? LIST_RESTORE_GENERATION_RECOVERY_FIRST_PAGE_QUERY
          : LIST_RESTORE_GENERATION_RECOVERY_AFTER_QUERY;
      const values =
        afterCursor === null
          ? [maximumRows]
          : [afterCursor, maximumRows];
      const operationRows = pageRowsFromResult(
        await transaction.query(query.text, values),
        maximumRows,
        "operation_state_invalid",
      );
      const candidateCount =
        operationRows.length > listInput.limit
          ? listInput.limit
          : operationRows.length;
      const candidates = new ArrayConstructor(candidateCount);
      objectSetPrototypeOf(candidates, null);
      let previousSessionId = afterCursor;
      let lastCandidateSessionId = null;

      for (let index = 0; index < operationRows.length; index += 1) {
        const operation = operationSnapshotFromRow(
          ownDataValue(
            operationRows,
            reflectApply(StringConstructor, undefined, [index]),
            "operation_state_invalid",
          ),
        );
        ensure(
          operation.kind ===
              RESTORE_DESTINATION_GENERATION_OPERATION_KIND &&
            (operation.state === "starting" ||
              operation.state === "uncertain") &&
            operation.retiredAt === null &&
            (previousSessionId === null ||
              operation.sessionId > previousSessionId),
          "operation_state_invalid",
        );
        const input = restoreGenerationInput(
          inputForOperation(operation),
          OPERATION_INPUT_KEYS,
          "operation_state_invalid",
        );
        const durableRequest = input.request.admission.request;
        const session = await readSessionSnapshot(
          transaction,
          operation.sessionId,
          false,
        );
        const observed = await readRequestedOperation(
          transaction,
          session,
          input,
          false,
        );
        ensure(
          observed.active !== null &&
            observed.active.operation.operationId === operation.operationId &&
            observed.operation !== null &&
            observed.reservation !== null &&
            observed.generation?.generation !== null &&
            observed.generation?.generation !== undefined &&
            observed.generation.generation.state === "authorized" &&
            (observed.operation.state === "starting" ||
              observed.operation.state === "uncertain") &&
            canonicalSerialize(observed.operation) ===
              canonicalSerialize(operation),
          "operation_state_invalid",
        );
        if (index < candidateCount) {
          candidates[index] = deepFreeze({
            checkpoint: input.request.admission.checkpoint,
            generationId: observed.generation.generation.generationId,
            ...(input.request.contractVersion ===
              RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2
              ? { launchIntent: input.request.launchIntent }
              : {}),
            request: durableRequest,
          });
          lastCandidateSessionId = durableRequest.sessionId;
        }
        previousSessionId = durableRequest.sessionId;
      }
      objectSetPrototypeOf(candidates, arrayPrototype);

      return deepFreeze({
        candidates,
        nextAfterSessionId:
          operationRows.length > listInput.limit
            ? lastCandidateSessionId
            : null,
      });
    });
  }

  async listRestoreAttachmentActivationRecoveryCandidates(options) {
    const listInput = restoreAttachmentActivationRecoveryListInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const maximumRows = listInput.limit + 1;
      const afterCursor = listInput.afterSessionId;
      const query =
        afterCursor === null
          ? LIST_RESTORE_ATTACHMENT_ACTIVATION_RECOVERY_FIRST_PAGE_QUERY
          : LIST_RESTORE_ATTACHMENT_ACTIVATION_RECOVERY_AFTER_QUERY;
      const values =
        afterCursor === null
          ? [maximumRows]
          : [afterCursor, maximumRows];
      const operationRows = pageRowsFromResult(
        await transaction.query(query.text, values),
        maximumRows,
        "operation_state_invalid",
      );
      const candidateCount =
        operationRows.length > listInput.limit
          ? listInput.limit
          : operationRows.length;
      const candidates = new ArrayConstructor(candidateCount);
      objectSetPrototypeOf(candidates, null);
      let previousSessionId = afterCursor;
      let lastCandidateSessionId = null;
      for (let index = 0; index < operationRows.length; index += 1) {
        const operation = operationSnapshotFromRow(
          ownDataValue(
            operationRows,
            reflectApply(StringConstructor, undefined, [index]),
            "operation_state_invalid",
          ),
        );
        ensure(
          operation.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND &&
            (operation.state === "starting" ||
              operation.state === "uncertain") &&
            operation.retiredAt === null &&
            (previousSessionId === null ||
              operation.sessionId > previousSessionId),
          "operation_state_invalid",
        );
        const input = restoreAttachmentActivationInput(
          inputForOperation(operation),
          OPERATION_INPUT_KEYS,
          "operation_state_invalid",
        );
        const session = await readSessionSnapshot(
          transaction,
          operation.sessionId,
          false,
        );
        const observed = await readRequestedOperation(
          transaction,
          session,
          input,
          false,
        );
        ensure(
          observed.active !== null &&
            observed.active.operation.operationId === operation.operationId &&
            observed.operation !== null &&
            observed.reservation !== null &&
            observed.activation !== null &&
            observed.activation.activationRequest !== null &&
            (observed.operation.state === "starting" ||
              observed.operation.state === "uncertain") &&
            canonicalSerialize(observed.operation) ===
              canonicalSerialize(operation),
          "operation_state_invalid",
        );
        if (index < candidateCount) {
          candidates[index] = deepFreeze({
            activationOperationId: input.operationId,
            request: input.request,
            state: observed.operation.state,
          });
          lastCandidateSessionId = operation.sessionId;
        }
        previousSessionId = operation.sessionId;
      }
      objectSetPrototypeOf(candidates, arrayPrototype);
      return deepFreeze({
        candidates,
        nextAfterSessionId:
          operationRows.length > listInput.limit
            ? lastCandidateSessionId
            : null,
      });
    });
  }

  async listWriterLaunchAttemptRecoveryCandidates(options) {
    const listInput = writerLaunchAttemptRecoveryListInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const maximumRows = listInput.limit + 1;
      const afterCursor = listInput.afterSessionId;
      const query =
        afterCursor === null
          ? LIST_WRITER_LAUNCH_RECOVERY_FIRST_PAGE_QUERY
          : LIST_WRITER_LAUNCH_RECOVERY_AFTER_QUERY;
      const values =
        afterCursor === null
          ? [maximumRows]
          : [afterCursor, maximumRows];
      const operationRows = pageRowsFromResult(
        await transaction.query(query.text, values),
        maximumRows,
        "operation_state_invalid",
      );
      const candidateCount =
        operationRows.length > listInput.limit
          ? listInput.limit
          : operationRows.length;
      const candidates = new ArrayConstructor(candidateCount);
      objectSetPrototypeOf(candidates, null);
      let previousSessionId = afterCursor;
      let lastCandidateSessionId = null;

      for (let index = 0; index < operationRows.length; index += 1) {
        const operation = operationSnapshotFromRow(
          ownDataValue(
            operationRows,
            reflectApply(StringConstructor, undefined, [index]),
            "operation_state_invalid",
          ),
        );
        ensure(
          operation.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND &&
            (operation.state === "prepared" ||
              operation.state === "starting" ||
              operation.state === "uncertain") &&
            operation.retiredAt === null &&
            (previousSessionId === null ||
              operation.sessionId > previousSessionId),
          "operation_state_invalid",
        );
        const input = writerLaunchAttemptInput(
          inputForOperation(operation),
          OPERATION_INPUT_KEYS,
          "operation_state_invalid",
        );
        const session = await readSessionSnapshot(
          transaction,
          operation.sessionId,
          false,
        );
        const observed = await readRequestedOperation(
          transaction,
          session,
          input,
          false,
        );
        ensure(
          observed.active !== null &&
            observed.active.operation.operationId === operation.operationId &&
            observed.operation !== null &&
            observed.reservation !== null &&
            observed.launchAttempt !== null &&
            (observed.operation.state === "prepared" ||
              observed.operation.state === "starting" ||
              observed.operation.state === "uncertain") &&
            canonicalSerialize(observed.operation) ===
              canonicalSerialize(operation),
          "operation_state_invalid",
        );
        isAtomicRestoreLaunchHandoff(session, observed) ||
          isAtomicRestoreAttachmentActivationLaunchHandoff(
            session,
            observed,
          );
        if (index < candidateCount) {
          candidates[index] = deepFreeze({
            launchAttemptId: input.operationId,
            request: input.request,
            state: observed.operation.state,
          });
          lastCandidateSessionId = operation.sessionId;
        }
        previousSessionId = operation.sessionId;
      }
      objectSetPrototypeOf(candidates, arrayPrototype);
      return deepFreeze({
        candidates,
        nextAfterSessionId:
          operationRows.length > listInput.limit
            ? lastCandidateSessionId
            : null,
      });
    });
  }

  async listCurrentWriterLaunchRecoveryCandidates(options) {
    const listInput = currentWriterLaunchRecoveryListInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const maximumRows = listInput.limit + 1;
      const afterCursor = listInput.afterSessionId;
      const query =
        afterCursor === null
          ? LIST_CURRENT_WRITER_LAUNCH_FIRST_PAGE_QUERY
          : LIST_CURRENT_WRITER_LAUNCH_AFTER_QUERY;
      const values =
        afterCursor === null
          ? [maximumRows]
          : [afterCursor, maximumRows];
      const sessionRows = pageRowsFromResult(
        await transaction.query(query.text, values),
        maximumRows,
        "session_state_invalid",
      );
      const scannedCount =
        sessionRows.length > listInput.limit
          ? listInput.limit
          : sessionRows.length;
      const scannedCandidates = new ArrayConstructor(scannedCount);
      objectSetPrototypeOf(scannedCandidates, null);
      let candidateCount = 0;
      let previousSessionId = afterCursor;
      let lastScannedSessionId = null;

      for (let index = 0; index < sessionRows.length; index += 1) {
        const row = ownDataValue(
          sessionRows,
          reflectApply(StringConstructor, undefined, [index]),
          "session_state_invalid",
        );
        const sessionId = canonicalSessionId(
          ownDataValue(row, "session_id", "session_state_invalid"),
          "session_state_invalid",
        );
        ensure(
          previousSessionId === null || sessionId > previousSessionId,
          "session_state_invalid",
        );
        const session = snapshotFromRow(row, sessionId);
        if (index < scannedCount) {
          const relations = await validateSessionRelations(
            transaction,
            session,
            false,
          );
          if (relations.currentLaunch !== null) {
            const launch = session.document.launch;
            ensure(
              launch !== null &&
                launch.launchAttemptId ===
                  relations.currentLaunch.operation.operationId,
              "operation_state_invalid",
            );
            scannedCandidates[candidateCount] = deepFreeze({
              launch,
              launchAttemptId: launch.launchAttemptId,
              request: relations.currentLaunch.relation.input.request,
            });
            candidateCount += 1;
          }
          lastScannedSessionId = sessionId;
        }
        previousSessionId = sessionId;
      }

      const candidates = new ArrayConstructor(candidateCount);
      objectSetPrototypeOf(candidates, null);
      for (let index = 0; index < candidateCount; index += 1) {
        candidates[index] = ownDataValue(
          scannedCandidates,
          reflectApply(StringConstructor, undefined, [index]),
          "session_state_invalid",
        );
      }
      objectSetPrototypeOf(candidates, arrayPrototype);
      return deepFreeze({
        candidates,
        nextAfterSessionId:
          sessionRows.length > listInput.limit
            ? lastScannedSessionId
            : null,
      });
    });
  }

  async reserveOperation(options) {
    const input = canonicalOperationInput(options);
    ensure(
      input.kind !== WRITER_LEASE_RENEW_OPERATION_KIND,
      "invalid_operation_request",
    );
    const reserve = () =>
      runSerializable(this.#store, async (transaction) => {
        const session = await readSessionSnapshot(
          transaction,
          input.expectedSession.sessionId,
          true,
        );
        const observed = await readRequestedOperation(
          transaction,
          session,
          input,
          true,
        );
        if (observed.operation !== null) {
          return operationReceipt({
            acquired: false,
            operation: observed.operation,
            reservation: observed.reservation,
            session,
          });
        }
        ensure(
          input.kind !== RESTORE_DESTINATION_GENERATION_OPERATION_KIND ||
            input.request.contractVersion !==
              RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2 ||
            this.#restoreGenerationV2FleetCompatible,
          "restore_generation_v2_fleet_capability_required",
        );
        ensure(
          input.kind !== RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND ||
            input.request.contractVersion !==
              RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION_V2 ||
            this.#restoreAttachmentActivationV2FleetCompatible,
          "restore_attachment_activation_v2_fleet_capability_required",
        );
        if (
          input.kind === WRITER_FORCE_FENCE_OPERATION_KIND &&
          input.expectedSession.document.attachment === null
        ) {
          validateForceFenceTargetSource(
            input,
            observed.terminal,
            "operation_transition_conflict",
          );
        }
        ensure(observed.active === null, "session_operation_conflict");
        ensureExactExpectedSession(session, input.expectedSession);
        if (
          input.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND &&
          input.request.contractVersion ===
            RESTORE_ATTACHMENT_ACTIVATION_OPERATION_CONTRACT_VERSION_V2
        ) {
          await readRestoreAttachmentActivationCoreRelations(
            transaction,
            input,
            session,
            true,
          );
        }
        // Preserve operation recoverability: a prepared operation must retain
        // enough revision space to reach one legal terminal state. BLOCKED
        // force-fence reservations cannot use generic cancellation because it
        // would replace their anchored fence target, so reserve the complete
        // reserve -> claim -> uncertain -> BLOCKED path before writing.
        revisionAfter(
          session.revision,
          input.kind === WRITER_FORCE_FENCE_OPERATION_KIND &&
            session.document.lifecycle === "BLOCKED"
            ? 4
            : 2,
        );

        const operationRows = rowsFromResult(
          await transaction.query(INSERT_OPERATION_QUERY.text, [
            input.operationId,
            session.sessionId,
            input.kind,
            input.serializedEnvelope,
            transaction.now,
          ]),
          "operation_state_invalid",
        );
        if (operationRows.length === 0) {
          const existing = await readOperationSnapshot(
            transaction,
            input.operationId,
            true,
          );
          if (existing === null) {
            const conflictingClaim = await readOperationIdClaim(
              transaction,
              input.operationId,
              false,
            );
            if (conflictingClaim === null) {
              throw OPERATION_VISIBILITY_RETRY;
            }
            fail("operation_identity_conflict");
          }
          validateOperationIdentity(existing, input);
          fail("operation_state_invalid");
        }
        const operation = operationSnapshotFromRow(operationRows[0]);
        validateOperationIdentity(operation, input);

        const payload = reservationPayload(input);
        const reservationRows = rowsFromResult(
          await transaction.query(INSERT_RESERVATION_QUERY.text, [
            input.reservationId,
            input.operationId,
            session.sessionId,
            input.kind,
            session.revision,
            canonicalSerialize(payload),
            transaction.now,
          ]),
          "operation_state_invalid",
        );
        ensure(reservationRows.length === 1, "operation_state_invalid");
        const reservation = reservationSnapshotFromRow(reservationRows[0]);
        validateOperationReservation(operation, reservation, input);

        const updatedSession = await updateSessionPhase(
          transaction,
          session,
          input,
          activePointerFor(input, "prepared", "0"),
        );
        validateActivePointer(updatedSession, operation, reservation);
        return operationReceipt({
          acquired: true,
          operation,
          reservation,
          session: updatedSession,
        });
      });
    try {
      return await reserve();
    } catch (error) {
      if (error !== OPERATION_VISIBILITY_RETRY) {
        throw error;
      }
    }
    try {
      return await reserve();
    } catch (error) {
      if (error === OPERATION_VISIBILITY_RETRY) {
        fail("operation_state_invalid");
      }
      throw error;
    }
  }

  async reconcileOperation(options) {
    const input = canonicalOperationInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        false,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        false,
      );
      if (observed.operation !== null) {
        return operationReceipt({
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(observed.active === null, "session_operation_conflict");
      ensureExactExpectedSession(session, input.expectedSession);
      return operationReceipt({
        operation: null,
        reservation: null,
        session,
        status: "absent",
      });
    });
  }

  async reconcileWriterLaunchStopOperation(options) {
    const input = writerLaunchStopReconcileInput(options);
    return runSerializable(this.#store, async (transaction) => {
      // Lock the session before proving absence so an earlier ambiguous
      // reserve must finish committing or rolling back before this read.
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      if (observed.operation !== null) {
        ensure(
          observed.reservation !== null && observed.launchStop !== null,
          "operation_state_invalid",
        );
        return operationReceipt({
          ...(input.request.contractVersion ===
          WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2
            ? {
                claimTokenMatched:
                  writerLaunchStopClaimSha256(
                    input.claimToken,
                    "invalid_operation_request",
                  ) === input.request.dispatchClaimSha256,
              }
            : {}),
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(observed.active === null, "session_operation_conflict");
      const operationIdClaim = await readOperationIdClaim(
        transaction,
        input.operationId,
        false,
      );
      ensure(operationIdClaim === null, "operation_identity_conflict");

      const expectedSessionMatched =
        canonicalSnapshotBytes(session) ===
        canonicalSnapshotBytes(input.expectedSession);
      if (!expectedSessionMatched) {
        ensure(
          session.createdAt === input.expectedSession.createdAt &&
            canonicalIdentityBytes(session.document) ===
              canonicalIdentityBytes(input.expectedSession.document) &&
            BigIntConstructor(session.revision) >
              BigIntConstructor(input.expectedSession.revision),
          "session_revision_conflict",
        );
      }
      return operationReceipt({
        ...(input.request.contractVersion ===
        WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2
          ? { claimTokenMatched: false }
          : {}),
        expectedSessionMatched,
        operation: null,
        reservation: null,
        session,
        status: "absent",
      });
    });
  }

  async claimOperationDispatch(options) {
    const input = operationInputWithExpectedRevision(options, "0");
    ensure(
      input.kind !== WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND &&
        input.kind !== WRITER_LEASE_RENEW_OPERATION_KIND &&
        input.kind !== WRITER_RELEASE_OPERATION_KIND &&
        input.kind !== WRITER_FORCE_FENCE_OPERATION_KIND &&
        input.kind !== CHECKPOINT_CAPTURE_OPERATION_KIND &&
        input.kind !== RESTORE_DESTINATION_GENERATION_OPERATION_KIND &&
        input.kind !== RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND &&
        input.kind !== WRITER_LAUNCH_ATTEMPT_OPERATION_KIND &&
        input.kind !== WRITER_LAUNCH_STOP_OPERATION_KIND,
      "invalid_operation_request",
    );
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state !== "prepared") {
        return operationReceipt({
          dispatchGranted: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.revision === input.expectedOperationRevision,
        "operation_transition_conflict",
      );
      revisionAfter(session.revision, 2);
      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        activePointerFor(input, "starting", "1"),
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        dispatchGranted: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async claimCheckpointCaptureDispatch(options) {
    const input = checkpointCaptureTransitionInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state !== "prepared") {
        ensure(
          observed.checkpoint?.attempt !== null &&
            observed.checkpoint?.attempt !== undefined,
          "checkpoint_capture_not_authorized",
        );
        return operationReceipt({
          attempt: checkpointCaptureAttemptRecord(
            input,
            observed.checkpoint.attempt,
            observed.checkpoint.catalogue,
          ),
          authorityNow: observed.checkpoint.attempt.claimedAt,
          dispatchGranted: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.revision === input.expectedOperationRevision &&
          observed.checkpoint?.attempt === null &&
          observed.checkpoint?.catalogue === null &&
          session.document.lifecycle === "ATTACHED" &&
          session.document.lease !== null &&
          session.document.attachment !== null &&
          canonicalSerialize(
            canonicalJsonObject(
              session.document.attachment,
              "operation_state_invalid",
            ),
          ) ===
            canonicalSerialize(input.request.admission.attachment),
        "operation_transition_conflict",
      );
      revisionAfter(session.revision, 3);
      const authorityNow = await readAuthorityClock(transaction);
      ensure(
        timestampMilliseconds(authorityNow) >=
          timestampMilliseconds(transaction.now),
        "session_state_invalid",
      );
      ensure(
        timestampMilliseconds(session.document.lease.expiresAt) >
          timestampMilliseconds(authorityNow),
        "writer_lease_expired",
      );
      try {
        assertStorageMutationMatchesLeaseSnapshot({
          canonicalLease: session.document.lease,
          now: timestampMilliseconds(authorityNow),
          request: input.request.admission.request,
          storageRef: session.document.storageRef,
        });
      } catch {
        fail("operation_transition_conflict");
      }

      const binding = checkpointCaptureBinding(input);
      const attemptRows = rowsFromResult(
        await transaction.query(INSERT_CAPTURE_ATTEMPT_QUERY.text, [
          input.request.admission.captureAttemptId,
          input.operationId,
          session.sessionId,
          canonicalSerialize(binding),
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(attemptRows.length === 1, "checkpoint_identity_conflict");
      const attempt = captureAttemptSnapshotFromRow(attemptRows[0], input);

      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(attempt.claimedAt === operation.updatedAt, "operation_state_invalid");
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        activePointerFor(input, "starting", "1"),
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        attempt: checkpointCaptureAttemptRecord(input, attempt, null),
        authorityNow,
        dispatchGranted: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async finalizeCheckpointCapture(options) {
    const input = checkpointCaptureFinalizationInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "committed") {
        ensure(
          observed.operation.result?.outcome === "checkpoint-captured" &&
            BigIntConstructor(input.expectedOperationRevision) + 1n ===
              BigIntConstructor(observed.operation.revision) &&
            observed.checkpoint?.attempt !== null &&
            observed.checkpoint?.attempt !== undefined &&
            observed.checkpoint.catalogue !== null,
          "operation_transition_conflict",
        );
        ensure(
          canonicalSerialize(observed.checkpoint.catalogue.document) ===
            canonicalSerialize(input.completion.document),
          "operation_result_conflict",
        );
        return operationReceipt({
          attempt: checkpointCaptureAttemptRecord(
            input,
            observed.checkpoint.attempt,
            observed.checkpoint.catalogue,
          ),
          catalogue: observed.checkpoint.catalogue,
          finalized: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        (observed.operation.state === "starting" ||
          observed.operation.state === "uncertain") &&
          observed.operation.revision ===
            input.expectedOperationRevision &&
          observed.checkpoint?.attempt !== null &&
          observed.checkpoint?.attempt !== undefined &&
          observed.checkpoint.catalogue === null &&
          session.document.lifecycle === "ATTACHED",
        "operation_transition_conflict",
      );
      nextRevision(session.revision);

      const catalogueRows = rowsFromResult(
        await transaction.query(INSERT_CHECKPOINT_CATALOGUE_QUERY.text, [
          input.request.admission.checkpoint.checkpointId,
          session.sessionId,
          input.request.admission.captureAttemptId,
          canonicalSerialize(input.completion.document),
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(catalogueRows.length === 1, "checkpoint_identity_conflict");
      const catalogue = checkpointCatalogueSnapshotFromRow(
        catalogueRows[0],
        input,
      );
      const result = checkpointCaptureTerminalResult(
        input,
        catalogue.document,
      );
      const serializedResult = canonicalSerialize(result);
      const predecessorState = observed.operation.state;
      const operationRows = rowsFromResult(
        await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          serializedResult,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        canonicalSerialize(operation.result) === serializedResult &&
          catalogue.committedAt === operation.updatedAt,
        "operation_result_conflict",
      );
      const nextDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: null,
          lastOperation: lastPointerFor(operation, reservation),
        },
      );
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateLastOperationPointer(
        updatedSession,
        operation,
        reservation,
      );
      return operationReceipt({
        attempt: checkpointCaptureAttemptRecord(
          input,
          observed.checkpoint.attempt,
          catalogue,
        ),
        catalogue,
        finalized: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async readCheckpointCaptureAttempt(options) {
    const readInput = checkpointCaptureReadInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const operation = await readOperationSnapshot(
        transaction,
        readInput.request.operationId,
        false,
      );
      ensure(operation !== null, "checkpoint_capture_not_authorized");
      const input = checkpointCaptureInput(inputForOperation(operation));
      ensure(
        canonicalSerialize(input.request.admission.checkpoint) ===
            canonicalSerialize(readInput.checkpoint) &&
          canonicalSerialize(input.request.admission.request) ===
            canonicalSerialize(readInput.request),
        "checkpoint_capture_not_authorized",
      );
      const session = await readSessionSnapshot(
        transaction,
        operation.sessionId,
        false,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        false,
      );
      ensure(
        observed.operation !== null &&
          observed.reservation !== null &&
          observed.checkpoint?.attempt !== null &&
          observed.checkpoint?.attempt !== undefined &&
          observed.operation.state !== "prepared",
        "checkpoint_capture_not_authorized",
      );
      const attempt = checkpointCaptureAttemptRecord(
        input,
        observed.checkpoint.attempt,
        observed.checkpoint.catalogue,
      );
      return operationReceipt({
        attempt,
        catalogue: observed.checkpoint.catalogue,
        operation: observed.operation,
        reservation: observed.reservation,
        session,
        status: attempt.state,
      });
    });
  }

  async readCheckpointCatalogue(options) {
    const readInput = checkpointCatalogueReadInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const catalogueRows = rowsFromResult(
        await transaction.query(
          READ_CHECKPOINT_CATALOGUE_BY_ID_QUERY.text,
          [readInput.checkpoint.checkpointId],
        ),
        "operation_state_invalid",
      );
      ensure(catalogueRows.length === 1, "checkpoint_catalogue_not_found");
      const catalogueIdentity = checkpointCatalogueIdentityFromRow(
        catalogueRows[0],
      );
      ensure(
        catalogueIdentity.checkpointId ===
          readInput.checkpoint.checkpointId,
        "operation_state_invalid",
      );

      const attemptRows = rowsFromResult(
        await transaction.query(READ_CAPTURE_ATTEMPT_BY_ID_QUERY.text, [
          catalogueIdentity.captureAttemptId,
        ]),
        "operation_state_invalid",
      );
      ensure(attemptRows.length === 1, "operation_state_invalid");
      const attemptIdentity = captureAttemptIdentityFromRow(attemptRows[0]);
      ensure(
        attemptIdentity.captureAttemptId ===
            catalogueIdentity.captureAttemptId &&
          attemptIdentity.sessionId === catalogueIdentity.sessionId,
        "operation_state_invalid",
      );
      const operation = await readOperationSnapshot(
        transaction,
        attemptIdentity.operationId,
        false,
      );
      ensure(operation !== null, "operation_state_invalid");
      const input = checkpointCaptureInput(inputForOperation(operation));
      ensure(
        canonicalSerialize(input.request.admission.checkpoint) ===
          canonicalSerialize(readInput.checkpoint),
        "checkpoint_catalogue_not_found",
      );
      const session = await readSessionSnapshot(
        transaction,
        operation.sessionId,
        false,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        false,
      );
      ensure(
        observed.operation !== null &&
          observed.operation.state === "committed" &&
          observed.operation.result?.outcome === "checkpoint-captured" &&
          observed.checkpoint?.attempt !== null &&
          observed.checkpoint?.attempt !== undefined &&
          observed.checkpoint.catalogue !== null &&
          canonicalSerialize(observed.checkpoint.catalogue) ===
            canonicalSerialize(
              checkpointCatalogueSnapshotFromRow(
                catalogueRows[0],
                input,
              ),
            ),
        "operation_state_invalid",
      );
      return deepFreeze({
        attempt: checkpointCaptureAttemptRecord(
          input,
          observed.checkpoint.attempt,
          observed.checkpoint.catalogue,
        ),
        catalogue: observed.checkpoint.catalogue,
        operation: observed.operation,
      });
    });
  }

  async claimRestoreDestinationGenerationDispatch(options) {
    const input = restoreGenerationTransitionInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state !== "prepared") {
        const relation = observed.generation;
        ensure(
          relation?.generation !== null &&
            relation?.generation !== undefined,
          "restore_generation_not_authorized",
        );
        ensure(
          relation.generation.generationId === input.generationId &&
            relation.generation.binding.destinationIsolationProofId ===
              input.destinationIsolationProofId,
          "restore_generation_identity_conflict",
        );
        return operationReceipt({
          authorityNow: relation.generation.claimedAt,
          catalogue: relation.source.catalogue,
          dispatchGranted: false,
          generation: relation.generation,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      const relation = observed.generation;
      ensure(
        observed.operation.revision === input.expectedOperationRevision &&
          relation !== null &&
          relation.generation === null &&
          session.document.lifecycle === "ATTACHED" &&
          session.document.lease !== null &&
          session.document.attachment !== null,
        "operation_transition_conflict",
      );
      // V1 needs the restore claim, optional uncertain, and terminal writes.
      // V2 needs the restore claim, optional restore uncertain, two handoff
      // writes, and the launch claim, optional uncertain, and terminal writes:
      // seven in total. Prove that budget before publishing restore authority.
      revisionAfter(
        session.revision,
        input.request.contractVersion ===
          RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2
          ? 7
          : 3,
      );
      const source = await readCommittedCheckpointSource(
        transaction,
        input.request.admission.checkpoint,
        session,
        true,
      );
      const binding = restoreGenerationBinding(input, source, {
        destinationIsolationProofId: input.destinationIsolationProofId,
        generationId: input.generationId,
      });
      if (
        input.request.contractVersion ===
        RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION_V2
      ) {
        const launchIdClaimRows = rowsFromResult(
          await transaction.query(
            INSERT_RESTORE_LAUNCH_ID_CLAIM_QUERY.text,
            [
              input.request.launchIntent.launchAttemptId,
              session.sessionId,
              input.operationId,
              canonicalSerialize(input.request.launchIntent),
              transaction.now,
            ],
          ),
          "operation_state_invalid",
        );
        ensure(
          launchIdClaimRows.length === 1,
          "operation_identity_conflict",
        );
        const launchIdClaim = operationIdClaimSnapshotFromRow(
          launchIdClaimRows[0],
        );
        validateRestoreLaunchIdClaim(launchIdClaim, input, "reserved");
        ensure(
          launchIdClaim.claimedAt === transaction.now,
          "operation_state_invalid",
        );
      }
      // All source and global-ID locks are acquired before the lease clock is
      // sampled, so a blocked claim cannot publish under an expired lease.
      const authorityNow = await readAuthorityClock(transaction);
      ensure(
        timestampMilliseconds(authorityNow) >=
          timestampMilliseconds(transaction.now),
        "session_state_invalid",
      );
      ensure(
        timestampMilliseconds(session.document.lease.expiresAt) >
          timestampMilliseconds(authorityNow),
        "writer_lease_expired",
      );
      try {
        assertStorageMutationMatchesLeaseSnapshot({
          canonicalLease: session.document.lease,
          now: timestampMilliseconds(authorityNow),
          request: input.request.admission.request,
          storageRef: session.document.storageRef,
        });
      } catch {
        fail("operation_transition_conflict");
      }
      const generationRows = rowsFromResult(
        await transaction.query(INSERT_RESTORE_GENERATION_QUERY.text, [
          input.generationId,
          input.operationId,
          session.sessionId,
          input.request.admission.checkpoint.checkpointId,
          canonicalSerialize(binding),
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(
        generationRows.length === 1,
        "restore_generation_identity_conflict",
      );
      const generation = restoreGenerationSnapshotFromRow(
        generationRows[0],
        input,
        source,
      );

      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(
        reservationRows.length === 1,
        "operation_transition_conflict",
      );
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        generation.claimedAt === operation.updatedAt,
        "operation_state_invalid",
      );
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        activePointerFor(input, "starting", "1"),
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        authorityNow,
        catalogue: source.catalogue,
        dispatchGranted: true,
        generation,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async finalizeRestoreDestinationGeneration(options) {
    const input = restoreGenerationFinalizationInput(options);
    ensure(
      input.request.contractVersion ===
        RESTORE_DESTINATION_GENERATION_OPERATION_CONTRACT_VERSION,
      "invalid_operation_request",
    );
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null &&
          observed.reservation !== null &&
          observed.generation?.generation !== null &&
          observed.generation?.generation !== undefined,
        "operation_transition_conflict",
      );
      const completion = canonicalRestoreGenerationCompletion(
        input.completion,
        input,
        observed.generation.source,
        observed.generation.generation.binding,
        "invalid_operation_request",
      );
      if (observed.operation.state === "committed") {
        ensure(
          observed.operation.result?.outcome ===
              "restore-generation-committed" &&
            BigIntConstructor(input.expectedOperationRevision) + 1n ===
              BigIntConstructor(observed.operation.revision) &&
            observed.generation.generation.state === "committed" &&
            observed.generation.generation.document !== null,
          "operation_transition_conflict",
        );
        ensure(
          canonicalSerialize(observed.generation.generation.document) ===
            canonicalSerialize(completion.document),
          "operation_result_conflict",
        );
        return operationReceipt({
          catalogue: observed.generation.source.catalogue,
          finalized: false,
          generation: observed.generation.generation,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        (observed.operation.state === "starting" ||
          observed.operation.state === "uncertain") &&
          observed.operation.revision ===
            input.expectedOperationRevision &&
          observed.generation.generation.state === "authorized" &&
          session.document.lifecycle === "ATTACHED" &&
          session.document.lease !== null &&
          session.document.attachment !== null,
        "operation_transition_conflict",
      );
      nextRevision(session.revision);

      const generationRows = rowsFromResult(
        await transaction.query(COMMIT_RESTORE_GENERATION_QUERY.text, [
          input.operationId,
          canonicalSerialize(completion.document),
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(generationRows.length === 1, "operation_transition_conflict");
      const generation = restoreGenerationSnapshotFromRow(
        generationRows[0],
        input,
        observed.generation.source,
      );
      const result = restoreGenerationTerminalResult(
        input,
        observed.generation.source,
        generation,
      );
      const serializedResult = canonicalSerialize(result);
      const predecessorState = observed.operation.state;
      const operationRows = rowsFromResult(
        await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          serializedResult,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(
        reservationRows.length === 1,
        "operation_transition_conflict",
      );
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        canonicalSerialize(operation.result) === serializedResult &&
          generation.committedAt === operation.updatedAt,
        "operation_result_conflict",
      );
      const nextDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: null,
          lastOperation: lastPointerFor(operation, reservation),
        },
      );
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateLastOperationPointer(
        updatedSession,
        operation,
        reservation,
      );
      return operationReceipt({
        catalogue: observed.generation.source.catalogue,
        finalized: true,
        generation,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
    options,
  ) {
    const input = restoreGenerationLaunchHandoffInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.restore.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input.restore,
        true,
      );
      ensure(
        observed.operation !== null &&
          observed.reservation !== null &&
          observed.generation?.generation !== null &&
          observed.generation?.generation !== undefined,
        "operation_transition_conflict",
      );
      const completion = canonicalRestoreGenerationCompletion(
        input.restore.completion,
        input.restore,
        observed.generation.source,
        observed.generation.generation.binding,
        "invalid_operation_request",
      );
      if (observed.operation.state === "committed") {
        ensure(
          observed.operation.result?.outcome ===
              "restore-generation-committed" &&
            BigIntConstructor(input.restore.expectedOperationRevision) + 1n ===
              BigIntConstructor(observed.operation.revision) &&
            observed.generation.generation.state === "committed" &&
            observed.generation.generation.document !== null,
          "operation_transition_conflict",
        );
        ensure(
          canonicalSerialize(observed.generation.generation.document) ===
            canonicalSerialize(completion.document),
          "operation_result_conflict",
        );
        return readRestoreLaunchHandoffReplay(
          transaction,
          session,
          input,
          observed,
        );
      }
      ensure(
        (observed.operation.state === "starting" ||
          observed.operation.state === "uncertain") &&
          observed.operation.revision ===
            input.restore.expectedOperationRevision &&
          observed.generation.generation.state === "authorized" &&
          session.document.lifecycle === "ATTACHED" &&
          session.document.launch === null &&
          session.document.lease !== null &&
          session.document.attachment !== null,
        "operation_transition_conflict",
      );

      // R+1 commits the generation, R+2 makes the prepared launch active,
      // and the launch claim must retain its complete three-revision budget
      // before either handoff write begins.
      ensure(
        session.revision ===
          restoreTerminalRevisionForLaunchHandoff(
            input.restore,
            observed.operation,
            "operation_state_invalid",
          ),
        "operation_state_invalid",
      );
      revisionAfter(session.revision, 5);

      const reservedLaunchIdClaim = observed.generation.launchIdClaim;
      ensure(reservedLaunchIdClaim !== null, "operation_state_invalid");
      ensure(
        reservedLaunchIdClaim.claimedAt ===
          observed.generation.generation.claimedAt,
        "operation_state_invalid",
      );

      const generationRows = rowsFromResult(
        await transaction.query(COMMIT_RESTORE_GENERATION_QUERY.text, [
          input.restore.operationId,
          canonicalSerialize(completion.document),
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(generationRows.length === 1, "operation_transition_conflict");
      const generation = restoreGenerationSnapshotFromRow(
        generationRows[0],
        input.restore,
        observed.generation.source,
      );
      const restoreResult = restoreGenerationTerminalResult(
        input.restore,
        observed.generation.source,
        generation,
      );
      const serializedRestoreResult = canonicalSerialize(restoreResult);
      const restorePredecessorState = observed.operation.state;
      const restoreOperationRows = rowsFromResult(
        await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
          input.restore.operationId,
          input.restore.expectedOperationRevision,
          serializedRestoreResult,
          transaction.now,
          restorePredecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(
        restoreOperationRows.length === 1,
        "operation_transition_conflict",
      );
      const restoreReservationRows = rowsFromResult(
        await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
          input.restore.operationId,
          transaction.now,
          restorePredecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(
        restoreReservationRows.length === 1,
        "operation_transition_conflict",
      );
      const restoreOperation = operationSnapshotFromRow(
        restoreOperationRows[0],
      );
      const restoreReservation = reservationSnapshotFromRow(
        restoreReservationRows[0],
      );
      validateOperationIdentity(restoreOperation, input.restore);
      validateOperationReservation(
        restoreOperation,
        restoreReservation,
        input.restore,
      );
      ensure(
        canonicalSerialize(restoreOperation.result) ===
            serializedRestoreResult &&
          generation.committedAt === restoreOperation.updatedAt,
        "operation_result_conflict",
      );

      const restoreTerminalDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: null,
          lastOperation: lastPointerFor(
            restoreOperation,
            restoreReservation,
          ),
        },
      );
      const restoreTerminalSession = await updateSessionDocument(
        transaction,
        session,
        input.restore,
        restoreTerminalDocument,
      );
      validateLastOperationPointer(
        restoreTerminalSession,
        restoreOperation,
        restoreReservation,
      );

      const launchInput = writerLaunchAttemptInputForRestoreHandoff(
        input.restore,
        generation,
        restoreTerminalSession,
        input.launch,
      );
      const launchOperationRows = rowsFromResult(
        await transaction.query(INSERT_PRECLAIMED_OPERATION_QUERY.text, [
          launchInput.operationId,
          restoreTerminalSession.sessionId,
          launchInput.kind,
          launchInput.serializedEnvelope,
          transaction.now,
          input.restore.operationId,
          canonicalSerialize(input.restore.request.launchIntent),
        ]),
        "operation_state_invalid",
      );
      ensure(
        launchOperationRows.length === 1,
        "operation_identity_conflict",
      );
      const launchOperation = operationSnapshotFromRow(
        launchOperationRows[0],
      );
      validateOperationIdentity(launchOperation, launchInput);

      const launchReservationPayload = reservationPayload(launchInput);
      const launchReservationRows = rowsFromResult(
        await transaction.query(INSERT_RESERVATION_QUERY.text, [
          launchInput.reservationId,
          launchInput.operationId,
          restoreTerminalSession.sessionId,
          launchInput.kind,
          restoreTerminalSession.revision,
          canonicalSerialize(launchReservationPayload),
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(
        launchReservationRows.length === 1,
        "operation_state_invalid",
      );
      const launchReservation = reservationSnapshotFromRow(
        launchReservationRows[0],
      );
      validateOperationReservation(
        launchOperation,
        launchReservation,
        launchInput,
      );

      const materializedClaimRows = rowsFromResult(
        await transaction.query(
          MATERIALIZE_RESTORE_LAUNCH_ID_CLAIM_QUERY.text,
          [
            launchInput.operationId,
            restoreTerminalSession.sessionId,
            transaction.now,
            input.restore.operationId,
            canonicalSerialize(input.restore.request.launchIntent),
          ],
        ),
        "operation_state_invalid",
      );
      ensure(
        materializedClaimRows.length === 1,
        "operation_transition_conflict",
      );
      const materializedLaunchIdClaim = operationIdClaimSnapshotFromRow(
        materializedClaimRows[0],
      );
      validateRestoreLaunchIdClaim(
        materializedLaunchIdClaim,
        input.restore,
        "materialized",
      );
      ensure(
        materializedLaunchIdClaim.claimedAt ===
            reservedLaunchIdClaim.claimedAt &&
          materializedLaunchIdClaim.materializedAt ===
            launchOperation.createdAt &&
          materializedLaunchIdClaim.materializedAt ===
            restoreOperation.updatedAt,
        "operation_state_invalid",
      );

      const updatedSession = await updateSessionPhase(
        transaction,
        restoreTerminalSession,
        launchInput,
        activePointerFor(launchInput, "prepared", "0"),
      );
      validateActivePointer(
        updatedSession,
        launchOperation,
        launchReservation,
      );
      return restoreLaunchHandoffReceipt({
        generation,
        launchInput,
        launchOperation,
        launchReservation,
        restoreCatalogue: observed.generation.source.catalogue,
        restoreFinalized: true,
        restoreInput: input.restore,
        restoreOperation,
        restoreReservation,
        session: updatedSession,
      });
    });
  }

  async readRestoreDestinationGeneration(options) {
    const readInput = restoreGenerationReadInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const generationRows = rowsFromResult(
        await transaction.query(READ_RESTORE_GENERATION_BY_ID_QUERY.text, [
          readInput.generationId,
        ]),
        "operation_state_invalid",
      );
      ensure(
        generationRows.length === 1,
        "restore_generation_not_authorized",
      );
      const identity = restoreGenerationIdentityFromRow(generationRows[0]);
      ensure(
        identity.generationId === readInput.generationId &&
          identity.checkpointId === readInput.checkpoint.checkpointId &&
          identity.sessionId === readInput.checkpoint.sessionId,
        "restore_generation_not_authorized",
      );
      const operation = await readOperationSnapshot(
        transaction,
        identity.operationId,
        false,
      );
      ensure(
        operation !== null &&
          operation.kind ===
            RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
        "restore_generation_not_authorized",
      );
      const input = restoreGenerationInput(
        inputForOperation(operation),
        OPERATION_INPUT_KEYS,
        "operation_state_invalid",
      );
      ensure(
        canonicalSerialize(input.request.admission.checkpoint) ===
            canonicalSerialize(readInput.checkpoint) &&
          canonicalSerialize(input.request.admission.request) ===
            canonicalSerialize(readInput.request),
        "restore_generation_not_authorized",
      );
      const session = await readSessionSnapshot(
        transaction,
        operation.sessionId,
        false,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        false,
      );
      ensure(
        observed.operation !== null &&
          observed.reservation !== null &&
          observed.generation?.generation !== null &&
          observed.generation?.generation !== undefined &&
          observed.generation.generation.generationId ===
            readInput.generationId &&
          observed.operation.state !== "prepared",
        "restore_generation_not_authorized",
      );
      return operationReceipt({
        catalogue: observed.generation.source.catalogue,
        generation: observed.generation.generation,
        operation: observed.operation,
        reservation: observed.reservation,
        session,
        status: observed.generation.generation.state,
      });
    });
  }

  async claimRestoreAttachmentActivationDispatch(options) {
    const input = restoreAttachmentActivationTransitionInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state !== "prepared") {
        ensure(
          observed.activation !== null &&
            observed.activation.activationRequest !== null,
          "operation_transition_conflict",
        );
        return operationReceipt({
          activationRequest: observed.activation.activationRequest,
          dispatchGranted: false,
          generation: observed.activation.generation.generation,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.revision === input.expectedOperationRevision &&
          observed.activation !== null &&
          observed.activation.generation !== null &&
          observed.activation.predecessor !== null &&
          observed.activation.launchIdClaim === null &&
          session.document.lifecycle === "DETACHED" &&
          session.document.lease === null &&
          session.document.attachment === null &&
          session.document.launch === null,
        "operation_transition_conflict",
      );
      revisionAfter(session.revision, 7);
      const fencingEpoch = nextWriterEpoch(session.document.writerEpoch);

      const claimRows = rowsFromResult(
        await transaction.query(
          INSERT_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY.text,
          [
            input.request.launchIntent.launchAttemptId,
            session.sessionId,
            input.operationId,
            canonicalSerialize(input.request.launchIntent),
            transaction.now,
          ],
        ),
        "operation_state_invalid",
      );
      ensure(claimRows.length === 1, "operation_identity_conflict");
      const launchIdClaim = operationIdClaimSnapshotFromRow(claimRows[0]);
      validateRestoreAttachmentActivationLaunchIdClaim(
        launchIdClaim,
        input,
        "reserved",
      );

      const authorityNow = await readAuthorityClock(transaction);
      ensure(
        timestampMilliseconds(authorityNow) >=
          timestampMilliseconds(transaction.now),
        "session_state_invalid",
      );
      const lease = restoreAttachmentActivationLeaseFor(
        input,
        authorityNow,
        fencingEpoch,
        "operation_state_invalid",
      );
      const activationRequest = restoreAttachmentActivationRequestFor(
        input,
        observed.activation.generation.generation,
        lease,
        "operation_state_invalid",
      );

      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(
        reservationRows.length === 1,
        "operation_transition_conflict",
      );
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        launchIdClaim.claimedAt === operation.updatedAt,
        "operation_state_invalid",
      );
      const nextDocument = documentWithAuthorityState(session.document, {
        activeOperation: activePointerFor(input, "starting", "1"),
        attachment: null,
        lease,
        lifecycle: "ATTACHING",
        writerEpoch: fencingEpoch,
      });
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        activationRequest,
        authorityNow,
        dispatchGranted: true,
        generation: observed.activation.generation.generation,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
    options,
  ) {
    const input = restoreAttachmentActivationFinalizationInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null &&
          observed.reservation !== null &&
          observed.activation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "committed") {
        ensure(
          observed.operation.result?.outcome ===
              "restore-attachment-activated" &&
            BigIntConstructor(input.expectedOperationRevision) + 1n ===
              BigIntConstructor(observed.operation.revision),
          "operation_transition_conflict",
        );
        const candidate = canonicalRestoreAttachmentActivationProviderResult(
          input.activationResult,
          observed.operation.result.activationRequest,
          "invalid_operation_request",
        );
        ensure(
          canonicalSerialize(candidate) ===
            canonicalSerialize(observed.operation.result.activationResult),
          "operation_result_conflict",
        );
        return readRestoreAttachmentActivationHandoffReplay(
          transaction,
          session,
          input,
          observed,
        );
      }
      ensure(
        (observed.operation.state === "starting" ||
          observed.operation.state === "uncertain") &&
          observed.operation.revision === input.expectedOperationRevision &&
          observed.activation.activationRequest !== null &&
          observed.activation.generation !== null &&
          observed.activation.launchIdClaim !== null &&
          session.document.lifecycle === "ATTACHING" &&
          session.document.lease !== null &&
          session.document.attachment === null,
        "operation_transition_conflict",
      );
      revisionAfter(session.revision, 5);
      const activationRequest = observed.activation.activationRequest;
      const activationResult =
        canonicalRestoreAttachmentActivationProviderResult(
          input.activationResult,
          activationRequest,
          "invalid_operation_request",
        );
      const result = restoreAttachmentActivationTerminalResult(
        input,
        activationRequest,
        activationResult,
        "invalid_operation_request",
      );
      const serializedResult = canonicalSerialize(result);
      const predecessorState = observed.operation.state;
      const operationRows = rowsFromResult(
        await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          serializedResult,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(
        reservationRows.length === 1,
        "operation_transition_conflict",
      );
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        canonicalSerialize(operation.result) === serializedResult,
        "operation_result_conflict",
      );

      const terminalDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: null,
          attachment: activationResult.attachment,
          lastOperation: lastPointerFor(operation, reservation),
          lease: activationRequest.lease,
          lifecycle: "ATTACHED",
          writerEpoch: activationRequest.lease.fencingEpoch,
        },
      );
      const terminalSession = await updateSessionDocument(
        transaction,
        session,
        input,
        terminalDocument,
      );
      validateLastOperationPointer(terminalSession, operation, reservation);

      const launchInput =
        writerLaunchAttemptInputForRestoreActivationHandoff(
          input,
          observed.activation.generation.generation,
          terminalSession,
        );
      const launchOperationRows = rowsFromResult(
        await transaction.query(
          INSERT_PRECLAIMED_RESTORE_ACTIVATION_LAUNCH_QUERY.text,
          [
            launchInput.operationId,
            terminalSession.sessionId,
            launchInput.kind,
            launchInput.serializedEnvelope,
            transaction.now,
            input.operationId,
            canonicalSerialize(input.request.launchIntent),
          ],
        ),
        "operation_state_invalid",
      );
      ensure(
        launchOperationRows.length === 1,
        "operation_identity_conflict",
      );
      const launchOperation = operationSnapshotFromRow(
        launchOperationRows[0],
      );
      validateOperationIdentity(launchOperation, launchInput);

      const launchReservationRows = rowsFromResult(
        await transaction.query(INSERT_RESERVATION_QUERY.text, [
          launchInput.reservationId,
          launchInput.operationId,
          terminalSession.sessionId,
          launchInput.kind,
          terminalSession.revision,
          canonicalSerialize(reservationPayload(launchInput)),
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(
        launchReservationRows.length === 1,
        "operation_state_invalid",
      );
      const launchReservation = reservationSnapshotFromRow(
        launchReservationRows[0],
      );
      validateOperationReservation(
        launchOperation,
        launchReservation,
        launchInput,
      );

      const materializedRows = rowsFromResult(
        await transaction.query(
          MATERIALIZE_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY.text,
          [
            launchInput.operationId,
            terminalSession.sessionId,
            transaction.now,
            input.operationId,
            canonicalSerialize(input.request.launchIntent),
          ],
        ),
        "operation_state_invalid",
      );
      ensure(
        materializedRows.length === 1,
        "operation_transition_conflict",
      );
      const materializedClaim = operationIdClaimSnapshotFromRow(
        materializedRows[0],
      );
      validateRestoreAttachmentActivationLaunchIdClaim(
        materializedClaim,
        input,
        "materialized",
      );
      ensure(
        materializedClaim.claimedAt ===
            observed.activation.launchIdClaim.claimedAt &&
          materializedClaim.materializedAt === operation.updatedAt &&
          materializedClaim.materializedAt === launchOperation.createdAt,
        "operation_state_invalid",
      );

      const updatedSession = await updateSessionPhase(
        transaction,
        terminalSession,
        launchInput,
        activePointerFor(launchInput, "prepared", "0"),
      );
      validateActivePointer(
        updatedSession,
        launchOperation,
        launchReservation,
      );
      return restoreAttachmentActivationHandoffReceipt({
        activationFinalized: true,
        activationInput: input,
        activationOperation: operation,
        activationReservation: reservation,
        generation: observed.activation.generation.generation,
        launchInput,
        launchOperation,
        launchReservation,
        session: updatedSession,
      });
    });
  }

  async readRestoreAttachmentActivation(options) {
    const normalized = exactPlainObject(
      options,
      RESTORE_ATTACHMENT_ACTIVATION_READ_KEYS,
      "invalid_operation_request",
    );
    const operationId = canonicalOpaqueId(
      normalized.operationId,
      128,
      "invalid_operation_request",
    );
    return runSerializable(this.#store, async (transaction) => {
      const operation = await readOperationSnapshot(
        transaction,
        operationId,
        false,
      );
      ensure(
        operation !== null &&
          operation.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
        "restore_attachment_activation_not_authorized",
      );
      const input = restoreAttachmentActivationInput(
        inputForOperation(operation),
        OPERATION_INPUT_KEYS,
        "operation_state_invalid",
      );
      const session = await readSessionSnapshot(
        transaction,
        operation.sessionId,
        false,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        false,
      );
      ensure(
        observed.operation !== null &&
          observed.reservation !== null &&
          observed.activation !== null,
        "operation_transition_conflict",
      );
      return operationReceipt({
        activationRequest: observed.activation.activationRequest,
        generation: observed.activation.generation?.generation ?? null,
        operation: observed.operation,
        reservation: observed.reservation,
        session,
      });
    });
  }

  async claimWriterLaunchAttemptDispatch(options) {
    const input = writerLaunchAttemptTransitionInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null &&
          observed.reservation !== null &&
          observed.launchAttempt !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state !== "prepared") {
        return operationReceipt({
          attempt: writerLaunchAttemptRecord(input, observed.operation),
          dispatchGranted: false,
          generation:
            observed.launchAttempt.generation?.generation ?? null,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.revision === input.expectedOperationRevision &&
          session.document.lifecycle === "ATTACHED" &&
          session.document.launch === null &&
          session.document.lease !== null &&
          session.document.attachment !== null &&
          canonicalSerialize(
            canonicalJsonObject(
              session.document.lease,
              "operation_transition_conflict",
            ),
          ) ===
            canonicalSerialize(input.request.lease) &&
          canonicalSerialize(
            canonicalJsonObject(
              session.document.attachment,
              "operation_transition_conflict",
            ),
          ) ===
            canonicalSerialize(input.request.attachment) &&
          observed.launchAttempt.generation === null,
        "operation_transition_conflict",
      );
      revisionAfter(session.revision, 3);
      const generation = await readCommittedRestoreGenerationForLaunch(
        transaction,
        input.request.generation,
        session,
        true,
      );
      await validateWriterLaunchGenerationAttachmentRelation(
        transaction,
        input,
        generation,
        session,
        true,
      );
      // Read the non-transactional authority clock only after every
      // generation relation lock has been acquired. Otherwise a wait on the
      // generation row could cross lease expiry after an earlier successful
      // time check and still grant dispatch.
      const authorityNow = await readAuthorityClock(transaction);
      ensure(
        timestampMilliseconds(authorityNow) >=
          timestampMilliseconds(transaction.now),
        "session_state_invalid",
      );
      ensure(
        timestampMilliseconds(session.document.lease.expiresAt) >
          timestampMilliseconds(authorityNow),
        "writer_lease_expired",
      );
      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(
        reservationRows.length === 1,
        "operation_transition_conflict",
      );
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        activePointerFor(input, "starting", "1"),
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        attempt: writerLaunchAttemptRecord(input, operation),
        authorityNow,
        dispatchGranted: true,
        generation: generation.generation,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async finalizeWriterLaunchAttemptStarted(options) {
    return finalizeWriterLaunchAttempt(this.#store, options, ["started"]);
  }

  async finalizeWriterLaunchAttemptStopped(options) {
    return finalizeWriterLaunchAttempt(this.#store, options, [
      "not-started",
      "complete-stopped",
    ]);
  }

  async claimWriterLaunchStopDispatch(options) {
    const input = writerLaunchStopTransitionInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null &&
          observed.reservation !== null &&
          observed.launchStop !== null,
        "operation_transition_conflict",
      );
      const usesClaimToken =
        input.request.contractVersion ===
        WRITER_LAUNCH_STOP_OPERATION_CONTRACT_VERSION_V2;
      const claimTokenMatched =
        usesClaimToken &&
        writerLaunchStopClaimSha256(
          input.claimToken,
          "invalid_operation_request",
        ) === input.request.dispatchClaimSha256;
      if (observed.operation.state !== "prepared") {
        return operationReceipt({
          ...(usesClaimToken ? { claimTokenMatched } : {}),
          dispatchGranted: false,
          launch: currentWriterLaunchForAttempt(
            session,
            input.request.launch.launchAttemptId,
          ),
          operation: observed.operation,
          reservation: observed.reservation,
          session,
          stop: writerLaunchStopRecord(input, observed.operation),
        });
      }
      ensure(
        observed.operation.revision === input.expectedOperationRevision &&
          session.document.lifecycle === "ATTACHED" &&
          session.document.launch !== null &&
          canonicalSerialize(session.document.launch) ===
            canonicalSerialize(input.request.launch),
        "operation_transition_conflict",
      );
      if (usesClaimToken && !claimTokenMatched) {
        return operationReceipt({
          claimTokenMatched: false,
          dispatchGranted: false,
          launch: session.document.launch,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
          stop: writerLaunchStopRecord(input, observed.operation),
        });
      }
      revisionAfter(session.revision, 3);
      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        activePointerFor(input, "starting", "1"),
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        ...(usesClaimToken ? { claimTokenMatched: true } : {}),
        dispatchGranted: true,
        launch: updatedSession.document.launch,
        operation,
        reservation,
        session: updatedSession,
        stop: writerLaunchStopRecord(input, operation),
      });
    });
  }

  async finalizeWriterLaunchStopped(options) {
    return finalizeWriterLaunchStop(this.#store, options);
  }

  async readWriterLaunchAttempt(options) {
    const readInput = writerLaunchAttemptReadInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const operation = await readOperationSnapshot(
        transaction,
        readInput.operationId,
        false,
      );
      ensure(
        operation !== null &&
          operation.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
        "writer_launch_attempt_not_authorized",
      );
      const input = writerLaunchAttemptInput(
        inputForOperation(operation),
        OPERATION_INPUT_KEYS,
        "operation_state_invalid",
      );
      const session = await readSessionSnapshot(
        transaction,
        operation.sessionId,
        false,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        false,
      );
      ensure(
        observed.operation !== null &&
          observed.reservation !== null &&
          observed.launchAttempt !== null,
        "writer_launch_attempt_not_authorized",
      );
      const launch =
        session.document.launch?.launchAttemptId === input.operationId
          ? session.document.launch
          : null;
      return operationReceipt({
        attempt: writerLaunchAttemptRecord(input, observed.operation),
        launch,
        operation: observed.operation,
        reservation: observed.reservation,
        session,
      });
    });
  }

  async claimWriterAttachmentDispatch(options) {
    const input = operationInputWithExpectedRevision(options, "0");
    writerAttachmentRequest(input);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state !== "prepared") {
        const lease =
          observed.operation.state === "starting" ||
          observed.operation.state === "uncertain"
            ? session.document.lease
            : observed.operation.result?.outcome === "writer-attached"
            ? observed.operation.result.lease
            : null;
        const evidence =
          lease === null
            ? {}
            : {
                lease,
                mutationRequest: attachMutationRequestFor(
                  input,
                  lease,
                  "operation_state_invalid",
                ),
              };
        return operationReceipt({
          dispatchGranted: false,
          ...evidence,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.revision === input.expectedOperationRevision,
        "operation_transition_conflict",
      );
      revisionAfter(session.revision, 3);
      const fencingEpoch = nextWriterEpoch(
        session.document.writerEpoch,
      );
      const authorityNow = await readAuthorityClock(transaction);
      ensure(
        timestampMilliseconds(authorityNow) >=
          timestampMilliseconds(transaction.now),
        "session_state_invalid",
      );
      const lease = writerLeaseFor(
        input,
        authorityNow,
        fencingEpoch,
        "operation_state_invalid",
      );
      const mutationRequest = attachMutationRequestFor(
        input,
        lease,
        "operation_state_invalid",
      );
      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(
        reservationRows.length === 1,
        "operation_transition_conflict",
      );
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      const nextDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: activePointerFor(
            input,
            "starting",
            "1",
          ),
          attachment: null,
          lease,
          lifecycle: "ATTACHING",
          writerEpoch: fencingEpoch,
        },
      );
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        authorityNow,
        dispatchGranted: true,
        lease,
        mutationRequest,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async finalizeWriterAttachment(options) {
    const input = writerAttachmentFinalizationInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "committed") {
        ensure(
          observed.operation.result?.outcome === "writer-attached" &&
            BigIntConstructor(input.expectedOperationRevision) + 1n ===
              BigIntConstructor(observed.operation.revision),
          "operation_transition_conflict",
        );
        const candidate = canonicalWriterAttachmentResult(
          {
            attachment: input.attachment,
            input,
            lease: observed.operation.result.lease,
            mutationResult: input.mutationResult,
          },
          "invalid_operation_request",
        );
        ensure(
          canonicalSerialize(candidate) ===
            canonicalSerialize(observed.operation.result),
          "operation_result_conflict",
        );
        return operationReceipt({
          finalized: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        (observed.operation.state === "starting" ||
          observed.operation.state === "uncertain") &&
          observed.operation.revision ===
            input.expectedOperationRevision &&
          session.document.lifecycle === "ATTACHING" &&
          session.document.lease !== null,
        "operation_transition_conflict",
      );
      nextRevision(session.revision);
      const result = canonicalWriterAttachmentResult(
        {
          attachment: input.attachment,
          input,
          lease: session.document.lease,
          mutationResult: input.mutationResult,
        },
        "invalid_operation_request",
      );
      const serializedResult = canonicalSerialize(result);
      const predecessorState = observed.operation.state;
      const operationRows = rowsFromResult(
        await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          serializedResult,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(
        reservationRows.length === 1,
        "operation_transition_conflict",
      );
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        canonicalSerialize(operation.result) === serializedResult,
        "operation_result_conflict",
      );
      const nextDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: null,
          attachment: result.attachment,
          lastOperation: lastPointerFor(operation, reservation),
          lease: result.lease,
          lifecycle: "ATTACHED",
        },
      );
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateLastOperationPointer(
        updatedSession,
        operation,
        reservation,
      );
      return operationReceipt({
        finalized: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async renewWriterLease(options) {
    const input = canonicalOperationInput(options);
    const renewalRequest = writerLeaseRenewalRequest(input);
    const renew = () =>
      runSerializable(this.#store, async (transaction) => {
        const session = await readSessionSnapshot(
          transaction,
          input.expectedSession.sessionId,
          true,
        );
        const observed = await readRequestedOperation(
          transaction,
          session,
          input,
          true,
        );
        if (observed.operation !== null) {
          ensure(
            observed.operation.state === "committed" &&
              observed.operation.result?.outcome ===
                "writer-lease-renewed",
            "operation_transition_conflict",
          );
          return operationReceipt({
            operation: observed.operation,
            renewed: false,
            reservation: observed.reservation,
            session,
          });
        }
        ensure(observed.active === null, "session_operation_conflict");
        ensureExactExpectedSession(session, input.expectedSession);
        ensure(
          session.document.lifecycle === "ATTACHED" &&
            session.document.lease !== null &&
            session.document.attachment !== null,
          "operation_transition_conflict",
        );
        nextRevision(session.revision);
        const authorityNow = await readAuthorityClock(transaction);
        ensure(
          timestampMilliseconds(authorityNow) >=
            timestampMilliseconds(transaction.now),
          "session_state_invalid",
        );
        const previousLease = session.document.lease;
        ensure(
          timestampMilliseconds(previousLease.expiresAt) >
            timestampMilliseconds(authorityNow),
          "writer_lease_expired",
        );
        const expiresAt = timestampAfter(
          authorityNow,
          renewalRequest.leaseDurationMilliseconds,
          "invalid_operation_request",
        );
        ensure(
          timestampMilliseconds(expiresAt) >
            timestampMilliseconds(previousLease.expiresAt),
          "writer_lease_not_extended",
        );
        const lease = canonicalLeaseGrant(
          {
            ...previousLease,
            expiresAt,
          },
          "session_state_invalid",
        );
        const result = canonicalWriterLeaseRenewalResult(
          {
            attachment: session.document.attachment,
            input,
            lease,
          },
          "operation_state_invalid",
        );
        const serializedResult = canonicalSerialize(result);
        const operationRows = rowsFromResult(
          await transaction.query(
            INSERT_COMMITTED_OPERATION_QUERY.text,
            [
              input.operationId,
              session.sessionId,
              input.kind,
              input.serializedEnvelope,
              serializedResult,
              transaction.now,
            ],
          ),
          "operation_state_invalid",
        );
        if (operationRows.length === 0) {
          const existing = await readOperationSnapshot(
            transaction,
            input.operationId,
            true,
          );
          if (existing === null) {
            const conflictingClaim = await readOperationIdClaim(
              transaction,
              input.operationId,
              false,
            );
            if (conflictingClaim === null) {
              throw OPERATION_VISIBILITY_RETRY;
            }
            fail("operation_identity_conflict");
          }
          validateOperationIdentity(existing, input);
          fail("operation_state_invalid");
        }
        const operation = operationSnapshotFromRow(operationRows[0]);
        validateOperationIdentity(operation, input);
        ensure(
          canonicalSerialize(operation.result) === serializedResult,
          "operation_result_conflict",
        );
        const payload = reservationPayload(input);
        const reservationRows = rowsFromResult(
          await transaction.query(
            INSERT_RELEASED_RESERVATION_QUERY.text,
            [
              input.reservationId,
              input.operationId,
              session.sessionId,
              input.kind,
              session.revision,
              canonicalSerialize(payload),
              transaction.now,
            ],
          ),
          "operation_state_invalid",
        );
        ensure(reservationRows.length === 1, "operation_state_invalid");
        const reservation = reservationSnapshotFromRow(
          reservationRows[0],
        );
        validateOperationReservation(operation, reservation, input);
        const nextDocument = documentWithAuthorityState(
          session.document,
          {
            activeOperation: null,
            attachment: result.attachment,
            lastOperation: lastPointerFor(operation, reservation),
            lease: result.lease,
            lifecycle: "ATTACHED",
          },
        );
        const updatedSession = await updateSessionDocument(
          transaction,
          session,
          input,
          nextDocument,
        );
        validateLastOperationPointer(
          updatedSession,
          operation,
          reservation,
        );
        return operationReceipt({
          authorityNow,
          operation,
          renewed: true,
          reservation,
          session: updatedSession,
        });
      });
    try {
      return await renew();
    } catch (error) {
      if (error !== OPERATION_VISIBILITY_RETRY) {
        throw error;
      }
    }
    try {
      return await renew();
    } catch (error) {
      if (error === OPERATION_VISIBILITY_RETRY) {
        fail("operation_state_invalid");
      }
      throw error;
    }
  }

  async claimWriterReleaseDispatch(options) {
    const input = operationInputWithExpectedRevision(options, "0");
    writerReleaseRequest(input);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state !== "prepared") {
        const lease =
          observed.operation.state === "starting" ||
          observed.operation.state === "uncertain"
            ? session.document.lease
            : observed.operation.result?.lease ?? null;
        const evidence =
          lease === null
            ? {}
            : {
                lease,
                mutationRequest: detachMutationRequestFor(
                  input,
                  lease,
                  "operation_state_invalid",
                ),
              };
        return operationReceipt({
          dispatchGranted: false,
          ...evidence,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.revision === input.expectedOperationRevision,
        "operation_transition_conflict",
      );
      revisionAfter(session.revision, 3);
      const lease = session.document.lease;
      const attachment = session.document.attachment;
      ensure(lease !== null && attachment !== null, "session_state_invalid");
      const mutationRequest = detachMutationRequestFor(
        input,
        lease,
        "operation_state_invalid",
      );
      ensure(
        mutationRequest.target.attachmentId === attachment.attachmentId,
        "operation_state_invalid",
      );
      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      const nextDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: activePointerFor(input, "starting", "1"),
          lifecycle: "RELEASING",
        },
      );
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        dispatchGranted: true,
        lease,
        mutationRequest,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async finalizeWriterRelease(options) {
    const input = writerReleaseFinalizationInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "committed") {
        ensure(
          observed.operation.result?.outcome === "writer-released" &&
            BigIntConstructor(input.expectedOperationRevision) + 1n ===
              BigIntConstructor(observed.operation.revision),
          "operation_transition_conflict",
        );
        const candidate = canonicalWriterReleaseResult(
          {
            attachment: observed.operation.result.attachment,
            input,
            lease: observed.operation.result.lease,
            mutationResult: input.mutationResult,
          },
          "invalid_operation_request",
        );
        ensure(
          canonicalSerialize(candidate) ===
            canonicalSerialize(observed.operation.result),
          "operation_result_conflict",
        );
        return operationReceipt({
          finalized: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        (observed.operation.state === "starting" ||
          observed.operation.state === "uncertain") &&
          observed.operation.revision ===
            input.expectedOperationRevision &&
          session.document.lifecycle === "RELEASING" &&
          session.document.lease !== null &&
          session.document.attachment !== null,
        "operation_transition_conflict",
      );
      nextRevision(session.revision);
      const result = canonicalWriterReleaseResult(
        {
          attachment: session.document.attachment,
          input,
          lease: session.document.lease,
          mutationResult: input.mutationResult,
        },
        "invalid_operation_request",
      );
      const serializedResult = canonicalSerialize(result);
      const predecessorState = observed.operation.state;
      const operationRows = rowsFromResult(
        await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          serializedResult,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        canonicalSerialize(operation.result) === serializedResult,
        "operation_result_conflict",
      );
      const nextDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: null,
          attachment: null,
          lastOperation: lastPointerFor(operation, reservation),
          launch: null,
          lease: null,
          lifecycle: "DETACHED",
        },
      );
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateLastOperationPointer(
        updatedSession,
        operation,
        reservation,
      );
      return operationReceipt({
        finalized: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async claimWriterForceFenceDispatch(options) {
    const input = operationInputWithExpectedRevision(options, "0");
    writerForceFenceOperationRequest(input);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state !== "prepared") {
        const writerEpoch =
          observed.operation.state === "starting" ||
          observed.operation.state === "uncertain"
            ? session.document.writerEpoch
            : observed.operation.result?.writerEpoch ?? null;
        const evidence =
          writerEpoch === null
            ? {}
            : {
                fenceRequest: forceFenceRequestFor(
                  input,
                  writerEpoch,
                  "operation_state_invalid",
                ),
                writerEpoch,
              };
        return operationReceipt({
          dispatchGranted: false,
          ...evidence,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.revision === input.expectedOperationRevision,
        "operation_transition_conflict",
      );
      validateForceFenceTargetSource(
        input,
        observed.terminal,
        "operation_transition_conflict",
      );
      revisionAfter(session.revision, 3);
      const writerEpoch = nextWriterEpoch(session.document.writerEpoch);
      const fenceRequest = forceFenceRequestFor(
        input,
        writerEpoch,
        "operation_state_invalid",
      );
      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      const nextDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: activePointerFor(input, "starting", "1"),
          lifecycle: "FENCING",
          writerEpoch,
        },
      );
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        dispatchGranted: true,
        fenceRequest,
        operation,
        reservation,
        session: updatedSession,
        writerEpoch,
      });
    });
  }

  async finalizeWriterForceFence(options) {
    const input = writerForceFenceFinalizationInput(options);
    ensure(
      input.expectedSession.document.backendCapabilities.fencing !==
        "manual",
      "writer_fence_unsupported",
    );
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "committed") {
        ensure(
          observed.operation.result?.outcome === "writer-fenced" &&
            BigIntConstructor(input.expectedOperationRevision) + 1n ===
              BigIntConstructor(observed.operation.revision),
          "operation_transition_conflict",
        );
        const candidate = canonicalWriterForceFenceResult(
          {
            attachment: observed.operation.result.attachment,
            fenceResult: input.fenceResult,
            input,
            lease: observed.operation.result.lease,
            writerEpoch: observed.operation.result.writerEpoch,
          },
          "invalid_operation_request",
        );
        ensure(
          canonicalSerialize(candidate) ===
            canonicalSerialize(observed.operation.result),
          "operation_result_conflict",
        );
        return operationReceipt({
          finalized: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        (observed.operation.state === "starting" ||
          observed.operation.state === "uncertain") &&
          observed.operation.revision ===
            input.expectedOperationRevision &&
          session.document.lifecycle === "FENCING" &&
          session.document.lease !== null,
        "operation_transition_conflict",
      );
      nextRevision(session.revision);
      const result = canonicalWriterForceFenceResult(
        {
          attachment: session.document.attachment,
          fenceResult: input.fenceResult,
          input,
          lease: session.document.lease,
          writerEpoch: session.document.writerEpoch,
        },
        "invalid_operation_request",
      );
      const serializedResult = canonicalSerialize(result);
      const predecessorState = observed.operation.state;
      const operationRows = rowsFromResult(
        await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          serializedResult,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
          predecessorState,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        canonicalSerialize(operation.result) === serializedResult,
        "operation_result_conflict",
      );
      const nextDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: null,
          attachment: null,
          lastOperation: lastPointerFor(operation, reservation),
          launch: null,
          lease: null,
          lifecycle: "DETACHED",
          writerEpoch: result.writerEpoch,
        },
      );
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateLastOperationPointer(
        updatedSession,
        operation,
        reservation,
      );
      return operationReceipt({
        finalized: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async finalizeWriterOperationBlocked(options) {
    const input = writerBlockedFinalizationInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "committed") {
        ensure(
          observed.operation.result?.outcome === "writer-blocked" &&
            observed.operation.revision === "3",
          "operation_transition_conflict",
        );
        const candidate = canonicalWriterBlockedResult(
          {
            attachment: observed.operation.result.attachment,
            input,
            lease: observed.operation.result.lease,
            reason: input.reason,
            writerEpoch: observed.operation.result.writerEpoch,
          },
          "invalid_operation_request",
        );
        ensure(
          canonicalSerialize(candidate) ===
            canonicalSerialize(observed.operation.result),
          "operation_result_conflict",
        );
        return operationReceipt({
          finalized: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      const expectedLifecycle =
        input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND
          ? "ATTACHING"
          : input.kind === WRITER_RELEASE_OPERATION_KIND
            ? "RELEASING"
            : "FENCING";
      ensure(
        observed.operation.state === "uncertain" &&
          observed.operation.revision ===
            input.expectedOperationRevision &&
          session.document.lifecycle === expectedLifecycle &&
          session.document.lease !== null,
        "operation_transition_conflict",
      );
      nextRevision(session.revision);
      const result = canonicalWriterBlockedResult(
        {
          attachment: session.document.attachment,
          input,
          lease: session.document.lease,
          reason: input.reason,
          writerEpoch: session.document.writerEpoch,
        },
        "invalid_operation_request",
      );
      const serializedResult = canonicalSerialize(result);
      const operationRows = rowsFromResult(
        await transaction.query(COMMIT_ACTIVE_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          serializedResult,
          transaction.now,
          "uncertain",
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(RELEASE_ACTIVE_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
          "uncertain",
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(
        reservationRows[0],
      );
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        canonicalSerialize(operation.result) === serializedResult,
        "operation_result_conflict",
      );
      const nextDocument = documentWithAuthorityState(
        session.document,
        {
          activeOperation: null,
          lastOperation: lastPointerFor(operation, reservation),
          lifecycle: "BLOCKED",
        },
      );
      const updatedSession = await updateSessionDocument(
        transaction,
        session,
        input,
        nextDocument,
      );
      validateLastOperationPointer(
        updatedSession,
        operation,
        reservation,
      );
      return operationReceipt({
        finalized: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async markOperationUncertain(options) {
    const input = operationInputWithExpectedRevision(options, "1");
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "uncertain") {
        return operationReceipt({
          changed: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.state === "starting" &&
          observed.operation.revision === input.expectedOperationRevision,
        "operation_transition_conflict",
      );
      revisionAfter(
        session.revision,
        input.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND ||
          input.kind === WRITER_RELEASE_OPERATION_KIND ||
          input.kind === WRITER_FORCE_FENCE_OPERATION_KIND ||
          input.kind === CHECKPOINT_CAPTURE_OPERATION_KIND ||
          input.kind === RESTORE_DESTINATION_GENERATION_OPERATION_KIND ||
          input.kind === RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND ||
          input.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND ||
          input.kind === WRITER_LAUNCH_STOP_OPERATION_KIND
          ? 2
          : 1,
      );
      const operationRows = rowsFromResult(
        await transaction.query(UNCERTAIN_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(UNCERTAIN_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        activePointerFor(input, "uncertain", "2"),
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        changed: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async cancelPreparedOperation(options) {
    const input = cancellationInput(options);
    ensure(
      input.expectedSession.document.lifecycle !== "BLOCKED",
      "operation_transition_conflict",
    );
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "committed") {
        ensure(
          canonicalSerialize(observed.operation.result) ===
            canonicalSerialize(input.result),
          "operation_result_conflict",
        );
        return operationReceipt({
          cancelled: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.state === "prepared" &&
          observed.operation.revision === input.expectedOperationRevision,
        "operation_transition_conflict",
      );
      const atomicRestoreLaunchHandoff =
        isAtomicRestoreLaunchHandoff(session, observed) ||
        isAtomicRestoreAttachmentActivationLaunchHandoff(
          session,
          observed,
        );
      let mutationTimestamp = transaction.now;
      if (atomicRestoreLaunchHandoff) {
        ensure(
          input.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND &&
            input.reason ===
              WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
          "operation_transition_conflict",
        );
        // The session and active launch rows are locked, and the immutable
        // terminal restore/generation/ID-claim provenance has been fully
        // revalidated, before sampling the non-transactional clock. Claim and
        // expiry cancellation therefore form complementary boundaries around
        // the same bound lease without a dispatch-versus-cancel race.
        const authorityNow = await readAuthorityClock(transaction);
        ensure(
          timestampMilliseconds(authorityNow) >=
            timestampMilliseconds(transaction.now),
          "session_state_invalid",
        );
        ensure(
          timestampMilliseconds(input.request.lease.expiresAt) <=
            timestampMilliseconds(authorityNow),
          "operation_transition_conflict",
        );
        mutationTimestamp = authorityNow;
      }
      nextRevision(session.revision);
      const operationRows = rowsFromResult(
        await transaction.query(CANCEL_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          input.serializedResult,
          mutationTimestamp,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(RELEASE_RESERVATION_QUERY.text, [
          input.operationId,
          mutationTimestamp,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        canonicalSerialize(operation.result) ===
          canonicalSerialize(input.result),
        "operation_result_conflict",
      );
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        null,
        lastPointerFor(operation, reservation),
        mutationTimestamp,
      );
      validateLastOperationPointer(
        updatedSession,
        operation,
        reservation,
      );
      return operationReceipt({
        cancelled: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }
}
