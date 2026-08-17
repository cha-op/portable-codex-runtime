import { execFile } from "node:child_process";
import { Buffer } from "node:buffer";
import { readFile } from "node:fs/promises";
import { basename, dirname } from "node:path";
import { TextDecoder, promisify, types as utilTypes } from "node:util";

const execFileAsync = promisify(execFile);
const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const arrayJoinIntrinsic = Array.prototype.join;
const arrayPushIntrinsic = Array.prototype.push;
const bufferByteLength = Buffer.byteLength;
const bufferFrom = Buffer.from;
const bufferIsBuffer = Buffer.isBuffer;
const jsonParse = JSON.parse;
const jsonStringify = JSON.stringify;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperties = Object.defineProperties;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetOwnPropertyDescriptors = Object.getOwnPropertyDescriptors;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectPrototype = Object.prototype;
const promisePrototype = Promise.prototype;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const SetConstructor = Set;
const setAddIntrinsic = Set.prototype.add;
const setHasIntrinsic = Set.prototype.has;
const stringCharCodeAtIntrinsic = String.prototype.charCodeAt;
const StringConstructor = String;
const stringEndsWithIntrinsic = String.prototype.endsWith;
const stringFromCharCode = String.fromCharCode;
const stringIncludesIntrinsic = String.prototype.includes;
const stringIndexOfIntrinsic = String.prototype.indexOf;
const stringSliceIntrinsic = String.prototype.slice;
const stringSplitIntrinsic = String.prototype.split;
const stringStartsWithIntrinsic = String.prototype.startsWith;
const textDecoderDecodeIntrinsic = TextDecoder.prototype.decode;
const TypeErrorConstructor = TypeError;
const typedArrayPrototype = objectGetPrototypeOf(Uint8Array.prototype);
const typedArrayByteLengthGetter = objectGetOwnPropertyDescriptor(
  typedArrayPrototype,
  "byteLength",
).get;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const { isPromise: isPromiseValue, isProxy: isProxyValue } = utilTypes;

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayJoin(value, separator) {
  return callIntrinsic(arrayJoinIntrinsic, value, [separator]);
}

function arrayPush(value, entry) {
  return callIntrinsic(arrayPushIntrinsic, value, [entry]);
}

function bufferBytes(value) {
  return callIntrinsic(typedArrayByteLengthGetter, value, []);
}

function bufferFromUtf8(value) {
  return callIntrinsic(bufferFrom, Buffer, [value, "utf8"]);
}

function byteLengthUtf8(value) {
  return callIntrinsic(bufferByteLength, Buffer, [value, "utf8"]);
}

function isBufferValue(value) {
  return callIntrinsic(bufferIsBuffer, Buffer, [value]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function setAdd(value, entry) {
  return callIntrinsic(setAddIntrinsic, value, [entry]);
}

function setHas(value, entry) {
  return callIntrinsic(setHasIntrinsic, value, [entry]);
}

function stringCharCodeAt(value, index) {
  return callIntrinsic(stringCharCodeAtIntrinsic, value, [index]);
}

function stringEndsWith(value, suffix) {
  return callIntrinsic(stringEndsWithIntrinsic, value, [suffix]);
}

function stringIncludes(value, candidate) {
  return callIntrinsic(stringIncludesIntrinsic, value, [candidate]);
}

function stringIndexOf(value, candidate) {
  return callIntrinsic(stringIndexOfIntrinsic, value, [candidate]);
}

function stringSlice(value, start, end) {
  return callIntrinsic(stringSliceIntrinsic, value, [start, end]);
}

function stringSplit(value, separator) {
  return callIntrinsic(stringSplitIntrinsic, value, [separator]);
}

function stringStartsWith(value, prefix) {
  return callIntrinsic(stringStartsWithIntrinsic, value, [prefix]);
}

function weakSetAdd(value, entry) {
  return callIntrinsic(weakSetAddIntrinsic, value, [entry]);
}

function weakSetHas(value, entry) {
  return callIntrinsic(weakSetHasIntrinsic, value, [entry]);
}

const utf8Decoder = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: true,
});

function decodeUtf8(value) {
  return callIntrinsic(textDecoderDecodeIntrinsic, utf8Decoder, [value]);
}

const ALLOWED_OPTION_KEYS = new SetConstructor();
setAdd(ALLOWED_OPTION_KEYS, "helperPath");
setAdd(ALLOWED_OPTION_KEYS, "helperRunner");
setAdd(ALLOWED_OPTION_KEYS, "platform");
setAdd(ALLOWED_OPTION_KEYS, "readMountInfo");
setAdd(ALLOWED_OPTION_KEYS, "runHelper");
setAdd(ALLOWED_OPTION_KEYS, "trustedRoots");

const HELPER_STDOUT_LIMIT_BYTES = 4 * 1024;
const HELPER_STDERR_LIMIT_BYTES = 4 * 1024;
const MOUNTINFO_LIMIT_BYTES = 1024 * 1024;
const MAX_PATH_BYTES = 4095;
const HELPER_TIMEOUT_MS = 60_000;

const MAX_TRUSTED_ROOTS = 128;
const DEFAULT_PLATFORM = process.platform;

const HELPER_EXIT = objectFreeze({
  exists: 73,
  mismatch: 65,
  missing: 66,
  outcomeUncertain: 78,
  unsupported: 69,
  unreadable: 77,
});

const ERROR_MESSAGES = objectFreeze({
  helper_failed: "Linux ext4 inspection helper failed",
  helper_output_invalid: "Linux ext4 inspection helper output is invalid",
  helper_output_too_large: "Linux ext4 inspection helper output exceeded its limit",
  helper_unavailable: "Linux ext4 inspection helper is unavailable",
  invalid_options: "Linux ext4 inspector options are invalid",
  invalid_path: "Linux ext4 inspection path is invalid",
  mountinfo_failed: "Linux mount-point inspection failed",
  operation_outcome_uncertain:
    "Linux ext4 fd-bound operation outcome is uncertain",
  path_exists: "Linux ext4 operation target already exists",
  path_mismatch: "Linux ext4 inspection path does not match one trusted root",
  path_missing: "Linux ext4 inspection path is missing",
  path_unreadable: "Linux ext4 inspection path is unreadable",
  unsupported: "Linux ext4 inspection is unsupported",
  unsupported_platform: "Linux ext4 inspection requires Linux",
});

const internalErrors = new WeakSetConstructor();

const FILESYSTEM_UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const FILESYSTEM_ID_PATTERN =
  /^ext4fs:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const NIL_FILESYSTEM_UUID = "00000000-0000-0000-0000-000000000000";
const DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;
const INODE_PATTERN = /^[1-9][0-9]*$/u;
const OBJECT_ID_PATTERN = /^ext4fh1:[0-9a-f]{64}$/u;
const LOOP_DEVICE_PATTERN =
  /^\/dev\/loop(?:0|[1-9][0-9]{0,2}|[1-3][0-9]{3}|40(?:[0-8][0-9]|9[0-5]))$/u;
const DEVICE_PAIR_PATTERN = /^(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)$/u;
const CONTROL_FILE_NAMES = objectFreeze({
  journal: ".operation-journal.lock",
  publication: ".stopped-directory-publication.lock",
});
const UINT64_MAX_DECIMAL = "18446744073709551615";

const SAFE_HELPER_ENVIRONMENT = objectFreeze({
  LANG: "C",
  LC_ALL: "C",
});

const HELPER_RUN_OPTIONS = objectFreeze({
  encoding: "buffer",
  env: SAFE_HELPER_ENVIRONMENT,
  killSignal: "SIGKILL",
  maxBuffer: HELPER_STDOUT_LIMIT_BYTES,
  shell: false,
  timeout: HELPER_TIMEOUT_MS,
  windowsHide: true,
});

export class LinuxExt4InspectorError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor("unsupported Linux ext4 inspector error");
    }
    super(ERROR_MESSAGES[code]);
    objectDefineProperties(this, {
      code: {
        configurable: true,
        enumerable: true,
        value: code,
        writable: true,
      },
      name: {
        configurable: true,
        enumerable: true,
        value: "LinuxExt4InspectorError",
        writable: true,
      },
      retryable: {
        configurable: true,
        enumerable: true,
        value: false,
        writable: true,
      },
    });
    objectFreeze(this);
  }
}

function createError(code) {
  const error = new LinuxExt4InspectorError(code);
  weakSetAdd(internalErrors, error);
  return error;
}

function fail(code) {
  throw createError(code);
}

function ownDataOptions(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxyValue(value) ||
    arrayIsArray(value)
  ) {
    fail("invalid_options");
  }
  let prototype;
  let descriptors;
  try {
    prototype = objectGetPrototypeOf(value);
    descriptors = objectGetOwnPropertyDescriptors(value);
  } catch {
    fail("invalid_options");
  }
  if (prototype !== null && prototype !== objectPrototype) {
    fail("invalid_options");
  }
  const options = objectCreate(null);
  const keys = reflectOwnKeys(descriptors);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (
      typeof key !== "string" ||
      !setHas(ALLOWED_OPTION_KEYS, key) ||
      descriptors[key].enumerable !== true ||
      !objectHasOwn(descriptors[key], "value") ||
      objectHasOwn(descriptors[key], "get") ||
      objectHasOwn(descriptors[key], "set")
    ) {
      fail("invalid_options");
    }
    options[key] = descriptors[key].value;
  }
  if (
    !objectHasOwn(options, "helperPath") ||
    !objectHasOwn(options, "trustedRoots")
  ) {
    fail("invalid_options");
  }
  return options;
}

function validAbsolutePath(value) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > MAX_PATH_BYTES ||
    stringIncludes(value, "\0")
  ) {
    return false;
  }
  let encoded;
  let roundTrip;
  try {
    encoded = bufferFromUtf8(value);
    roundTrip = decodeUtf8(encoded);
  } catch {
    return false;
  }
  if (
    roundTrip !== value ||
    byteLengthUtf8(value) > MAX_PATH_BYTES ||
    !stringStartsWith(value, "/")
  ) {
    return false;
  }
  if (value === "/") return true;
  if (stringEndsWith(value, "/")) return false;
  const components = stringSplit(value, "/");
  for (let index = 1; index < components.length; index += 1) {
    if (
      components[index] === "" ||
      components[index] === "." ||
      components[index] === ".."
    ) {
      return false;
    }
  }
  return true;
}

function normalizeTrustedRoots(value) {
  if (isProxyValue(value) || !arrayIsArray(value)) fail("invalid_options");
  let descriptors;
  let prototype;
  try {
    descriptors = objectGetOwnPropertyDescriptors(value);
    prototype = objectGetPrototypeOf(value);
  } catch {
    fail("invalid_options");
  }
  const lengthDescriptor = objectHasOwn(descriptors, "length")
    ? descriptors.length
    : undefined;
  if (
    prototype !== arrayPrototype ||
    lengthDescriptor === undefined ||
    !objectHasOwn(lengthDescriptor, "value") ||
    lengthDescriptor.value < 1 ||
    lengthDescriptor.value > MAX_TRUSTED_ROOTS ||
    reflectOwnKeys(descriptors).length !== lengthDescriptor.value + 1
  ) {
    fail("invalid_options");
  }
  const roots = [];
  const seen = new SetConstructor();
  for (let index = 0; index < lengthDescriptor.value; index += 1) {
    const key = `${index}`;
    const descriptor = objectHasOwn(descriptors, key)
      ? descriptors[key]
      : undefined;
    if (
      descriptor === undefined ||
      descriptor.enumerable !== true ||
      !objectHasOwn(descriptor, "value") ||
      objectHasOwn(descriptor, "get") ||
      objectHasOwn(descriptor, "set")
    ) {
      fail("invalid_options");
    }
    const root = descriptor.value;
    if (!validAbsolutePath(root) || setHas(seen, root)) fail("invalid_options");
    for (
      let existingIndex = 0;
      existingIndex < roots.length;
      existingIndex += 1
    ) {
      const existing = roots[existingIndex];
      if (pathAtOrInside(existing, root) || pathAtOrInside(root, existing)) {
        fail("invalid_options");
      }
    }
    setAdd(seen, root);
    arrayPush(roots, root);
  }
  return objectFreeze(roots);
}

function normalizeOptions(raw) {
  const options = ownDataOptions(raw);
  const platform =
    options.platform === undefined ? DEFAULT_PLATFORM : options.platform;
  if (platform !== "linux") fail("unsupported_platform");
  if (!validAbsolutePath(options.helperPath)) fail("invalid_options");
  if (
    options.runHelper !== undefined &&
    options.helperRunner !== undefined
  ) {
    fail("invalid_options");
  }
  const runHelper =
    options.runHelper !== undefined
      ? options.runHelper
      : options.helperRunner !== undefined
        ? options.helperRunner
        : runExt4InspectionHelper;
  const readMountInfo =
    options.readMountInfo === undefined ? readFile : options.readMountInfo;
  if (
    typeof runHelper !== "function" ||
    isProxyValue(runHelper) ||
    typeof readMountInfo !== "function" ||
    isProxyValue(readMountInfo)
  ) {
    fail("invalid_options");
  }
  return objectFreeze({
    helperPath: options.helperPath,
    readMountInfo,
    runHelper,
    trustedRoots: normalizeTrustedRoots(options.trustedRoots),
  });
}

async function runExt4InspectionHelper(helperPath, args, options) {
  return execFileAsync(helperPath, args, options);
}

function pathAtOrInside(root, candidate) {
  if (candidate === root) return true;
  if (root === "/") return stringStartsWith(candidate, "/");
  return stringStartsWith(candidate, `${root}/`);
}

function selectTrustedRoot(path, trustedRoots) {
  if (!validAbsolutePath(path)) fail("invalid_path");
  const matches = [];
  for (let index = 0; index < trustedRoots.length; index += 1) {
    const root = trustedRoots[index];
    if (pathAtOrInside(root, path)) arrayPush(matches, root);
  }
  if (matches.length !== 1) fail("path_mismatch");
  const root = matches[0];
  const relativePath =
    root === path
      ? "."
      : stringSlice(path, root === "/" ? 1 : root.length + 1, path.length);
  if (
    stringIncludes(relativePath, "\0") ||
    stringStartsWith(relativePath, "/") ||
    relativePath === ".." ||
    stringStartsWith(relativePath, "../") ||
    byteLengthUtf8(relativePath) > MAX_PATH_BYTES
  ) {
    fail("path_mismatch");
  }
  return objectFreeze({ relativePath, root });
}

function boundedHelperBytes(value, limit) {
  if (isProxyValue(value) || !isBufferValue(value)) {
    fail("helper_output_invalid");
  }
  if (bufferBytes(value) > limit) fail("helper_output_too_large");
  return value;
}

function dataProperty(value, name, required = true) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function") ||
    isProxyValue(value)
  ) {
    fail("helper_failed");
  }
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, name);
  } catch {
    fail("helper_failed");
  }
  if (descriptor === undefined) {
    if (required) fail("helper_failed");
    return undefined;
  }
  if (
    !objectHasOwn(descriptor, "value") ||
    objectHasOwn(descriptor, "get") ||
    objectHasOwn(descriptor, "set")
  ) {
    fail("helper_failed");
  }
  return descriptor.value;
}

function normalizeHelperCompletion(value, thrown = false) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function") ||
    isProxyValue(value)
  ) {
    fail("helper_failed");
  }
  const stdout = boundedHelperBytes(
    dataProperty(value, "stdout"),
    HELPER_STDOUT_LIMIT_BYTES,
  );
  const stderr = boundedHelperBytes(
    dataProperty(value, "stderr"),
    HELPER_STDERR_LIMIT_BYTES,
  );
  const explicitExitCode = dataProperty(value, "exitCode", false);
  const errorCode = dataProperty(value, "code", false);
  const signal = dataProperty(value, "signal", false) ?? null;
  if (errorCode !== undefined && typeof errorCode !== "number") {
    fail("helper_failed");
  }
  let exitCode = explicitExitCode;
  if (exitCode === undefined && typeof errorCode === "number") {
    exitCode = errorCode;
  }
  if (exitCode === undefined && !thrown) exitCode = 0;
  if (!numberIsSafeInteger(exitCode) || exitCode < 0 || exitCode > 255) {
    fail("helper_failed");
  }
  if (signal !== null && typeof signal !== "string") fail("helper_failed");
  return objectFreeze({ exitCode, signal, stderr, stdout });
}

function assertNativePromise(value, code) {
  if (isProxyValue(value) || !isPromiseValue(value)) fail(code);
  let prototype;
  try {
    prototype = objectGetPrototypeOf(value);
    if (
      objectGetOwnPropertyDescriptor(value, "catch") !== undefined ||
      objectGetOwnPropertyDescriptor(value, "constructor") !== undefined ||
      objectGetOwnPropertyDescriptor(value, "finally") !== undefined ||
      objectGetOwnPropertyDescriptor(value, "then") !== undefined
    ) {
      fail(code);
    }
  } catch {
    fail(code);
  }
  if (prototype !== promisePrototype) fail(code);
  return value;
}

function helperFailureCode(exitCode, mutation, preserveMismatch) {
  if (exitCode === HELPER_EXIT.exists) return "path_exists";
  if (preserveMismatch && exitCode === HELPER_EXIT.mismatch) {
    return "path_mismatch";
  }
  if (mutation || exitCode === HELPER_EXIT.outcomeUncertain) {
    return "operation_outcome_uncertain";
  }
  if (exitCode === HELPER_EXIT.missing) return "path_missing";
  if (exitCode === HELPER_EXIT.unreadable) return "path_unreadable";
  if (exitCode === HELPER_EXIT.mismatch) return "path_mismatch";
  if (exitCode === HELPER_EXIT.unsupported) return "unsupported";
  return "helper_failed";
}

async function invokeHelper(
  state,
  path,
  command = "inspect",
  extraArgs = [],
  mutation = command === "operate",
  preserveMismatch = false,
) {
  const selected = await selectInspectionRoot(state, path);
  const args = [
    command,
    "--root",
    selected.root,
    "--relative",
    selected.relativePath,
  ];
  for (let index = 0; index < extraArgs.length; index += 1) {
    arrayPush(args, extraArgs[index]);
  }
  objectFreeze(args);
  let completion;
  try {
    const pending = callIntrinsic(state.runHelper, undefined, [
      state.helperPath,
      args,
      HELPER_RUN_OPTIONS,
    ]);
    const value = await assertNativePromise(pending, "helper_failed");
    completion = normalizeHelperCompletion(value);
  } catch (error) {
    if (weakSetHas(internalErrors, error)) throw error;
    let code = undefined;
    if (
      error !== null &&
      (typeof error === "object" || typeof error === "function")
    ) {
      code = dataProperty(error, "code", false);
    }
    if (code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER") {
      fail("helper_output_too_large");
    }
    if (code === "ENOENT" || code === "EACCES") {
      fail(mutation ? "operation_outcome_uncertain" : "helper_unavailable");
    }
    try {
      completion = normalizeHelperCompletion(error, true);
    } catch (normalizationError) {
      if (weakSetHas(internalErrors, normalizationError)) {
        throw normalizationError;
      }
      fail("helper_failed");
    }
  }
  if (completion.signal !== null) {
    fail(mutation ? "operation_outcome_uncertain" : "helper_failed");
  }
  if (completion.exitCode !== 0) {
    fail(helperFailureCode(completion.exitCode, mutation, preserveMismatch));
  }
  if (bufferBytes(completion.stderr) !== 0) fail("helper_output_invalid");
  return completion.stdout;
}

function exactHelperRecord(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxyValue(value) ||
    arrayIsArray(value) ||
    objectGetPrototypeOf(value) !== objectPrototype
  ) {
    fail("helper_output_invalid");
  }
  const descriptors = objectGetOwnPropertyDescriptors(value);
  const keys = reflectOwnKeys(descriptors);
  const expected = ["device", "filesystemUuid", "inode", "objectId"];
  if (keys.length !== expected.length) fail("helper_output_invalid");
  const record = objectCreate(null);
  for (let index = 0; index < expected.length; index += 1) {
    const key = expected[index];
    if (!objectHasOwn(descriptors, key)) fail("helper_output_invalid");
    const descriptor = descriptors[key];
    if (
      descriptor.enumerable !== true ||
      !objectHasOwn(descriptor, "value") ||
      objectHasOwn(descriptor, "get") ||
      objectHasOwn(descriptor, "set")
    ) {
      fail("helper_output_invalid");
    }
    record[key] = descriptor.value;
  }
  return record;
}

function uint64Decimal(value, pattern) {
  return (
    typeof value === "string" &&
    regexpTest(pattern, value) &&
    (value.length < UINT64_MAX_DECIMAL.length ||
      (value.length === UINT64_MAX_DECIMAL.length &&
        value <= UINT64_MAX_DECIMAL))
  );
}

function parseHelperOutput(bytes) {
  let text;
  try {
    text = decodeUtf8(bytes);
  } catch {
    fail("helper_output_invalid");
  }
  if (
    !stringEndsWith(text, "\n") ||
    stringIndexOf(text, "\n") !== text.length - 1 ||
    stringIncludes(text, "\r")
  ) {
    fail("helper_output_invalid");
  }
  let parsed;
  try {
    parsed = callIntrinsic(jsonParse, undefined, [stringSlice(text, 0, -1)]);
  } catch {
    fail("helper_output_invalid");
  }
  const record = exactHelperRecord(parsed);
  if (
    typeof record.filesystemUuid !== "string" ||
    !regexpTest(FILESYSTEM_UUID_PATTERN, record.filesystemUuid) ||
    record.filesystemUuid === NIL_FILESYSTEM_UUID ||
    !uint64Decimal(record.device, DECIMAL_PATTERN) ||
    !uint64Decimal(record.inode, INODE_PATTERN) ||
    typeof record.objectId !== "string" ||
    !regexpTest(OBJECT_ID_PATTERN, record.objectId)
  ) {
    fail("helper_output_invalid");
  }
  const canonicalRecord = objectCreate(null);
  canonicalRecord.filesystemUuid = record.filesystemUuid;
  canonicalRecord.device = record.device;
  canonicalRecord.inode = record.inode;
  canonicalRecord.objectId = record.objectId;
  const canonical = `${callIntrinsic(jsonStringify, undefined, [
    canonicalRecord,
  ])}\n`;
  if (text !== canonical) fail("helper_output_invalid");
  return objectFreeze({
    device: record.device,
    filesystemUuid: record.filesystemUuid,
    inode: record.inode,
    objectId: record.objectId,
  });
}

async function inspectFilesystemInternal(state, path) {
  const inspected = parseHelperOutput(await invokeHelper(state, path));
  return objectFreeze({
    durability: "local-fsync-rename",
    filesystemId: `ext4fs:${inspected.filesystemUuid}`,
    objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
    type: "ext4",
  });
}

async function inspectPersistentObjectIdentityInternal(state, path) {
  const inspected = parseHelperOutput(await invokeHelper(state, path));
  return objectFreeze({
    device: inspected.device,
    inode: inspected.inode,
    objectId: inspected.objectId,
  });
}

async function inspectFilesystemObjectInternal(state, path) {
  const inspected = parseHelperOutput(await invokeHelper(state, path));
  return objectFreeze({
    filesystem: objectFreeze({
      durability: "local-fsync-rename",
      filesystemId: `ext4fs:${inspected.filesystemUuid}`,
      objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
      type: "ext4",
    }),
    identity: objectFreeze({
      device: inspected.device,
      inode: inspected.inode,
      objectId: inspected.objectId,
    }),
  });
}

const FD_OPERATION_KEYS = objectFreeze({
  "attach-loop": objectFreeze([
    "device",
    "inode",
    "operation",
    "parentDevice",
    "parentInode",
    "path",
  ]),
  "create-directory": objectFreeze([
    "exclusive",
    "operation",
    "parentDevice",
    "parentInode",
    "path",
  ]),
  "create-image": objectFreeze([
    "operation",
    "parentDevice",
    "parentInode",
    "path",
    "sizeBytes",
  ]),
  "detach-loop-settle": objectFreeze([
    "device",
    "inode",
    "loopDevice",
    "operation",
    "parentDevice",
    "parentInode",
    "path",
  ]),
  "find-loop": objectFreeze([
    "device",
    "inode",
    "operation",
    "parentDevice",
    "parentInode",
    "path",
  ]),
  "format-ext4": objectFreeze([
    "device",
    "executable",
    "inode",
    "operation",
    "parentDevice",
    "parentInode",
    "path",
  ]),
  "inspect-loop": objectFreeze([
    "device",
    "inode",
    "loopDevice",
    "operation",
    "parentDevice",
    "parentInode",
    "path",
  ]),
  "inspect-private-path": objectFreeze([
    "device",
    "inode",
    "kind",
    "linkPolicy",
    "mode",
    "operation",
    "path",
    "requireEmpty",
    "uid",
  ]),
  "mount-ext4": objectFreeze([
    "backingDevice",
    "backingInode",
    "loopDevice",
    "loopRdev",
    "operation",
    "parentDevice",
    "parentInode",
    "path",
    "sizeBytes",
    "targetDevice",
    "targetInode",
  ]),
  "provision-control-root": objectFreeze([
    "device",
    "expectedControlFilesystemId",
    "expectedControlObjectId",
    "filesystemId",
    "inode",
    "kind",
    "objectId",
    "operation",
    "rootPath",
  ]),
  "remove-directory": objectFreeze([
    "operation",
    "parentDevice",
    "parentInode",
    "path",
    "targetDevice",
    "targetInode",
  ]),
  "remove-file": objectFreeze([
    "operation",
    "parentDevice",
    "parentInode",
    "path",
    "targetDevice",
    "targetInode",
  ]),
  syncfs: objectFreeze([
    "device",
    "filesystemId",
    "inode",
    "objectId",
    "operation",
    "path",
  ]),
  "unmount-ext4": objectFreeze([
    "operation",
    "parentDevice",
    "parentInode",
    "path",
    "targetDevice",
    "targetFilesystemId",
    "targetInode",
    "targetObjectId",
  ]),
});

const FD_IDENTITY_KEYS = objectFreeze([
  "device",
  "inode",
  "parentDevice",
  "parentInode",
  "targetDevice",
  "targetInode",
]);

function exactFdOperationRequest(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxyValue(value) ||
    arrayIsArray(value)
  ) {
    fail("invalid_path");
  }
  let prototype;
  let descriptors;
  try {
    prototype = objectGetPrototypeOf(value);
    descriptors = objectGetOwnPropertyDescriptors(value);
  } catch {
    fail("invalid_path");
  }
  if (prototype !== null && prototype !== objectPrototype) fail("invalid_path");
  const operationDescriptor = descriptors.operation;
  if (
    operationDescriptor === undefined ||
    !objectHasOwn(operationDescriptor, "value") ||
    typeof operationDescriptor.value !== "string" ||
    !objectHasOwn(FD_OPERATION_KEYS, operationDescriptor.value)
  ) {
    fail("invalid_path");
  }
  const operation = operationDescriptor.value;
  const expected = FD_OPERATION_KEYS[operation];
  const keys = reflectOwnKeys(descriptors);
  if (keys.length !== expected.length) fail("invalid_path");
  const result = objectCreate(null);
  for (let index = 0; index < expected.length; index += 1) {
    const key = expected[index];
    const descriptor = descriptors[key];
    if (
      descriptor === undefined ||
      descriptor.enumerable !== true ||
      !objectHasOwn(descriptor, "value") ||
      objectHasOwn(descriptor, "get") ||
      objectHasOwn(descriptor, "set")
    ) {
      fail("invalid_path");
    }
    result[key] = descriptor.value;
  }
  const path =
    operation === "provision-control-root" ? result.rootPath : result.path;
  if (!validAbsolutePath(path) || path === "/") fail("invalid_path");
  for (let index = 0; index < FD_IDENTITY_KEYS.length; index += 1) {
    const key = FD_IDENTITY_KEYS[index];
    if (
      objectHasOwn(result, key) &&
      !uint64Decimal(
        result[key],
        stringEndsWith(key, "Inode") || key === "inode"
          ? INODE_PATTERN
          : DECIMAL_PATTERN,
      )
    ) {
      fail("invalid_path");
    }
  }
  if (
    operation === "create-image" &&
    (!numberIsSafeInteger(result.sizeBytes) ||
      result.sizeBytes < 1024 * 1024 ||
      result.sizeBytes > 8 * 1024 * 1024 * 1024 * 1024)
  ) {
    fail("invalid_path");
  }
  if (
    operation === "create-directory" &&
    typeof result.exclusive !== "boolean"
  ) {
    fail("invalid_path");
  }
  if (
    operation === "inspect-private-path" &&
    (typeof result.uid !== "string" ||
      !uint64Decimal(result.uid, DECIMAL_PATTERN) ||
      typeof result.requireEmpty !== "boolean" ||
      !(
        (result.kind === "directory" &&
          result.mode === "0700" &&
          result.linkPolicy === "positive") ||
        (result.kind === "file" &&
          result.mode === "0600" &&
          result.linkPolicy === "single")
      ) ||
      (result.requireEmpty && result.kind !== "directory"))
  ) {
    fail("invalid_path");
  }
  if (
    (operation === "inspect-loop" ||
      operation === "detach-loop-settle" ||
      operation === "mount-ext4") &&
    (typeof result.loopDevice !== "string" ||
      !regexpTest(LOOP_DEVICE_PATTERN, result.loopDevice))
  ) {
    fail("invalid_path");
  }
  if (
    operation === "mount-ext4" &&
    (!uint64Decimal(result.backingDevice, DECIMAL_PATTERN) ||
      !uint64Decimal(result.backingInode, INODE_PATTERN) ||
      typeof result.loopRdev !== "string" ||
      !regexpTest(DEVICE_PAIR_PATTERN, result.loopRdev) ||
      !uint64Decimal(result.sizeBytes, INODE_PATTERN))
  ) {
    fail("invalid_path");
  }
  if (
    (operation === "provision-control-root" || operation === "syncfs") &&
    (typeof result.filesystemId !== "string" ||
      !regexpTest(FILESYSTEM_ID_PATTERN, result.filesystemId) ||
      typeof result.objectId !== "string" ||
      !regexpTest(OBJECT_ID_PATTERN, result.objectId))
  ) {
    fail("invalid_path");
  }
  if (operation === "provision-control-root") {
    const expectedFilesystemId = result.expectedControlFilesystemId;
    const expectedObjectId = result.expectedControlObjectId;
    if (
      (expectedFilesystemId === null) !== (expectedObjectId === null) ||
      (expectedFilesystemId !== null &&
        (typeof expectedFilesystemId !== "string" ||
          !regexpTest(FILESYSTEM_ID_PATTERN, expectedFilesystemId) ||
          expectedFilesystemId !== result.filesystemId ||
          typeof expectedObjectId !== "string" ||
          !regexpTest(OBJECT_ID_PATTERN, expectedObjectId)))
    ) {
      fail("invalid_path");
    }
  }
  if (
    operation === "unmount-ext4" &&
    (typeof result.targetFilesystemId !== "string" ||
      !regexpTest(FILESYSTEM_ID_PATTERN, result.targetFilesystemId) ||
      typeof result.targetObjectId !== "string" ||
      !regexpTest(OBJECT_ID_PATTERN, result.targetObjectId))
  ) {
    fail("invalid_path");
  }
  if (
    operation === "format-ext4" &&
    !validAbsolutePath(result.executable)
  ) {
    fail("invalid_path");
  }
  if (
    operation === "provision-control-root" &&
    !objectHasOwn(CONTROL_FILE_NAMES, result.kind)
  ) {
    fail("invalid_path");
  }
  return objectFreeze(result);
}

function canonicalJsonRecord(bytes, keys) {
  let text;
  try {
    text = decodeUtf8(bytes);
  } catch {
    fail("helper_output_invalid");
  }
  if (
    !stringEndsWith(text, "\n") ||
    stringIndexOf(text, "\n") !== text.length - 1 ||
    stringIncludes(text, "\r")
  ) {
    fail("helper_output_invalid");
  }
  let parsed;
  try {
    parsed = callIntrinsic(jsonParse, undefined, [stringSlice(text, 0, -1)]);
  } catch {
    fail("helper_output_invalid");
  }
  if (
    parsed === null ||
    typeof parsed !== "object" ||
    isProxyValue(parsed) ||
    arrayIsArray(parsed) ||
    objectGetPrototypeOf(parsed) !== objectPrototype
  ) {
    fail("helper_output_invalid");
  }
  const descriptors = objectGetOwnPropertyDescriptors(parsed);
  if (reflectOwnKeys(descriptors).length !== keys.length) {
    fail("helper_output_invalid");
  }
  const record = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = descriptors[key];
    if (
      descriptor === undefined ||
      descriptor.enumerable !== true ||
      !objectHasOwn(descriptor, "value") ||
      objectHasOwn(descriptor, "get") ||
      objectHasOwn(descriptor, "set")
    ) {
      fail("helper_output_invalid");
    }
    record[key] = descriptor.value;
  }
  if (`${callIntrinsic(jsonStringify, undefined, [record])}\n` !== text) {
    fail("helper_output_invalid");
  }
  return record;
}

function parseFdOperationOutput(bytes, operation) {
  if (operation === "inspect-private-path") {
    const record = canonicalJsonRecord(bytes, [
      "device",
      "empty",
      "inode",
      "private",
      "status",
    ]);
    if (
      !uint64Decimal(record.device, DECIMAL_PATTERN) ||
      !uint64Decimal(record.inode, INODE_PATTERN) ||
      (record.empty !== null && typeof record.empty !== "boolean") ||
      typeof record.private !== "boolean" ||
      record.status !== "ok"
    ) {
      fail("helper_output_invalid");
    }
    return objectFreeze({
      device: record.device,
      empty: record.empty,
      inode: record.inode,
      private: record.private,
      status: "ok",
    });
  }
  if (operation === "create-directory") {
    const record = canonicalJsonRecord(bytes, [
      "created",
      "device",
      "inode",
      "status",
    ]);
    if (
      typeof record.created !== "boolean" ||
      !uint64Decimal(record.device, DECIMAL_PATTERN) ||
      !uint64Decimal(record.inode, INODE_PATTERN) ||
      record.status !== "ok"
    ) {
      fail("helper_output_invalid");
    }
    return objectFreeze({
      created: record.created,
      device: record.device,
      inode: record.inode,
      status: "ok",
    });
  }
  if (operation === "create-image") {
    const record = canonicalJsonRecord(bytes, ["device", "inode", "status"]);
    if (
      !uint64Decimal(record.device, DECIMAL_PATTERN) ||
      !uint64Decimal(record.inode, INODE_PATTERN) ||
      record.status !== "ok"
    ) {
      fail("helper_output_invalid");
    }
    return objectFreeze({
      device: record.device,
      inode: record.inode,
      status: "ok",
    });
  }
  if (operation === "provision-control-root") {
    const record = canonicalJsonRecord(bytes, [
      "controlFileName",
      "created",
      "device",
      "filesystemUuid",
      "inode",
      "kind",
      "objectId",
      "status",
    ]);
    if (
      record.controlFileName !== CONTROL_FILE_NAMES[record.kind] ||
      typeof record.created !== "boolean" ||
      !uint64Decimal(record.device, DECIMAL_PATTERN) ||
      typeof record.filesystemUuid !== "string" ||
      !regexpTest(FILESYSTEM_UUID_PATTERN, record.filesystemUuid) ||
      record.filesystemUuid === NIL_FILESYSTEM_UUID ||
      !uint64Decimal(record.inode, INODE_PATTERN) ||
      (record.kind !== "publication" && record.kind !== "journal") ||
      typeof record.objectId !== "string" ||
      !regexpTest(OBJECT_ID_PATTERN, record.objectId) ||
      record.status !== "ok"
    ) {
      fail("helper_output_invalid");
    }
    return objectFreeze({
      controlFileName: record.controlFileName,
      created: record.created,
      controlFileIdentity: objectFreeze({
        device: record.device,
        inode: record.inode,
        filesystemId: `ext4fs:${record.filesystemUuid}`,
        objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
        objectId: record.objectId,
      }),
      kind: record.kind,
      status: "ok",
    });
  }
  if (
    operation === "attach-loop" ||
    operation === "find-loop" ||
    operation === "inspect-loop"
  ) {
    let statusRecord;
    try {
      statusRecord = canonicalJsonRecord(bytes, ["status"]);
    } catch (error) {
      if (!weakSetHas(internalErrors, error)) throw error;
    }
    if (statusRecord !== undefined) {
      if (operation !== "find-loop" || statusRecord.status !== "absent") {
        fail("helper_output_invalid");
      }
      return objectFreeze({ status: "absent" });
    }
    const record = canonicalJsonRecord(bytes, [
      "backingDevice",
      "backingInode",
      "blockSize",
      "loopDevice",
      "loopRdev",
      "offset",
      "readOnly",
      "sizeBytes",
      "sizeLimit",
      "status",
    ]);
    if (
      !uint64Decimal(record.backingDevice, DECIMAL_PATTERN) ||
      !uint64Decimal(record.backingInode, INODE_PATTERN) ||
      record.blockSize !== "512" ||
      typeof record.loopDevice !== "string" ||
      !regexpTest(LOOP_DEVICE_PATTERN, record.loopDevice) ||
      typeof record.loopRdev !== "string" ||
      !regexpTest(DEVICE_PAIR_PATTERN, record.loopRdev) ||
      record.offset !== "0" ||
      record.readOnly !== false ||
      !uint64Decimal(record.sizeBytes, INODE_PATTERN) ||
      record.sizeLimit !== "0" ||
      (record.status !== "attached" && record.status !== "present")
    ) {
      fail("helper_output_invalid");
    }
    return objectFreeze({
      backingDevice: record.backingDevice,
      backingInode: record.backingInode,
      blockSize: record.blockSize,
      loopDevice: record.loopDevice,
      loopRdev: record.loopRdev,
      offset: record.offset,
      readOnly: false,
      sizeBytes: record.sizeBytes,
      sizeLimit: record.sizeLimit,
      status: record.status,
    });
  }
  const record = canonicalJsonRecord(bytes, ["status"]);
  if (record.status !== "ok") fail("helper_output_invalid");
  return objectFreeze({ status: "ok" });
}

async function runFdOperationInternal(state, requestValue) {
  const request = exactFdOperationRequest(requestValue);
  const operation = request.operation;
  const mutation =
    operation !== "find-loop" &&
    operation !== "inspect-loop" &&
    operation !== "inspect-private-path";
  const path =
    operation === "provision-control-root" ? request.rootPath : request.path;
  const childOperation =
    operation !== "inspect-private-path" &&
    operation !== "syncfs" &&
    operation !== "provision-control-root";
  const authorityPath = childOperation ? dirname(path) : path;
  const extraArgs = ["--verb", operation];
  if (childOperation) {
    arrayPush(extraArgs, "--name");
    arrayPush(extraArgs, basename(path));
  }
  if (operation === "create-image") {
    arrayPush(extraArgs, "--size");
    arrayPush(extraArgs, `${request.sizeBytes}`);
  } else if (operation === "create-directory") {
    arrayPush(extraArgs, "--exclusive");
    arrayPush(extraArgs, request.exclusive ? "yes" : "no");
  } else if (operation === "inspect-private-path") {
    arrayPush(extraArgs, "--kind");
    arrayPush(extraArgs, request.kind);
    arrayPush(extraArgs, "--uid");
    arrayPush(extraArgs, request.uid);
    arrayPush(extraArgs, "--mode");
    arrayPush(extraArgs, request.mode);
    arrayPush(extraArgs, "--link-policy");
    arrayPush(extraArgs, request.linkPolicy);
    arrayPush(extraArgs, "--require-empty");
    arrayPush(extraArgs, request.requireEmpty ? "yes" : "no");
  } else if (operation === "format-ext4") {
    arrayPush(extraArgs, "--executable");
    arrayPush(extraArgs, request.executable);
  } else if (
    operation === "inspect-loop" ||
    operation === "detach-loop-settle" ||
    operation === "mount-ext4"
  ) {
    arrayPush(extraArgs, "--loop");
    arrayPush(extraArgs, request.loopDevice);
    if (operation === "mount-ext4") {
      arrayPush(extraArgs, "--backing-device");
      arrayPush(extraArgs, request.backingDevice);
      arrayPush(extraArgs, "--backing-inode");
      arrayPush(extraArgs, request.backingInode);
      arrayPush(extraArgs, "--loop-rdev");
      arrayPush(extraArgs, request.loopRdev);
      arrayPush(extraArgs, "--size");
      arrayPush(extraArgs, request.sizeBytes);
    }
  } else if (operation === "provision-control-root") {
    arrayPush(extraArgs, "--kind");
    arrayPush(extraArgs, request.kind);
  }
  if (operation === "provision-control-root" || operation === "syncfs") {
    arrayPush(extraArgs, "--filesystem-id");
    arrayPush(extraArgs, request.filesystemId);
    arrayPush(extraArgs, "--object-id");
    arrayPush(extraArgs, request.objectId);
  }
  if (operation === "provision-control-root") {
    arrayPush(extraArgs, "--expected-control-filesystem-id");
    arrayPush(
      extraArgs,
      request.expectedControlFilesystemId === null
        ? "-"
        : request.expectedControlFilesystemId,
    );
    arrayPush(extraArgs, "--expected-control-object-id");
    arrayPush(
      extraArgs,
      request.expectedControlObjectId === null
        ? "-"
        : request.expectedControlObjectId,
    );
  }
  if (objectHasOwn(request, "parentDevice")) {
    arrayPush(extraArgs, "--parent-device");
    arrayPush(extraArgs, request.parentDevice);
    arrayPush(extraArgs, "--parent-inode");
    arrayPush(extraArgs, request.parentInode);
  }
  if (objectHasOwn(request, "targetDevice")) {
    arrayPush(extraArgs, "--target-device");
    arrayPush(extraArgs, request.targetDevice);
    arrayPush(extraArgs, "--target-inode");
    arrayPush(extraArgs, request.targetInode);
  }
  if (operation === "unmount-ext4") {
    arrayPush(extraArgs, "--target-filesystem-id");
    arrayPush(extraArgs, request.targetFilesystemId);
    arrayPush(extraArgs, "--target-object-id");
    arrayPush(extraArgs, request.targetObjectId);
  }
  if (objectHasOwn(request, "device")) {
    arrayPush(extraArgs, "--device");
    arrayPush(extraArgs, request.device);
    arrayPush(extraArgs, "--inode");
    arrayPush(extraArgs, request.inode);
  }
  let output;
  try {
    output = await invokeHelper(
      state,
      authorityPath,
      "operate",
      objectFreeze(extraArgs),
      mutation,
      operation === "create-directory",
    );
    const parsed = parseFdOperationOutput(output, operation);
    if (
      operation === "inspect-private-path" &&
      ((request.requireEmpty && typeof parsed.empty !== "boolean") ||
        (!request.requireEmpty && parsed.empty !== null))
    ) {
      fail("helper_output_invalid");
    }
    return parsed;
  } catch (error) {
    if (!mutation && weakSetHas(internalErrors, error)) {
      throw error;
    }
    if (
      weakSetHas(internalErrors, error) &&
      (error.code === "operation_outcome_uncertain" ||
        error.code === "path_exists" ||
        (operation === "create-directory" && error.code === "path_mismatch"))
    ) {
      throw error;
    }
    fail("operation_outcome_uncertain");
  }
}

async function provisionControlRootInternal(state, requestValue) {
  if (
    requestValue === null ||
    typeof requestValue !== "object" ||
    isProxyValue(requestValue) ||
    arrayIsArray(requestValue)
  ) {
    fail("invalid_path");
  }
  let descriptors;
  let prototype;
  try {
    descriptors = objectGetOwnPropertyDescriptors(requestValue);
    prototype = objectGetPrototypeOf(requestValue);
  } catch {
    fail("invalid_path");
  }
  if (
    (prototype !== null && prototype !== objectPrototype) ||
    reflectOwnKeys(descriptors).length !== 2 ||
    descriptors.rootPath === undefined ||
    descriptors.kind === undefined ||
    !objectHasOwn(descriptors.rootPath, "value") ||
    !objectHasOwn(descriptors.kind, "value") ||
    descriptors.rootPath.enumerable !== true ||
    descriptors.kind.enumerable !== true ||
    objectHasOwn(descriptors.rootPath, "get") ||
    objectHasOwn(descriptors.rootPath, "set") ||
    objectHasOwn(descriptors.kind, "get") ||
    objectHasOwn(descriptors.kind, "set")
  ) {
    fail("invalid_path");
  }
  const rootPath = descriptors.rootPath.value;
  const kind = descriptors.kind.value;
  if (
    !validAbsolutePath(rootPath) ||
    rootPath === "/" ||
    typeof kind !== "string" ||
    !objectHasOwn(CONTROL_FILE_NAMES, kind)
  ) {
    fail("invalid_path");
  }
  const before = await inspectFilesystemObjectInternal(state, rootPath);
  const control = await runFdOperationInternal(
    state,
    objectFreeze({
      device: before.identity.device,
      expectedControlFilesystemId: null,
      expectedControlObjectId: null,
      filesystemId: before.filesystem.filesystemId,
      inode: before.identity.inode,
      kind,
      objectId: before.identity.objectId,
      operation: "provision-control-root",
      rootPath,
    }),
  );
  const after = await inspectFilesystemObjectInternal(state, rootPath);
  if (
    after.filesystem.filesystemId !== before.filesystem.filesystemId ||
    after.identity.device !== before.identity.device ||
    after.identity.inode !== before.identity.inode ||
    after.identity.objectId !== before.identity.objectId ||
    control.kind !== kind ||
    control.controlFileIdentity.device !== after.identity.device ||
    control.controlFileIdentity.filesystemId !== after.filesystem.filesystemId ||
    control.controlFileIdentity.objectIdentityScheme !==
      after.filesystem.objectIdentityScheme
  ) {
    fail("operation_outcome_uncertain");
  }
  return objectFreeze({
    controlFileIdentity: control.controlFileIdentity,
    controlFileName: control.controlFileName,
    created: control.created,
    filesystem: after.filesystem,
    kind,
    rootIdentity: after.identity,
    rootPath,
    status: "ok",
  });
}

function octalDigit(code) {
  return code >= 0x30 && code <= 0x37;
}

function decodeMountPath(value) {
  const parts = [];
  let index = 0;
  let segmentStart = 0;
  while (index + 3 < value.length) {
    const first = stringCharCodeAt(value, index);
    const second = stringCharCodeAt(value, index + 1);
    const third = stringCharCodeAt(value, index + 2);
    const fourth = stringCharCodeAt(value, index + 3);
    if (
      first === 0x5c &&
      octalDigit(second) &&
      octalDigit(third) &&
      octalDigit(fourth)
    ) {
      if (segmentStart < index) {
        arrayPush(parts, stringSlice(value, segmentStart, index));
      }
      arrayPush(
        parts,
        callIntrinsic(stringFromCharCode, StringConstructor, [
          ((second - 0x30) << 6) |
            ((third - 0x30) << 3) |
            (fourth - 0x30),
        ]),
      );
      index += 4;
      segmentStart = index;
      continue;
    }
    index += 1;
  }
  if (segmentStart < value.length) {
    arrayPush(parts, stringSlice(value, segmentStart, value.length));
  }
  return arrayJoin(parts, "");
}

function parseMountInfoEntries(bytes) {
  const text = decodeUtf8(bytes);
  if (
    stringIncludes(text, "\0") ||
    stringIncludes(text, "\r") ||
    stringIncludes(text, "\ufeff")
  ) {
    fail("mountinfo_failed");
  }
  const lines = stringSplit(text, "\n");
  const entries = [];
  let rootSeen = false;
  for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
    const line = lines[lineIndex];
    if (line === "") continue;
    const fields = stringSplit(line, " ");
    if (fields.length < 10) fail("mountinfo_failed");
    let separatorIndex = -1;
    for (let fieldIndex = 0; fieldIndex < fields.length; fieldIndex += 1) {
      if (fields[fieldIndex] === "-") separatorIndex = fieldIndex;
    }
    if (separatorIndex < 6 || separatorIndex + 3 >= fields.length) {
      fail("mountinfo_failed");
    }
    const mountPoint = decodeMountPath(fields[4]);
    if (
      !validAbsolutePath(mountPoint) ||
      !regexpTest(DECIMAL_PATTERN, fields[0]) ||
      !regexpTest(/^(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)$/u, fields[2])
    ) {
      fail("mountinfo_failed");
    }
    if (mountPoint === "/") rootSeen = true;
    arrayPush(
      entries,
      objectFreeze({
        device: fields[2],
        filesystemType: fields[separatorIndex + 1],
        mountId: fields[0],
        mountPoint,
        source: decodeMountPath(fields[separatorIndex + 2]),
      }),
    );
  }
  if (!rootSeen) fail("mountinfo_failed");
  return objectFreeze(entries);
}

async function readMountInfoEntriesInternal(state) {
  let bytes;
  try {
    const pending = callIntrinsic(state.readMountInfo, undefined, [
      "/proc/self/mountinfo",
    ]);
    bytes = await assertNativePromise(pending, "mountinfo_failed");
  } catch {
    fail("mountinfo_failed");
  }
  if (
    isProxyValue(bytes) ||
    !isBufferValue(bytes) ||
    bufferBytes(bytes) > MOUNTINFO_LIMIT_BYTES
  ) {
    fail("mountinfo_failed");
  }
  try {
    return parseMountInfoEntries(bytes);
  } catch {
    fail("mountinfo_failed");
  }
}

async function listMountPointsInternal(state) {
  const entries = await readMountInfoEntriesInternal(state);
  const mountPoints = [];
  for (let index = 0; index < entries.length; index += 1) {
    arrayPush(mountPoints, entries[index].mountPoint);
  }
  return objectFreeze(mountPoints);
}

async function selectInspectionRoot(state, path) {
  const selectedTrusted = selectTrustedRoot(path, state.trustedRoots);
  const entries = await readMountInfoEntriesInternal(state);
  let selectedMount = selectedTrusted.root;
  let selectedCount = 0;
  for (let index = 0; index < entries.length; index += 1) {
    const mountPoint = entries[index].mountPoint;
    if (
      pathAtOrInside(selectedTrusted.root, mountPoint) &&
      pathAtOrInside(mountPoint, path)
    ) {
      if (mountPoint.length > selectedMount.length) {
        selectedMount = mountPoint;
        selectedCount = 1;
      } else if (mountPoint === selectedMount) {
        selectedCount += 1;
      }
    }
  }
  if (selectedCount > 1) fail("path_mismatch");
  const relativePath =
    selectedMount === path
      ? "."
      : stringSlice(
          path,
          selectedMount === "/" ? 1 : selectedMount.length + 1,
          path.length,
        );
  if (
    relativePath === "" ||
    stringStartsWith(relativePath, "/") ||
    relativePath === ".." ||
    stringStartsWith(relativePath, "../")
  ) {
    fail("path_mismatch");
  }
  return objectFreeze({ relativePath, root: selectedMount });
}

export class LinuxExt4Inspector {
  constructor(options) {
    if (arguments.length !== 1) {
      throw new TypeErrorConstructor("expected one options argument");
    }
    const state = normalizeOptions(options);
    const instance = this;

    const inspectFilesystem = function inspectFilesystem(path) {
      if (this !== instance) {
        throw new TypeErrorConstructor("invalid Linux ext4 inspector receiver");
      }
      if (arguments.length !== 1) {
        throw new TypeErrorConstructor("expected one path argument");
      }
      return inspectFilesystemInternal(state, path);
    };
    const inspectFilesystemObject = function inspectFilesystemObject(path) {
      if (this !== instance) {
        throw new TypeErrorConstructor("invalid Linux ext4 inspector receiver");
      }
      if (arguments.length !== 1) {
        throw new TypeErrorConstructor("expected one path argument");
      }
      return inspectFilesystemObjectInternal(state, path);
    };
    const inspectPersistentObjectIdentity =
      function inspectPersistentObjectIdentity(path) {
        if (this !== instance) {
          throw new TypeErrorConstructor("invalid Linux ext4 inspector receiver");
        }
        if (arguments.length !== 1) {
          throw new TypeErrorConstructor("expected one path argument");
        }
        return inspectPersistentObjectIdentityInternal(state, path);
      };
    const listMountPoints = function listMountPoints() {
      if (this !== instance) {
        throw new TypeErrorConstructor("invalid Linux ext4 inspector receiver");
      }
      if (arguments.length !== 0) {
        throw new TypeErrorConstructor("expected no arguments");
      }
      return listMountPointsInternal(state);
    };
    const provisionControlRoot = function provisionControlRoot(request) {
      if (this !== instance) {
        throw new TypeErrorConstructor("invalid Linux ext4 inspector receiver");
      }
      if (arguments.length !== 1) {
        throw new TypeErrorConstructor("expected one request argument");
      }
      return provisionControlRootInternal(state, request);
    };
    const runFdOperation = function runFdOperation(request) {
      if (this !== instance) {
        throw new TypeErrorConstructor("invalid Linux ext4 inspector receiver");
      }
      if (arguments.length !== 1) {
        throw new TypeErrorConstructor("expected one request argument");
      }
      return runFdOperationInternal(state, request);
    };

    objectFreeze(inspectFilesystem);
    objectFreeze(inspectFilesystemObject);
    objectFreeze(inspectPersistentObjectIdentity);
    objectFreeze(listMountPoints);
    objectFreeze(provisionControlRoot);
    objectFreeze(runFdOperation);
    objectDefineProperties(this, {
      inspectFilesystem: {
        enumerable: true,
        value: inspectFilesystem,
      },
      inspectFilesystemObject: {
        enumerable: true,
        value: inspectFilesystemObject,
      },
      inspectPersistentObjectIdentity: {
        enumerable: true,
        value: inspectPersistentObjectIdentity,
      },
      listMountPoints: {
        enumerable: true,
        value: listMountPoints,
      },
      provisionControlRoot: {
        enumerable: true,
        value: provisionControlRoot,
      },
      runFdOperation: {
        enumerable: true,
        value: runFdOperation,
      },
    });
    objectFreeze(this);
  }
}

export function createLinuxExt4Inspector(options) {
  if (arguments.length !== 1) {
    throw new TypeErrorConstructor("expected one options argument");
  }
  return new LinuxExt4Inspector(options);
}

objectFreeze(LinuxExt4Inspector.prototype);
objectFreeze(LinuxExt4Inspector);
objectFreeze(createLinuxExt4Inspector);
