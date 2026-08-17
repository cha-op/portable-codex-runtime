import { Buffer } from "node:buffer";
import { execFile } from "node:child_process";
import { Hash, createHash } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
  lstat,
  open,
  readFile,
  realpath,
} from "node:fs/promises";
import {
  basename,
  dirname,
  isAbsolute,
  parse,
  resolve,
  sep,
} from "node:path";
import { TextDecoder, promisify, types as utilTypes } from "node:util";

const execFileAsync = promisify(execFile);
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayPushIntrinsic = Array.prototype.push;
const arraySliceIntrinsic = Array.prototype.slice;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const bufferFromIntrinsic = Buffer.from;
const bufferIsBufferIntrinsic = Buffer.isBuffer;
const bigIntToStringIntrinsic = BigInt.prototype.toString;
const BigIntConstructor = BigInt;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const numberIsSafeIntegerIntrinsic = Number.isSafeInteger;
const MapConstructor = Map;
const mapDeleteIntrinsic = Map.prototype.delete;
const mapGetIntrinsic = Map.prototype.get;
const mapSetIntrinsic = Map.prototype.set;
const NumberConstructor = Number;
const objectCreateIntrinsic = Object.create;
const objectDefinePropertiesIntrinsic = Object.defineProperties;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic = Object.getOwnPropertyDescriptor;
const objectGetOwnPropertyDescriptorsIntrinsic = Object.getOwnPropertyDescriptors;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectPrototype = Object.prototype;
const promisePrototype = Promise.prototype;
const PromiseConstructor = Promise;
const promiseResolveIntrinsic = Promise.resolve;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const SetConstructor = Set;
const setAddIntrinsic = Set.prototype.add;
const setHasIntrinsic = Set.prototype.has;
const stringCharCodeAtIntrinsic = String.prototype.charCodeAt;
const stringEndsWithIntrinsic = String.prototype.endsWith;
const stringFromCharCodeIntrinsic = String.fromCharCode;
const stringIncludesIntrinsic = String.prototype.includes;
const stringSliceIntrinsic = String.prototype.slice;
const stringSplitIntrinsic = String.prototype.split;
const stringStartsWithIntrinsic = String.prototype.startsWith;
const StringConstructor = String;
const textDecoderDecodeIntrinsic = TextDecoder.prototype.decode;
const typedArrayPrototype = objectGetPrototypeOfIntrinsic(Uint8Array.prototype);
const typedArrayByteLengthGetter = objectGetOwnPropertyDescriptorIntrinsic(
  typedArrayPrototype,
  "byteLength",
).get;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const { isPromise: isPromiseValue, isProxy: isProxyValue } = utilTypes;

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApplyIntrinsic(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function arrayEvery(value, callback) {
  return callIntrinsic(arrayEveryIntrinsic, value, [callback]);
}

function arraySlice(value, start, end) {
  return callIntrinsic(arraySliceIntrinsic, value, [start, end]);
}

function arrayPush(value, item) {
  return callIntrinsic(arrayPushIntrinsic, value, [item]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function setAdd(value, item) {
  return callIntrinsic(setAddIntrinsic, value, [item]);
}

function setHas(value, item) {
  return callIntrinsic(setHasIntrinsic, value, [item]);
}

function stringEndsWith(value, suffix) {
  return callIntrinsic(stringEndsWithIntrinsic, value, [suffix]);
}

function stringCharCodeAt(value, index) {
  return callIntrinsic(stringCharCodeAtIntrinsic, value, [index]);
}

function stringFromCharCode(value) {
  return callIntrinsic(stringFromCharCodeIntrinsic, StringConstructor, [value]);
}

function stringIncludes(value, candidate) {
  return callIntrinsic(stringIncludesIntrinsic, value, [candidate]);
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

function weakSetAdd(value, item) {
  return callIntrinsic(weakSetAddIntrinsic, value, [item]);
}

function weakSetHas(value, item) {
  return callIntrinsic(weakSetHasIntrinsic, value, [item]);
}

function bufferBytes(value) {
  return callIntrinsic(typedArrayByteLengthGetter, value, []);
}

function mapDelete(map, key) {
  return callIntrinsic(mapDeleteIntrinsic, map, [key]);
}

function mapGet(map, key) {
  return callIntrinsic(mapGetIntrinsic, map, [key]);
}

function mapSet(map, key, value) {
  return callIntrinsic(mapSetIntrinsic, map, [key, value]);
}

function bigIntToString(value) {
  return callIntrinsic(bigIntToStringIntrinsic, value, []);
}

export const LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION = 1;

const DEFAULT_EXECUTABLES = objectFreezeIntrinsic({
  getfacl: "/usr/bin/getfacl",
  losetup: "/usr/sbin/losetup",
  mkfsExt4: "/usr/sbin/mkfs.ext4",
  mount: "/usr/bin/mount",
  sync: "/usr/bin/sync",
  umount: "/usr/bin/umount",
});
const DEFAULT_PLATFORM = process.platform;
const DEFAULT_UID = typeof process.getuid === "function" ? process.getuid() : null;
const DEFAULT_UID_BIGINT = DEFAULT_UID === null ? null : BigInt(DEFAULT_UID);
const FILE_TYPE_MASK = BigInt(fsConstants.S_IFMT);
const DIRECTORY_FILE_TYPE = BigInt(fsConstants.S_IFDIR);
const REGULAR_FILE_TYPE = BigInt(fsConstants.S_IFREG);
const IMAGE_MODE = 0o600;
const DIRECTORY_MODE = 0o700;
const MAX_COMMAND_OUTPUT_BYTES = 64 * 1024;
const MAX_MOUNTINFO_BYTES = 1024 * 1024;
const MAX_PATH_BYTES = 4095;
const MAX_IMAGE_SIZE_BYTES = 8 * 1024 * 1024 * 1024 * 1024;
const MIN_IMAGE_SIZE_BYTES = 1024 * 1024;
const COMMAND_TIMEOUT_MILLISECONDS = 60_000;
const REQUIRED_VFS_OPTIONS = objectFreezeIntrinsic([
  "rw",
  "nosuid",
  "nodev",
  "noexec",
  "noatime",
]);
const LOOP_DEVICE_PATTERN =
  /^\/dev\/loop(?:0|[1-9][0-9]{0,2}|[1-3][0-9]{3}|40(?:[0-8][0-9]|9[0-5]))$/u;
const PROC_FD_MOUNT_SOURCE_PATTERN = /^\/proc\/self\/fd\/[1-9][0-9]*$/u;
const DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;
const DEVICE_PATTERN = /^(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)$/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const CONTROL_PATTERN = /[\u0000-\u001f\u007f]/u;
const DIRECT_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$/u;

const OPTION_KEYS = objectFreezeIntrinsic([
  "commandRunner",
  "getfaclExecutable",
  "inspector",
  "losetupExecutable",
  "mkfsExt4Executable",
  "mountExecutable",
  "platform",
  "readMountInfo",
  "syncExecutable",
  "umountExecutable",
]);
const PROVISION_KEYS = objectFreezeIntrinsic([
  "imagePath",
  "imageSizeBytes",
  "mountPath",
]);
const MOUNT_KEYS = objectFreezeIntrinsic(["imagePath", "mountPath"]);
const PUBLICATION_KEYS = objectFreezeIntrinsic([
  "expectedPublicationControlIdentity",
  "imagePath",
  "mountPath",
]);
const PUBLICATION_CONTROL_IDENTITY_KEYS = objectFreezeIntrinsic([
  "filesystemId",
  "objectIdentityScheme",
  "objectId",
]);
const ATTACHMENT_KEYS = objectFreezeIntrinsic([
  "attachmentRootPath",
  "imagePath",
  "mountPath",
]);
const FILESYSTEM_KEYS = objectFreezeIntrinsic([
  "durability",
  "filesystemId",
  "objectIdentityScheme",
  "type",
]);
const INSPECTED_IDENTITY_KEYS = objectFreezeIntrinsic([
  "device",
  "inode",
  "objectId",
]);
const INSPECTED_OBJECT_KEYS = objectFreezeIntrinsic(["filesystem", "identity"]);
const FD_STATUS_KEYS = objectFreezeIntrinsic(["status"]);
const FD_CREATED_IMAGE_KEYS = objectFreezeIntrinsic(["device", "inode", "status"]);
const FD_DIRECTORY_KEYS = objectFreezeIntrinsic([
  "created",
  "device",
  "inode",
  "status",
]);
const FD_CONTROL_KEYS = objectFreezeIntrinsic([
  "controlFileName",
  "created",
  "controlFileIdentity",
  "kind",
  "status",
]);
const CONTROL_FILE_IDENTITY_KEYS = objectFreezeIntrinsic([
  "device",
  "inode",
  "filesystemId",
  "objectIdentityScheme",
  "objectId",
]);
const FD_LOOP_KEYS = objectFreezeIntrinsic([
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
const COMMAND_RESULT_KEYS = objectFreezeIntrinsic(["stderr", "stdout"]);

const SAFE_COMMAND_ENVIRONMENT = frozenRecord({
  LANG: "C",
  LC_ALL: "C",
});
const COMMAND_OPTIONS = frozenRecord({
  cwd: "/",
  encoding: "buffer",
  env: SAFE_COMMAND_ENVIRONMENT,
  killSignal: "SIGTERM",
  maxBuffer: MAX_COMMAND_OUTPUT_BYTES,
  shell: false,
  timeout: COMMAND_TIMEOUT_MILLISECONDS,
  windowsHide: true,
});

const ERROR_MESSAGES = objectFreezeIntrinsic({
  access_policy_mismatch: "Linux ext4 path access policy does not match",
  attachment_root_absent: "Linux ext4 attachment root is absent",
  attachment_root_unsafe: "Linux ext4 attachment root is unsafe",
  backing_mismatch: "Linux ext4 loop device does not back the expected image",
  image_exists: "Linux ext4 image already exists",
  image_io_failed: "Linux ext4 image I/O failed",
  inspection_failed: "Linux ext4 identity inspection failed",
  invalid_options: "Linux ext4 image driver options are invalid",
  invalid_request: "Linux ext4 image driver request is invalid",
  loop_ambiguous: "Linux ext4 image has ambiguous loop attachments",
  mount_absent: "Linux ext4 mount is absent",
  mount_mismatch: "Linux ext4 mount does not match the expected image mount",
  mount_path_exists: "Linux ext4 mount path already exists",
  operation_outcome_uncertain: "Linux ext4 image operation outcome is uncertain",
  observation_failed: "Linux ext4 mount observation failed",
  unsupported_platform: "Linux ext4 image driver requires Linux",
});
const internalErrors = new WeakSet();
const imageOperationTails = new MapConstructor();

export class LinuxExt4ImageDriverError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwnIntrinsic(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported Linux ext4 image driver error");
    }
    super(ERROR_MESSAGES[code]);
    objectDefinePropertiesIntrinsic(this, {
      code: {
        configurable: true,
        enumerable: true,
        value: code,
        writable: true,
      },
      name: {
        configurable: true,
        enumerable: true,
        value: "LinuxExt4ImageDriverError",
        writable: true,
      },
      retryable: {
        configurable: true,
        enumerable: true,
        value: false,
        writable: true,
      },
    });
    objectFreezeIntrinsic(this);
  }
}

function frozenRecord(values) {
  const result = objectCreateIntrinsic(null);
  const descriptors = objectGetOwnPropertyDescriptorsIntrinsic(values);
  const keys = reflectOwnKeysIntrinsic(descriptors);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    result[key] = descriptors[key].value;
  }
  return objectFreezeIntrinsic(result);
}

function createError(code) {
  const error = new LinuxExt4ImageDriverError(code);
  weakSetAdd(internalErrors, error);
  return error;
}

function fail(code) {
  throw createError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactDataObject(value, keys, code) {
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  let prototype;
  let descriptors;
  try {
    prototype = objectGetPrototypeOfIntrinsic(value);
    descriptors = objectGetOwnPropertyDescriptorsIntrinsic(value);
  } catch {
    fail(code);
  }
  ensure(prototype === objectPrototype || prototype === null, code);
  const actual = reflectOwnKeysIntrinsic(descriptors);
  ensure(actual.length === keys.length, code);
  const result = objectCreateIntrinsic(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    ensure(
      typeof key === "string" &&
        arrayIncludes(keys, key) &&
        descriptors[key].enumerable === true &&
        objectHasOwnIntrinsic(descriptors[key], "value") &&
        !objectHasOwnIntrinsic(descriptors[key], "get") &&
        !objectHasOwnIntrinsic(descriptors[key], "set"),
      code,
    );
    result[key] = descriptors[key].value;
  }
  for (let index = 0; index < keys.length; index += 1) {
    ensure(objectHasOwnIntrinsic(result, keys[index]), code);
  }
  return result;
}

function optionsDataObject(value) {
  const code = "invalid_options";
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  let prototype;
  let descriptors;
  try {
    prototype = objectGetPrototypeOfIntrinsic(value);
    descriptors = objectGetOwnPropertyDescriptorsIntrinsic(value);
  } catch {
    fail(code);
  }
  ensure(prototype === objectPrototype || prototype === null, code);
  const actual = reflectOwnKeysIntrinsic(descriptors);
  const result = objectCreateIntrinsic(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    ensure(
      typeof key === "string" &&
        arrayIncludes(OPTION_KEYS, key) &&
        descriptors[key].enumerable === true &&
        objectHasOwnIntrinsic(descriptors[key], "value") &&
        !objectHasOwnIntrinsic(descriptors[key], "get") &&
        !objectHasOwnIntrinsic(descriptors[key], "set"),
      code,
    );
    result[key] = descriptors[key].value;
  }
  ensure(objectHasOwnIntrinsic(result, "inspector"), code);
  return result;
}

function validPath(value) {
  if (
    typeof value !== "string" ||
    value.length <= 1 ||
    value.length > MAX_PATH_BYTES ||
    regexpTest(CONTROL_PATTERN, value)
  ) {
    return false;
  }
  let bytes;
  let decoded;
  try {
    bytes = callIntrinsic(bufferFromIntrinsic, Buffer, [value, "utf8"]);
    decoded = decodeUtf8(bytes, "invalid_request");
  } catch {
    return false;
  }
  return (
    decoded === value &&
    callIntrinsic(bufferByteLengthIntrinsic, Buffer, [value, "utf8"]) <=
      MAX_PATH_BYTES &&
    isAbsolute(value) &&
    resolve(value) === value &&
    value !== parse(value).root &&
    !stringEndsWith(value, sep)
  );
}

function executablePath(value, code) {
  ensure(validPath(value), code);
  return value;
}

function functionDataProperty(value, key, code) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function") ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
  } catch {
    fail(code);
  }
  ensure(
    descriptor !== undefined &&
      objectHasOwnIntrinsic(descriptor, "value") &&
      !objectHasOwnIntrinsic(descriptor, "get") &&
      !objectHasOwnIntrinsic(descriptor, "set") &&
      typeof descriptor.value === "function" &&
      !isProxyValue(descriptor.value),
    code,
  );
  return descriptor.value;
}

function normalizeInspector(value) {
  const code = "invalid_options";
  return objectFreezeIntrinsic({
    inspectFilesystemObject: functionDataProperty(
      value,
      "inspectFilesystemObject",
      code,
    ),
    receiver: value,
    runFdOperation: functionDataProperty(value, "runFdOperation", code),
  });
}

function assertFunction(value, code) {
  ensure(typeof value === "function" && !isProxyValue(value), code);
  return value;
}

function normalizeOptions(value) {
  const options = optionsDataObject(value);
  const platform = options.platform ?? DEFAULT_PLATFORM;
  ensure(typeof platform === "string", "invalid_options");
  if (platform !== "linux") fail("unsupported_platform");
  ensure(DEFAULT_UID !== null, "unsupported_platform");
  return objectFreezeIntrinsic({
    commandRunner:
      options.commandRunner === undefined
        ? defaultCommandRunner
        : assertFunction(options.commandRunner, "invalid_options"),
    executables: objectFreezeIntrinsic({
      getfacl: executablePath(
        options.getfaclExecutable ?? DEFAULT_EXECUTABLES.getfacl,
        "invalid_options",
      ),
      losetup: executablePath(
        options.losetupExecutable ?? DEFAULT_EXECUTABLES.losetup,
        "invalid_options",
      ),
      mkfsExt4: executablePath(
        options.mkfsExt4Executable ?? DEFAULT_EXECUTABLES.mkfsExt4,
        "invalid_options",
      ),
      mount: executablePath(
        options.mountExecutable ?? DEFAULT_EXECUTABLES.mount,
        "invalid_options",
      ),
      sync: executablePath(
        options.syncExecutable ?? DEFAULT_EXECUTABLES.sync,
        "invalid_options",
      ),
      umount: executablePath(
        options.umountExecutable ?? DEFAULT_EXECUTABLES.umount,
        "invalid_options",
      ),
    }),
    inspector: normalizeInspector(options.inspector),
    readMountInfo:
      options.readMountInfo === undefined
        ? readFile
        : assertFunction(options.readMountInfo, "invalid_options"),
  });
}

function mountRequest(value) {
  const request = exactDataObject(value, MOUNT_KEYS, "invalid_request");
  const imagePath = request.imagePath;
  const mountPath = request.mountPath;
  ensure(
    validPath(imagePath) &&
      validPath(mountPath) &&
      imagePath !== mountPath &&
      !stringStartsWith(imagePath, `${mountPath}${sep}`) &&
      !stringStartsWith(mountPath, `${imagePath}${sep}`),
    "invalid_request",
  );
  return frozenRecord({ imagePath, mountPath });
}

function publicationRequest(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    isProxyValue(value)
  ) {
    fail("invalid_request");
  }
  let descriptors;
  try {
    descriptors = objectGetOwnPropertyDescriptorsIntrinsic(value);
  } catch {
    fail("invalid_request");
  }
  const hasExpected =
    objectHasOwnIntrinsic(descriptors, "expectedPublicationControlIdentity");
  if (!hasExpected) {
    const base = mountRequest(value);
    return frozenRecord({
      expectedPublicationControlIdentity: null,
      imagePath: base.imagePath,
      mountPath: base.mountPath,
    });
  }
  const request = exactDataObject(value, PUBLICATION_KEYS, "invalid_request");
  const base = mountRequest(
    frozenRecord({ imagePath: request.imagePath, mountPath: request.mountPath }),
  );
  if (request.expectedPublicationControlIdentity === null) {
    return frozenRecord({
      expectedPublicationControlIdentity: null,
      imagePath: base.imagePath,
      mountPath: base.mountPath,
    });
  }
  const expected = exactDataObject(
    request.expectedPublicationControlIdentity,
    PUBLICATION_CONTROL_IDENTITY_KEYS,
    "invalid_request",
  );
  ensure(
    typeof expected.filesystemId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, expected.filesystemId) &&
      typeof expected.objectId === "string" &&
      expected.objectId.length > 0 &&
      expected.objectId.length <= 512 &&
      expected.objectIdentityScheme === "linux-ext4-file-handle-sha256-v1",
    "invalid_request",
  );
  return frozenRecord({
    expectedPublicationControlIdentity: frozenRecord({
      filesystemId: expected.filesystemId,
      objectIdentityScheme: expected.objectIdentityScheme,
      objectId: expected.objectId,
    }),
    imagePath: base.imagePath,
    mountPath: base.mountPath,
  });
}

function provisionRequest(value) {
  const request = exactDataObject(value, PROVISION_KEYS, "invalid_request");
  const base = mountRequest(
    frozenRecord({ imagePath: request.imagePath, mountPath: request.mountPath }),
  );
  ensure(
    numberIsSafeIntegerIntrinsic(request.imageSizeBytes) &&
      request.imageSizeBytes >= MIN_IMAGE_SIZE_BYTES &&
      request.imageSizeBytes <= MAX_IMAGE_SIZE_BYTES &&
      request.imageSizeBytes % 512 === 0,
    "invalid_request",
  );
  return frozenRecord({
    imagePath: base.imagePath,
    imageSizeBytes: request.imageSizeBytes,
    mountPath: base.mountPath,
  });
}

function attachmentRequest(value) {
  const request = exactDataObject(value, ATTACHMENT_KEYS, "invalid_request");
  const base = mountRequest(
    frozenRecord({ imagePath: request.imagePath, mountPath: request.mountPath }),
  );
  ensure(
    validPath(request.attachmentRootPath) &&
      dirname(request.attachmentRootPath) === base.mountPath &&
      basename(request.attachmentRootPath) !== "." &&
      basename(request.attachmentRootPath) !== ".." &&
      regexpTest(DIRECT_NAME_PATTERN, basename(request.attachmentRootPath)),
    "invalid_request",
  );
  return frozenRecord({
    attachmentRootPath: request.attachmentRootPath,
    imagePath: base.imagePath,
    mountPath: base.mountPath,
  });
}

function isNativePromise(value) {
  if (isProxyValue(value) || !isPromiseValue(value)) return false;
  try {
    return (
      objectGetPrototypeOfIntrinsic(value) === promisePrototype &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "then") === undefined &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "catch") === undefined &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "finally") === undefined &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "constructor") === undefined
    );
  } catch {
    return false;
  }
}

function defaultCommandRunner(executable, arguments_, options) {
  return execFileAsync(executable, arguments_, options);
}

function boundedCommandResult(value, code) {
  const result = exactDataObject(value, COMMAND_RESULT_KEYS, code);
  ensure(
    !isProxyValue(result.stdout) &&
      !isProxyValue(result.stderr) &&
      callIntrinsic(bufferIsBufferIntrinsic, Buffer, [result.stdout]) &&
      callIntrinsic(bufferIsBufferIntrinsic, Buffer, [result.stderr]) &&
      bufferBytes(result.stdout) <= MAX_COMMAND_OUTPUT_BYTES &&
      bufferBytes(result.stderr) <= MAX_COMMAND_OUTPUT_BYTES &&
      bufferBytes(result.stderr) === 0,
    code,
  );
  return result;
}

async function runCommand(state, executable, arguments_, mutation) {
  const args = objectFreezeIntrinsic(arraySlice(arguments_, 0));
  let pending;
  try {
    pending = callIntrinsic(state.commandRunner, undefined, [
      executable,
      args,
      COMMAND_OPTIONS,
    ]);
  } catch {
    fail(mutation ? "operation_outcome_uncertain" : "observation_failed");
  }
  ensure(
    isNativePromise(pending),
    mutation ? "operation_outcome_uncertain" : "observation_failed",
  );
  let raw;
  try {
    raw = await pending;
  } catch {
    fail(mutation ? "operation_outcome_uncertain" : "observation_failed");
  }
  return boundedCommandResult(
    raw,
    mutation ? "operation_outcome_uncertain" : "observation_failed",
  );
}

async function invokeFdOperation(
  state,
  request,
  failureCode = "operation_outcome_uncertain",
  mismatchCode = failureCode,
  existsCode = failureCode,
) {
  let pending;
  try {
    pending = callIntrinsic(
      state.inspector.runFdOperation,
      state.inspector.receiver,
      [request],
    );
  } catch {
    fail(failureCode);
  }
  ensure(isNativePromise(pending), failureCode);
  try {
    return await pending;
  } catch (error) {
    if (safeErrorCode(error) === "path_mismatch") fail(mismatchCode);
    if (safeErrorCode(error) === "path_exists") fail(existsCode);
    fail(failureCode);
  }
}

function normalizeFdStatus(value) {
  const result = exactDataObject(
    value,
    FD_STATUS_KEYS,
    "operation_outcome_uncertain",
  );
  ensure(result.status === "ok", "operation_outcome_uncertain");
  return frozenRecord({ status: "ok" });
}

function normalizeDirectoryReceipt(value) {
  const result = exactDataObject(
    value,
    FD_DIRECTORY_KEYS,
    "operation_outcome_uncertain",
  );
  ensure(
    typeof result.created === "boolean" && result.status === "ok",
    "operation_outcome_uncertain",
  );
  ensure(
    typeof result.device === "string" &&
      regexpTest(DECIMAL_PATTERN, result.device) &&
      typeof result.inode === "string" &&
      regexpTest(/^[1-9][0-9]*$/u, result.inode),
    "operation_outcome_uncertain",
  );
  return frozenRecord({
    created: result.created,
    device: result.device,
    inode: result.inode,
    status: "ok",
  });
}

function normalizeCreatedImageReceipt(value) {
  const result = exactDataObject(
    value,
    FD_CREATED_IMAGE_KEYS,
    "operation_outcome_uncertain",
  );
  ensure(
    typeof result.device === "string" &&
      regexpTest(DECIMAL_PATTERN, result.device) &&
      typeof result.inode === "string" &&
      regexpTest(/^[1-9][0-9]*$/u, result.inode) &&
      result.status === "ok",
    "operation_outcome_uncertain",
  );
  return frozenRecord({
    device: result.device,
    inode: result.inode,
    status: "ok",
  });
}

function normalizeControlReceipt(value) {
  const result = exactDataObject(
    value,
    FD_CONTROL_KEYS,
    "operation_outcome_uncertain",
  );
  const identity = exactDataObject(
    result.controlFileIdentity,
    CONTROL_FILE_IDENTITY_KEYS,
    "operation_outcome_uncertain",
  );
  ensure(
    result.controlFileName === ".stopped-directory-publication.lock" &&
      typeof result.created === "boolean" &&
      typeof identity.device === "string" &&
      regexpTest(DECIMAL_PATTERN, identity.device) &&
      typeof identity.filesystemId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, identity.filesystemId) &&
      typeof identity.inode === "string" &&
      regexpTest(/^[1-9][0-9]*$/u, identity.inode) &&
      typeof identity.objectId === "string" &&
      identity.objectId.length > 0 &&
      identity.objectId.length <= 512 &&
      identity.objectIdentityScheme === "linux-ext4-file-handle-sha256-v1" &&
      result.kind === "publication" &&
      result.status === "ok",
    "operation_outcome_uncertain",
  );
  return frozenRecord({
    controlFileName: result.controlFileName,
    created: result.created,
    controlFileIdentity: frozenRecord({
      device: identity.device,
      inode: identity.inode,
      filesystemId: identity.filesystemId,
      objectIdentityScheme: identity.objectIdentityScheme,
      objectId: identity.objectId,
    }),
    kind: result.kind,
    status: "ok",
  });
}

function normalizeLoopReceipt(value, expectedStatus, code) {
  const result = exactDataObject(
    value,
    FD_LOOP_KEYS,
    code,
  );
  ensure(
    typeof result.backingDevice === "string" &&
      regexpTest(DECIMAL_PATTERN, result.backingDevice) &&
      typeof result.backingInode === "string" &&
      regexpTest(/^[1-9][0-9]*$/u, result.backingInode) &&
      result.blockSize === "512" &&
      typeof result.loopDevice === "string" &&
      regexpTest(LOOP_DEVICE_PATTERN, result.loopDevice) &&
      typeof result.loopRdev === "string" &&
      regexpTest(DEVICE_PATTERN, result.loopRdev) &&
      result.offset === "0" &&
      result.readOnly === false &&
      typeof result.sizeBytes === "string" &&
      regexpTest(/^[1-9][0-9]*$/u, result.sizeBytes) &&
      result.sizeLimit === "0" &&
      result.status === expectedStatus,
    code,
  );
  return frozenRecord({
    backingDevice: result.backingDevice,
    backingInode: result.backingInode,
    blockSize: "512",
    loopDevice: result.loopDevice,
    loopRdev: result.loopRdev,
    offset: "0",
    readOnly: false,
    sizeBytes: result.sizeBytes,
    sizeLimit: "0",
    status: result.status,
  });
}

function isAbsentLoopReceipt(value) {
  try {
    const record = exactDataObject(value, FD_STATUS_KEYS, "observation_failed");
    return record.status === "absent";
  } catch (error) {
    if (weakSetHas(internalErrors, error)) return false;
    throw error;
  }
}

function decodeUtf8(value, code) {
  try {
    return callIntrinsic(textDecoderDecodeIntrinsic, utf8Decoder, [value]);
  } catch {
    fail(code);
  }
}

const utf8Decoder = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: true,
});

async function attachLoop(
  state,
  imagePath,
  parentAuthority = undefined,
  imageAuthority = undefined,
) {
  return withImageAuthorities(
    state,
    imagePath,
    parentAuthority,
    imageAuthority,
    async (parent, image) =>
      normalizeLoopReceipt(
        await invokeFdOperation(
          state,
          frozenRecord({
            device: authorityDevice(image),
            inode: authorityInode(image),
            operation: "attach-loop",
            parentDevice: authorityDevice(parent),
            parentInode: authorityInode(parent),
            path: imagePath,
          }),
        ),
        "attached",
        "operation_outcome_uncertain",
      ),
  );
}

async function readLoopBacking(
  state,
  loopDevice,
  imagePath,
  parentAuthority = undefined,
  imageAuthority = undefined,
) {
  return withImageAuthorities(
    state,
    imagePath,
    parentAuthority,
    imageAuthority,
    async (parent, image) =>
      normalizeLoopReceipt(
        await invokeFdOperation(
          state,
          frozenRecord({
            device: authorityDevice(image),
            inode: authorityInode(image),
            loopDevice,
            operation: "inspect-loop",
            parentDevice: authorityDevice(parent),
            parentInode: authorityInode(parent),
            path: imagePath,
          }),
          "observation_failed",
          "backing_mismatch",
        ),
        "present",
        "observation_failed",
      ),
  );
}

async function loopsForImage(
  state,
  imagePath,
  parentAuthority = undefined,
  imageAuthority = undefined,
) {
  return withImageAuthorities(
    state,
    imagePath,
    parentAuthority,
    imageAuthority,
    async (parent, image) => {
      const raw = await invokeFdOperation(
        state,
        frozenRecord({
          device: authorityDevice(image),
          inode: authorityInode(image),
          operation: "find-loop",
          parentDevice: authorityDevice(parent),
          parentInode: authorityInode(parent),
          path: imagePath,
        }),
        "observation_failed",
        "loop_ambiguous",
      );
      if (isAbsentLoopReceipt(raw)) return objectFreezeIntrinsic([]);
      return objectFreezeIntrinsic([
        normalizeLoopReceipt(raw, "present", "observation_failed"),
      ]);
    },
  );
}

function safeErrorCode(error) {
  if (
    error === null ||
    (typeof error !== "object" && typeof error !== "function") ||
    isProxyValue(error)
  ) {
    return undefined;
  }
  try {
    const descriptor = objectGetOwnPropertyDescriptorIntrinsic(error, "code");
    return descriptor !== undefined && objectHasOwnIntrinsic(descriptor, "value")
      ? descriptor.value
      : undefined;
  } catch {
    return undefined;
  }
}

function isDirectoryStat(metadata) {
  return (
    typeof metadata?.mode === "bigint" &&
    (metadata.mode & FILE_TYPE_MASK) === DIRECTORY_FILE_TYPE
  );
}

function isRegularFileStat(metadata) {
  return (
    typeof metadata?.mode === "bigint" &&
    (metadata.mode & FILE_TYPE_MASK) === REGULAR_FILE_TYPE
  );
}

function statMode(metadata) {
  return callIntrinsic(NumberConstructor, undefined, [metadata.mode & 0o777n]);
}

function sameRuntimeIdentity(left, right) {
  return left.dev === right.dev && left.ino === right.ino;
}

function authorityDevice(authority) {
  return bigIntToString(authority.identity.dev);
}

function authorityInode(authority) {
  return bigIntToString(authority.identity.ino);
}

// Path hardening protects two separate properties. Runtime dev+ino comparisons
// prove that a pathname and retained handle still name the same object; they
// are never persisted as the image incarnation. Type, owner, exact private
// mode, link count, and absence of an extended ACL prove the local access
// policy. We deliberately do not treat ctime, directory size, or child-entry
// churn as replacement evidence.

const ACL_ENTRY_PATTERN =
  /^(default:)?(user|group|mask|other):([^:]*):[rwx-]{3}(?:\s+#effective:[rwx-]{3})?$/u;

function parseExtendedAcl(bytes, code) {
  const output = decodeUtf8(bytes, code);
  ensure(
    output.length > 0 &&
      stringEndsWith(output, "\n") &&
      !stringIncludes(output, "\0") &&
      !stringIncludes(output, "\r"),
    code,
  );
  const lines = stringSplit(stringSlice(output, 0, -1), "\n");
  let hasUser = false;
  let hasGroup = false;
  let hasOther = false;
  let extended = false;
  for (let index = 0; index < lines.length; index += 1) {
    if (lines[index] === "") continue;
    const match = callIntrinsic(regexpExecIntrinsic, ACL_ENTRY_PATTERN, [
      lines[index],
    ]);
    ensure(match !== null, code);
    const defaultEntry = match[1] !== undefined;
    const kind = match[2];
    const qualifier = match[3];
    if (defaultEntry || kind === "mask" || qualifier !== "") {
      extended = true;
      continue;
    }
    if (kind === "user") {
      ensure(!hasUser, code);
      hasUser = true;
    } else if (kind === "group") {
      ensure(!hasGroup, code);
      hasGroup = true;
    } else if (kind === "other") {
      ensure(!hasOther, code);
      hasOther = true;
    }
  }
  ensure(hasUser && hasGroup && hasOther, code);
  return extended;
}

async function pathHasExtendedAcl(state, path, code) {
  let result;
  try {
    result = await runCommand(
      state,
      state.executables.getfacl,
      ["--absolute-names", "--omit-header", "--", path],
      false,
    );
  } catch (error) {
    if (weakSetHas(internalErrors, error)) fail(code);
    throw error;
  }
  return parseExtendedAcl(result.stdout, code);
}

function statHasPolicy(metadata, type, mode) {
  return (
    metadata.uid === DEFAULT_UID_BIGINT &&
    metadata.nlink >= 1n &&
    statMode(metadata) === mode &&
    (type === "directory"
      ? isDirectoryStat(metadata)
      : isRegularFileStat(metadata) && metadata.nlink === 1n)
  );
}

async function validateOwnedPath(state, path, type, mode, code) {
  let before;
  let after;
  try {
    before = await lstat(path, { bigint: true });
  } catch {
    fail(code);
  }
  ensure(statHasPolicy(before, type, mode), "access_policy_mismatch");
  ensure(!(await pathHasExtendedAcl(state, path, code)), "access_policy_mismatch");
  try {
    after = await lstat(path, { bigint: true });
  } catch {
    fail(code);
  }
  ensure(
    sameRuntimeIdentity(before, after) && statHasPolicy(after, type, mode),
    code,
  );
  return after;
}

async function openDirectoryAuthority(state, path, code) {
  try {
    ensure((await realpath(path)) === path, "access_policy_mismatch");
  } catch (error) {
    if (weakSetHas(internalErrors, error)) throw error;
    fail(code);
  }
  const before = await validateOwnedPath(
    state,
    path,
    "directory",
    DIRECTORY_MODE,
    code,
  );
  let handle;
  try {
    handle = await open(
      path,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    const held = await handle.stat({ bigint: true });
    ensure(
      sameRuntimeIdentity(before, held) &&
        statHasPolicy(held, "directory", DIRECTORY_MODE),
      code,
    );
    const current = await validateOwnedPath(
      state,
      path,
      "directory",
      DIRECTORY_MODE,
      code,
    );
    ensure(sameRuntimeIdentity(before, current), code);
  } catch (error) {
    if (handle !== undefined) {
      try {
        await handle.close();
      } catch {
        // The authority never escaped, so closing remains best effort.
      }
    }
    if (weakSetHas(internalErrors, error)) throw error;
    fail(code);
  }
  return { handle, identity: before, path };
}

async function assertDirectoryAuthorityCurrent(state, authority, code) {
  let held;
  try {
    held = await authority.handle.stat({ bigint: true });
  } catch {
    fail(code);
  }
  const current = await validateOwnedPath(
    state,
    authority.path,
    "directory",
    DIRECTORY_MODE,
    code,
  );
  ensure(
    sameRuntimeIdentity(authority.identity, held) &&
      sameRuntimeIdentity(authority.identity, current),
    code,
  );
}

async function closeAuthority(authority) {
  if (authority === undefined) return;
  try {
    await authority.handle.close();
  } catch {
    // Closing a retained read-only pin does not alter the operation outcome.
  }
}

async function assertImageAuthorityCurrent(state, authority, code) {
  let held;
  try {
    held = await authority.handle.stat({ bigint: true });
  } catch {
    fail(code);
  }
  const current = await validateOwnedPath(
    state,
    authority.path,
    "file",
    IMAGE_MODE,
    code,
  );
  ensure(
    sameRuntimeIdentity(authority.identity, held) &&
      sameRuntimeIdentity(authority.identity, current),
    code,
  );
}

async function openImageAuthority(state, imagePath, code) {
  let handle;
  let identity;
  try {
    handle = await open(
      imagePath,
      fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW,
    );
    const metadata = await handle.stat({ bigint: true });
    identity = metadata;
    ensure(
      isRegularFileStat(metadata) &&
        metadata.nlink === 1n &&
        metadata.uid === DEFAULT_UID_BIGINT &&
        statMode(metadata) === IMAGE_MODE,
      "access_policy_mismatch",
    );
  } catch (error) {
    if (handle !== undefined) {
      try {
        await handle.close();
      } catch {
        // The authority never escaped, so closing remains best effort.
      }
    }
    if (weakSetHas(internalErrors, error)) throw error;
    fail(code);
  }
  const authority = {
    handle,
    identity,
    path: imagePath,
  };
  try {
    await assertImageAuthorityCurrent(state, authority, code);
    return authority;
  } catch (error) {
    await closeAuthority(authority);
    throw error;
  }
}

async function withImageAuthorities(
  state,
  imagePath,
  parentAuthority,
  imageAuthority,
  operation,
) {
  let ownedParent;
  let ownedImage;
  try {
    const parent =
      parentAuthority ??
      (ownedParent = await openDirectoryAuthority(
        state,
        dirname(imagePath),
        "observation_failed",
      ));
    const image =
      imageAuthority ??
      (ownedImage = await openImageAuthority(
        state,
        imagePath,
        "observation_failed",
      ));
    await assertDirectoryAuthorityCurrent(state, parent, "observation_failed");
    await assertImageAuthorityCurrent(state, image, "observation_failed");
    return await operation(parent, image);
  } finally {
    await closeAuthority(ownedImage);
    await closeAuthority(ownedParent);
  }
}

async function createImage(state, imagePath, size, parentAuthority) {
  await assertDirectoryAuthorityCurrent(
    state,
    parentAuthority,
    "operation_outcome_uncertain",
  );
  const created = normalizeCreatedImageReceipt(
    await invokeFdOperation(
      state,
      frozenRecord({
        operation: "create-image",
        parentDevice: authorityDevice(parentAuthority),
        parentInode: authorityInode(parentAuthority),
        path: imagePath,
        sizeBytes: size,
      }),
      "operation_outcome_uncertain",
      "operation_outcome_uncertain",
      "image_exists",
    ),
  );
  const authority = await openImageAuthority(
    state,
    imagePath,
    "operation_outcome_uncertain",
  );
  try {
    const metadata = await authority.handle.stat({ bigint: true });
    ensure(
      metadata.size === callIntrinsic(BigIntConstructor, undefined, [size]) &&
        created.device === bigIntToString(metadata.dev) &&
        created.inode === bigIntToString(metadata.ino),
      "operation_outcome_uncertain",
    );
    await assertImageAuthorityCurrent(
      state,
      authority,
      "operation_outcome_uncertain",
    );
    await parentAuthority.handle.sync();
    await assertDirectoryAuthorityCurrent(
      state,
      parentAuthority,
      "operation_outcome_uncertain",
    );
    return authority;
  } catch (error) {
    await closeAuthority(authority);
    throw error;
  }
}

async function createMountPath(state, mountPath, parentAuthority) {
  await assertDirectoryAuthorityCurrent(
    state,
    parentAuthority,
    "operation_outcome_uncertain",
  );
  const receipt = normalizeDirectoryReceipt(
    await invokeFdOperation(
      state,
      frozenRecord({
        exclusive: true,
        operation: "create-directory",
        parentDevice: authorityDevice(parentAuthority),
        parentInode: authorityInode(parentAuthority),
        path: mountPath,
      }),
    ),
  );
  ensure(receipt.created, "operation_outcome_uncertain");
  const authority = await openDirectoryAuthority(
    state,
    mountPath,
    "operation_outcome_uncertain",
  );
  try {
    ensure(
      receipt.device === authorityDevice(authority) &&
        receipt.inode === authorityInode(authority),
      "operation_outcome_uncertain",
    );
    await parentAuthority.handle.sync();
    await assertDirectoryAuthorityCurrent(
      state,
      parentAuthority,
      "operation_outcome_uncertain",
    );
    return authority;
  } catch (error) {
    await closeAuthority(authority);
    throw error;
  }
}

function octalDigit(code) {
  return code >= 0x30 && code <= 0x37;
}

function decodeMountField(value) {
  let result = "";
  let index = 0;
  while (index < value.length) {
    if (
      index + 3 < value.length &&
      stringCharCodeAt(value, index) === 0x5c &&
      octalDigit(stringCharCodeAt(value, index + 1)) &&
      octalDigit(stringCharCodeAt(value, index + 2)) &&
      octalDigit(stringCharCodeAt(value, index + 3))
    ) {
      result += stringFromCharCode(
        ((stringCharCodeAt(value, index + 1) - 0x30) << 6) |
          ((stringCharCodeAt(value, index + 2) - 0x30) << 3) |
          (stringCharCodeAt(value, index + 3) - 0x30),
      );
      index += 4;
    } else {
      result += value[index];
      index += 1;
    }
  }
  return result;
}

function optionSet(value, code) {
  const options = stringSplit(value, ",");
  const result = new SetConstructor();
  for (let index = 0; index < options.length; index += 1) {
    ensure(options[index].length > 0, code);
    setAdd(result, options[index]);
  }
  return result;
}

function parseMountInfo(bytes, mountPath, requirePolicy, allowMissing = false) {
  ensure(
    !isProxyValue(bytes) &&
      callIntrinsic(bufferIsBufferIntrinsic, Buffer, [bytes]) &&
      bufferBytes(bytes) <= MAX_MOUNTINFO_BYTES,
    "observation_failed",
  );
  const text = decodeUtf8(bytes, "observation_failed");
  ensure(
    !stringIncludes(text, "\0") &&
      !stringIncludes(text, "\r") &&
      !stringIncludes(text, "\ufeff"),
    "observation_failed",
  );
  const lines = stringSplit(text, "\n");
  let selected = null;
  let rootSeen = false;
  for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
    if (lines[lineIndex] === "") continue;
    const fields = stringSplit(lines[lineIndex], " ");
    ensure(fields.length >= 10, "observation_failed");
    let separatorIndex = -1;
    for (let index = 6; index < fields.length; index += 1) {
      if (fields[index] === "-") {
        separatorIndex = index;
        break;
      }
    }
    ensure(
      separatorIndex >= 6 && separatorIndex + 3 < fields.length,
      "observation_failed",
    );
    const decodedMountPath = decodeMountField(fields[4]);
    if (decodedMountPath === "/") rootSeen = true;
    if (decodedMountPath !== mountPath) continue;
    ensure(selected === null, "mount_mismatch");
    const mountOptions = optionSet(fields[5], "observation_failed");
    const superOptions = optionSet(fields[separatorIndex + 3], "observation_failed");
    const optionalFields = arraySlice(fields, 6, separatorIndex);
    const privatePropagation = arrayEvery(
      optionalFields,
      (field) =>
        field !== "unbindable" &&
        !stringStartsWith(field, "shared:") &&
        !stringStartsWith(field, "master:") &&
        !stringStartsWith(field, "propagate_from:"),
    );
    if (requirePolicy) {
      for (let index = 0; index < REQUIRED_VFS_OPTIONS.length; index += 1) {
        ensure(setHas(mountOptions, REQUIRED_VFS_OPTIONS[index]), "mount_mismatch");
      }
      ensure(
        setHas(superOptions, "errors=remount-ro") && privatePropagation,
        "mount_mismatch",
      );
    }
    const source = decodeMountField(fields[separatorIndex + 2]);
    ensure(
      regexpTest(DECIMAL_PATTERN, fields[0]) &&
        regexpTest(DECIMAL_PATTERN, fields[1]) &&
        regexpTest(DEVICE_PATTERN, fields[2]) &&
        decodeMountField(fields[3]) === "/" &&
        fields[separatorIndex + 1] === "ext4" &&
        (regexpTest(LOOP_DEVICE_PATTERN, source) ||
          regexpTest(PROC_FD_MOUNT_SOURCE_PATTERN, source)),
      "mount_mismatch",
    );
    selected = frozenRecord({
      device: fields[2],
      mountId: fields[0],
      parentMountId: fields[1],
      propagation: privatePropagation ? "private" : "not-private",
      root: "/",
    });
  }
  ensure(rootSeen, "observation_failed");
  if (selected === null && allowMissing) return null;
  ensure(selected !== null, "mount_absent");
  return selected;
}

async function readMountEvidence(
  state,
  mountPath,
  requirePolicy,
  allowMissing = false,
) {
  let pending;
  try {
    pending = callIntrinsic(state.readMountInfo, undefined, [
      "/proc/self/mountinfo",
    ]);
  } catch {
    fail("observation_failed");
  }
  ensure(isNativePromise(pending), "observation_failed");
  let bytes;
  try {
    bytes = await pending;
  } catch {
    fail("observation_failed");
  }
  return parseMountInfo(bytes, mountPath, requirePolicy, allowMissing);
}

function sameMountEvidence(left, right) {
  return (
    left.device === right.device &&
    left.mountId === right.mountId &&
    left.parentMountId === right.parentMountId &&
    left.propagation === right.propagation &&
    left.root === right.root
  );
}

async function invokeInspector(state, method, path) {
  let pending;
  try {
    pending = callIntrinsic(method, state.inspector.receiver, [path]);
  } catch {
    fail("inspection_failed");
  }
  ensure(isNativePromise(pending), "inspection_failed");
  try {
    return await pending;
  } catch {
    fail("inspection_failed");
  }
}

function normalizeFilesystem(value) {
  const filesystem = exactDataObject(value, FILESYSTEM_KEYS, "inspection_failed");
  ensure(
    filesystem.type === "ext4" &&
      filesystem.durability === "local-fsync-rename" &&
      typeof filesystem.filesystemId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, filesystem.filesystemId) &&
      typeof filesystem.objectIdentityScheme === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, filesystem.objectIdentityScheme),
    "inspection_failed",
  );
  return frozenRecord({
    durability: filesystem.durability,
    filesystemId: filesystem.filesystemId,
    objectIdentityScheme: filesystem.objectIdentityScheme,
    type: filesystem.type,
  });
}

function normalizeInspectedIdentity(value) {
  const identity = exactDataObject(
    value,
    INSPECTED_IDENTITY_KEYS,
    "inspection_failed",
  );
  ensure(
    typeof identity.device === "string" &&
      regexpTest(DECIMAL_PATTERN, identity.device) &&
      typeof identity.inode === "string" &&
      regexpTest(/^[1-9][0-9]*$/u, identity.inode) &&
      typeof identity.objectId === "string" &&
      identity.objectId.length > 0 &&
      identity.objectId.length <= 512 &&
      !regexpTest(CONTROL_PATTERN, identity.objectId),
    "inspection_failed",
  );
  return frozenRecord({
    device: identity.device,
    inode: identity.inode,
    objectId: identity.objectId,
  });
}

function imageIdentity(filesystemId) {
  const hash = createHash("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [
    "portable-codex-runtime/ext4-filesystem-image/v1\0",
    "utf8",
  ]);
  callIntrinsic(hashUpdateIntrinsic, hash, [filesystemId, "utf8"]);
  const digest = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
  return frozenRecord({
    filesystemId,
    objectId: `ext4image1:${digest}`,
    objectIdentityScheme: "ext4-filesystem-image-v1",
  });
}

async function inspectObject(state, path, expectedFilesystemId = null) {
  const before = await validateOwnedPath(
    state,
    path,
    "directory",
    DIRECTORY_MODE,
    "inspection_failed",
  );
  const combined = exactDataObject(
    await invokeInspector(
      state,
      state.inspector.inspectFilesystemObject,
      path,
    ),
    INSPECTED_OBJECT_KEYS,
    "inspection_failed",
  );
  const filesystem = normalizeFilesystem(combined.filesystem);
  const inspected = normalizeInspectedIdentity(combined.identity);
  const after = await validateOwnedPath(
    state,
    path,
    "directory",
    DIRECTORY_MODE,
    "inspection_failed",
  );
  ensure(
    sameRuntimeIdentity(before, after) &&
      inspected.device === bigIntToString(before.dev) &&
      inspected.inode === bigIntToString(before.ino) &&
      (expectedFilesystemId === null ||
        filesystem.filesystemId === expectedFilesystemId),
    "inspection_failed",
  );
  return frozenRecord({
    filesystem,
    identity: frozenRecord({
      filesystemId: filesystem.filesystemId,
      objectId: inspected.objectId,
      objectIdentityScheme: filesystem.objectIdentityScheme,
    }),
    runtimeIdentity: frozenRecord({
      device: inspected.device,
      inode: inspected.inode,
    }),
  });
}

function mountEvidenceRecord(raw, runtimeIdentity) {
  return frozenRecord({
    device: raw.device,
    mountId: raw.mountId,
    parentMountId: raw.parentMountId,
    propagation: raw.propagation,
    root: raw.root,
    rootDevice: runtimeIdentity.device,
    rootInode: runtimeIdentity.inode,
  });
}

function sameMountedObservation(left, right) {
  return (
    left.loopDevice === right.loopDevice &&
    left.imageIdentity.objectId === right.imageIdentity.objectId &&
    left.rootIdentity.objectId === right.rootIdentity.objectId &&
    left.mountEvidence.device === right.mountEvidence.device &&
    left.mountEvidence.mountId === right.mountEvidence.mountId &&
    left.mountEvidence.parentMountId === right.mountEvidence.parentMountId &&
    left.mountEvidence.propagation === right.mountEvidence.propagation &&
    left.mountEvidence.root === right.mountEvidence.root &&
    left.mountEvidence.rootDevice === right.mountEvidence.rootDevice &&
    left.mountEvidence.rootInode === right.mountEvidence.rootInode
  );
}

async function observeMountInternal(state, requestValue, requirePolicy = true) {
  const request = mountRequest(requestValue);
  const before = await readMountEvidence(state, request.mountPath, requirePolicy);
  const beforeLoops = await loopsForImage(state, request.imagePath);
  ensure(beforeLoops.length === 1, "backing_mismatch");
  const loop = beforeLoops[0];
  ensure(
    loop.loopRdev === before.device,
    "backing_mismatch",
  );
  const inspected = await inspectObject(state, request.mountPath);
  const logicalImageIdentity = imageIdentity(inspected.filesystem.filesystemId);
  ensure(
    inspected.identity.filesystemId === logicalImageIdentity.filesystemId,
    "inspection_failed",
  );
  const after = await readMountEvidence(state, request.mountPath, requirePolicy);
  ensure(sameMountEvidence(before, after), "mount_mismatch");
  const stableLoop = await readLoopBacking(
    state,
    loop.loopDevice,
    request.imagePath,
  );
  ensure(
    stableLoop.loopRdev === after.device &&
      stableLoop.loopDevice === loop.loopDevice &&
      stableLoop.backingDevice === loop.backingDevice &&
      stableLoop.backingInode === loop.backingInode &&
      stableLoop.sizeBytes === loop.sizeBytes,
    "backing_mismatch",
  );
  return frozenRecord({
    filesystem: inspected.filesystem,
    imageIdentity: logicalImageIdentity,
    imagePath: request.imagePath,
    loopDevice: loop.loopDevice,
    mountEvidence: mountEvidenceRecord(before, inspected.runtimeIdentity),
    mountPath: request.mountPath,
    rootIdentity: inspected.identity,
  });
}

async function verifyMountedMutation(state, request, loopDevice) {
  try {
    const rawMount = await readMountEvidence(state, request.mountPath, false);
    const loop = await readLoopBacking(state, loopDevice, request.imagePath);
    ensure(loop.loopRdev === rawMount.device, "operation_outcome_uncertain");
  } catch (error) {
    if (weakSetHas(internalErrors, error)) fail("operation_outcome_uncertain");
    throw error;
  }
}

async function provisionInternal(state, requestValue) {
  const request = provisionRequest(requestValue);
  let imageParentAuthority;
  let mountParentAuthority;
  let imageAuthority;
  let mountAuthority;
  try {
    imageParentAuthority = await openDirectoryAuthority(
      state,
      dirname(request.imagePath),
      "image_io_failed",
    );
    mountParentAuthority = await openDirectoryAuthority(
      state,
      dirname(request.mountPath),
      "operation_outcome_uncertain",
    );
    imageAuthority = await createImage(
      state,
      request.imagePath,
      request.imageSizeBytes,
      imageParentAuthority,
    );
    normalizeFdStatus(
      await invokeFdOperation(
        state,
        frozenRecord({
          device: authorityDevice(imageAuthority),
          executable: state.executables.mkfsExt4,
          inode: authorityInode(imageAuthority),
          operation: "format-ext4",
          parentDevice: authorityDevice(imageParentAuthority),
          parentInode: authorityInode(imageParentAuthority),
          path: request.imagePath,
        }),
      ),
    );
    await assertImageAuthorityCurrent(
      state,
      imageAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      imageParentAuthority,
      "operation_outcome_uncertain",
    );
    mountAuthority = await createMountPath(
      state,
      request.mountPath,
      mountParentAuthority,
    );
    const loop = await attachLoop(
      state,
      request.imagePath,
      imageParentAuthority,
      imageAuthority,
    );
    const loopDevice = loop.loopDevice;
    await assertImageAuthorityCurrent(
      state,
      imageAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      mountAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      imageParentAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      mountParentAuthority,
      "operation_outcome_uncertain",
    );
    // Mounting intentionally changes the object visible at mountPath. The
    // provider-private parent policy and this last handle/path identity check
    // protect the covered host directory up to dispatch; post-dispatch checks
    // instead bind the visible ext4 root through mountinfo and the inspector.
    normalizeFdStatus(
      await invokeFdOperation(
        state,
        frozenRecord({
          backingDevice: loop.backingDevice,
          backingInode: loop.backingInode,
          loopDevice,
          loopRdev: loop.loopRdev,
          operation: "mount-ext4",
          parentDevice: authorityDevice(mountParentAuthority),
          parentInode: authorityInode(mountParentAuthority),
          path: request.mountPath,
          sizeBytes: loop.sizeBytes,
          targetDevice: authorityDevice(mountAuthority),
          targetInode: authorityInode(mountAuthority),
        }),
      ),
    );
    await verifyMountedMutation(state, request, loopDevice);
    let observation;
    try {
      observation = await observeMountInternal(
        state,
        frozenRecord({
          imagePath: request.imagePath,
          mountPath: request.mountPath,
        }),
      );
    } catch {
      fail("operation_outcome_uncertain");
    }
    ensure(observation.loopDevice === loopDevice, "operation_outcome_uncertain");
    await assertImageAuthorityCurrent(
      state,
      imageAuthority,
      "operation_outcome_uncertain",
    );
    await ensurePublicationRootInternal(
      state,
      frozenRecord({
        imagePath: request.imagePath,
        mountPath: request.mountPath,
      }),
    );
    await assertDirectoryAuthorityCurrent(
      state,
      imageParentAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      mountParentAuthority,
      "operation_outcome_uncertain",
    );
    return observation;
  } finally {
    await closeAuthority(mountAuthority);
    await closeAuthority(imageAuthority);
    await closeAuthority(mountParentAuthority);
    await closeAuthority(imageParentAuthority);
  }
}

async function remountInternal(state, requestValue) {
  const remount = publicationRequest(requestValue);
  const request = frozenRecord({
    imagePath: remount.imagePath,
    mountPath: remount.mountPath,
  });
  const initialMount = await readMountEvidence(
    state,
    request.mountPath,
    false,
    true,
  );
  ensure(initialMount === null, "mount_mismatch");
  let imageParentAuthority;
  let mountParentAuthority;
  let imageAuthority;
  let mountAuthority;
  try {
    imageParentAuthority = await openDirectoryAuthority(
      state,
      dirname(request.imagePath),
      "observation_failed",
    );
    mountParentAuthority = await openDirectoryAuthority(
      state,
      dirname(request.mountPath),
      "observation_failed",
    );
    imageAuthority = await openImageAuthority(
      state,
      request.imagePath,
      "observation_failed",
    );
    mountAuthority = await openDirectoryAuthority(
      state,
      request.mountPath,
      "observation_failed",
    );
    ensure(
      (await readMountEvidence(
        state,
        request.mountPath,
        false,
        true,
      )) === null,
      "mount_mismatch",
    );
    const associated = await loopsForImage(
      state,
      request.imagePath,
      imageParentAuthority,
      imageAuthority,
    );
    let loop;
    if (associated.length === 0) {
      loop = await attachLoop(
        state,
        request.imagePath,
        imageParentAuthority,
        imageAuthority,
      );
    } else {
      loop = associated[0];
      await readLoopBacking(
        state,
        loop.loopDevice,
        request.imagePath,
        imageParentAuthority,
        imageAuthority,
      );
    }
    const loopDevice = loop.loopDevice;
    await assertImageAuthorityCurrent(
      state,
      imageAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      mountAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      imageParentAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      mountParentAuthority,
      "operation_outcome_uncertain",
    );
    ensure(
      (await readMountEvidence(
        state,
        request.mountPath,
        false,
        true,
      )) === null,
      "operation_outcome_uncertain",
    );
    normalizeFdStatus(
      await invokeFdOperation(
        state,
        frozenRecord({
          backingDevice: loop.backingDevice,
          backingInode: loop.backingInode,
          loopDevice,
          loopRdev: loop.loopRdev,
          operation: "mount-ext4",
          parentDevice: authorityDevice(mountParentAuthority),
          parentInode: authorityInode(mountParentAuthority),
          path: request.mountPath,
          sizeBytes: loop.sizeBytes,
          targetDevice: authorityDevice(mountAuthority),
          targetInode: authorityInode(mountAuthority),
        }),
      ),
    );
    await verifyMountedMutation(state, request, loopDevice);
    let observation;
    try {
      observation = await observeMountInternal(state, request, true);
    } catch {
      fail("operation_outcome_uncertain");
    }
    ensure(
      observation.loopDevice === loopDevice,
      "operation_outcome_uncertain",
    );
    await assertImageAuthorityCurrent(
      state,
      imageAuthority,
      "operation_outcome_uncertain",
    );
    await ensurePublicationRootInternal(state, remount);
    await assertDirectoryAuthorityCurrent(
      state,
      imageParentAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      mountParentAuthority,
      "operation_outcome_uncertain",
    );
    return observation;
  } finally {
    await closeAuthority(mountAuthority);
    await closeAuthority(imageAuthority);
    await closeAuthority(mountParentAuthority);
    await closeAuthority(imageParentAuthority);
  }
}

async function ensureAttachmentRootInternal(state, requestValue) {
  const request = attachmentRequest(requestValue);
  const mount = frozenRecord({
    imagePath: request.imagePath,
    mountPath: request.mountPath,
  });
  const before = await observeMountInternal(state, mount, true);
  const directoryReceipt = normalizeDirectoryReceipt(
    await invokeFdOperation(
      state,
      frozenRecord({
        exclusive: false,
        operation: "create-directory",
        parentDevice: before.mountEvidence.rootDevice,
        parentInode: before.mountEvidence.rootInode,
        path: request.attachmentRootPath,
      }),
    ),
  );
  const created = directoryReceipt.created;
  let metadata;
  try {
    metadata = await lstat(request.attachmentRootPath, { bigint: true });
  } catch {
    fail(created ? "operation_outcome_uncertain" : "attachment_root_unsafe");
  }
  ensure(
    isDirectoryStat(metadata) && statMode(metadata) === DIRECTORY_MODE,
    created ? "operation_outcome_uncertain" : "attachment_root_unsafe",
  );
  const inspected = await inspectObject(
    state,
    request.attachmentRootPath,
    before.imageIdentity.filesystemId,
  );
  ensure(
    directoryReceipt.device === inspected.runtimeIdentity.device &&
      directoryReceipt.inode === inspected.runtimeIdentity.inode,
    created ? "operation_outcome_uncertain" : "attachment_root_unsafe",
  );
  ensure(
    inspected.identity.objectId !== before.rootIdentity.objectId,
    "attachment_root_unsafe",
  );
  const after = await observeMountInternal(state, mount, true);
  ensure(sameMountedObservation(after, before), "operation_outcome_uncertain");
  return frozenRecord({
    attachmentRootPath: request.attachmentRootPath,
    created,
    filesystem: after.filesystem,
    imageIdentity: after.imageIdentity,
    imagePath: request.imagePath,
    loopDevice: after.loopDevice,
    mountEvidence: after.mountEvidence,
    mountPath: request.mountPath,
    mountRootIdentity: after.rootIdentity,
    rootIdentity: inspected.identity,
  });
}

async function observeAttachmentRootInternal(state, requestValue) {
  const request = attachmentRequest(requestValue);
  const mount = frozenRecord({
    imagePath: request.imagePath,
    mountPath: request.mountPath,
  });
  const before = await observeMountInternal(state, mount, true);
  try {
    await lstat(request.attachmentRootPath, { bigint: true });
  } catch (error) {
    if (safeErrorCode(error) === "ENOENT") {
      let after;
      try {
        after = await observeMountInternal(state, mount, true);
      } catch {
        fail("observation_failed");
      }
      ensure(sameMountedObservation(after, before), "observation_failed");
      fail("attachment_root_absent");
    }
    fail("inspection_failed");
  }
  const inspected = await inspectObject(
    state,
    request.attachmentRootPath,
    before.imageIdentity.filesystemId,
  );
  ensure(
    inspected.identity.objectId !== before.rootIdentity.objectId,
    "attachment_root_unsafe",
  );
  const after = await observeMountInternal(state, mount, true);
  ensure(sameMountedObservation(after, before), "observation_failed");
  return frozenRecord({
    attachmentRootPath: request.attachmentRootPath,
    filesystem: after.filesystem,
    imageIdentity: after.imageIdentity,
    imagePath: request.imagePath,
    loopDevice: after.loopDevice,
    mountEvidence: after.mountEvidence,
    mountPath: request.mountPath,
    mountRootIdentity: after.rootIdentity,
    rootIdentity: inspected.identity,
  });
}

async function ensurePublicationRootInternal(state, requestValue) {
  const request = publicationRequest(requestValue);
  const mount = frozenRecord({
    imagePath: request.imagePath,
    mountPath: request.mountPath,
  });
  const before = await observeMountInternal(state, mount, true);
  const expected = request.expectedPublicationControlIdentity;
  const control = normalizeControlReceipt(
    await invokeFdOperation(
      state,
      frozenRecord({
        device: before.mountEvidence.rootDevice,
        expectedControlFilesystemId: expected?.filesystemId ?? null,
        expectedControlObjectId: expected?.objectId ?? null,
        filesystemId: before.rootIdentity.filesystemId,
        inode: before.mountEvidence.rootInode,
        kind: "publication",
        objectId: before.rootIdentity.objectId,
        operation: "provision-control-root",
        rootPath: request.mountPath,
      }),
    ),
  );
  ensure(
    control.controlFileIdentity.device === before.mountEvidence.rootDevice &&
      control.controlFileIdentity.filesystemId ===
        before.imageIdentity.filesystemId,
    "operation_outcome_uncertain",
  );
  ensure(
    expected === null ||
      (control.controlFileIdentity.filesystemId === expected.filesystemId &&
        control.controlFileIdentity.objectId === expected.objectId &&
        control.controlFileIdentity.objectIdentityScheme ===
          expected.objectIdentityScheme),
    "operation_outcome_uncertain",
  );
  const after = await observeMountInternal(state, mount, true);
  ensure(sameMountedObservation(after, before), "operation_outcome_uncertain");
  return frozenRecord({
    controlFileIdentity: control.controlFileIdentity,
    controlFileName: control.controlFileName,
    created: control.created,
    filesystem: after.filesystem,
    imageIdentity: after.imageIdentity,
    imagePath: request.imagePath,
    loopDevice: after.loopDevice,
    mountEvidence: after.mountEvidence,
    mountPath: request.mountPath,
    mountRootIdentity: after.rootIdentity,
    publicationControlIdentity: frozenRecord({
      filesystemId: control.controlFileIdentity.filesystemId,
      objectIdentityScheme: control.controlFileIdentity.objectIdentityScheme,
      objectId: control.controlFileIdentity.objectId,
    }),
  });
}

async function syncFilesystemInternal(state, requestValue) {
  const request = mountRequest(requestValue);
  const before = await observeMountInternal(state, request, true);
  normalizeFdStatus(
    await invokeFdOperation(
      state,
      frozenRecord({
        device: before.mountEvidence.rootDevice,
        filesystemId: before.rootIdentity.filesystemId,
        inode: before.mountEvidence.rootInode,
        objectId: before.rootIdentity.objectId,
        operation: "syncfs",
        path: request.mountPath,
      }),
    ),
  );
  const after = await observeMountInternal(state, request, true);
  ensure(sameMountedObservation(after, before), "operation_outcome_uncertain");
  return frozenRecord({ mount: after, status: "synced" });
}

async function quiescePhysical(state, request) {
  const initialMount = await readMountEvidence(
    state,
    request.mountPath,
    false,
    true,
  );
  const observation =
    initialMount === null
      ? null
      : await observeMountInternal(state, request, true);
  let imageParentAuthority;
  let mountParentAuthority;
  let imageAuthority;
  let mountAuthority;
  try {
    imageParentAuthority = await openDirectoryAuthority(
      state,
      dirname(request.imagePath),
      "operation_outcome_uncertain",
    );
    mountParentAuthority = await openDirectoryAuthority(
      state,
      dirname(request.mountPath),
      "operation_outcome_uncertain",
    );
    imageAuthority = await openImageAuthority(
      state,
      request.imagePath,
      "operation_outcome_uncertain",
    );
    if (observation === null) {
      mountAuthority = await openDirectoryAuthority(
        state,
        request.mountPath,
        "operation_outcome_uncertain",
      );
      ensure(
        (await readMountEvidence(
          state,
          request.mountPath,
          false,
          true,
        )) === null,
        "operation_outcome_uncertain",
      );
      let associated;
      try {
        associated = await loopsForImage(
          state,
          request.imagePath,
          imageParentAuthority,
          imageAuthority,
        );
      } catch {
        fail("operation_outcome_uncertain");
      }
      if (associated.length === 1) {
        const loop = associated[0];
        await readLoopBacking(
          state,
          loop.loopDevice,
          request.imagePath,
          imageParentAuthority,
          imageAuthority,
        );
        await assertImageAuthorityCurrent(
          state,
          imageAuthority,
          "operation_outcome_uncertain",
        );
        await assertDirectoryAuthorityCurrent(
          state,
          mountAuthority,
          "operation_outcome_uncertain",
        );
        normalizeFdStatus(
          await invokeFdOperation(
            state,
            frozenRecord({
              device: authorityDevice(imageAuthority),
              inode: authorityInode(imageAuthority),
              loopDevice: loop.loopDevice,
              operation: "detach-loop-settle",
              parentDevice: authorityDevice(imageParentAuthority),
              parentInode: authorityInode(imageParentAuthority),
              path: request.imagePath,
            }),
          ),
        );
        try {
          associated = await loopsForImage(
            state,
            request.imagePath,
            imageParentAuthority,
            imageAuthority,
          );
        } catch {
          fail("operation_outcome_uncertain");
        }
      }
      ensure(associated.length === 0, "operation_outcome_uncertain");
      await assertImageAuthorityCurrent(
        state,
        imageAuthority,
        "operation_outcome_uncertain",
      );
      await assertDirectoryAuthorityCurrent(
        state,
        mountAuthority,
        "operation_outcome_uncertain",
      );
      await assertDirectoryAuthorityCurrent(
        state,
        imageParentAuthority,
        "operation_outcome_uncertain",
      );
      await assertDirectoryAuthorityCurrent(
        state,
        mountParentAuthority,
        "operation_outcome_uncertain",
      );
      return {
        imageAuthority,
        imageParentAuthority,
        mountAuthority,
        mountParentAuthority,
      };
    }
    await assertImageAuthorityCurrent(
      state,
      imageAuthority,
      "operation_outcome_uncertain",
    );
    normalizeFdStatus(
      await invokeFdOperation(
        state,
        frozenRecord({
          device: observation.mountEvidence.rootDevice,
          filesystemId: observation.rootIdentity.filesystemId,
          inode: observation.mountEvidence.rootInode,
          objectId: observation.rootIdentity.objectId,
          operation: "syncfs",
          path: request.mountPath,
        }),
      ),
    );
    const beforeUnmount = await observeMountInternal(state, request, true);
    ensure(
      sameMountedObservation(beforeUnmount, observation),
      "operation_outcome_uncertain",
    );
    await assertImageAuthorityCurrent(
      state,
      imageAuthority,
      "operation_outcome_uncertain",
    );
    normalizeFdStatus(
      await invokeFdOperation(
        state,
        frozenRecord({
          operation: "unmount-ext4",
          parentDevice: authorityDevice(mountParentAuthority),
          parentInode: authorityInode(mountParentAuthority),
          path: request.mountPath,
          targetDevice: beforeUnmount.mountEvidence.rootDevice,
          targetFilesystemId: beforeUnmount.rootIdentity.filesystemId,
          targetInode: beforeUnmount.mountEvidence.rootInode,
          targetObjectId: beforeUnmount.rootIdentity.objectId,
        }),
      ),
    );
    ensure(
      (await readMountEvidence(
        state,
        request.mountPath,
        false,
        true,
      )) === null,
      "operation_outcome_uncertain",
    );
    mountAuthority = await openDirectoryAuthority(
      state,
      request.mountPath,
      "operation_outcome_uncertain",
    );
    await assertImageAuthorityCurrent(
      state,
      imageAuthority,
      "operation_outcome_uncertain",
    );
    normalizeFdStatus(
      await invokeFdOperation(
        state,
        frozenRecord({
          device: authorityDevice(imageAuthority),
          inode: authorityInode(imageAuthority),
          loopDevice: observation.loopDevice,
          operation: "detach-loop-settle",
          parentDevice: authorityDevice(imageParentAuthority),
          parentInode: authorityInode(imageParentAuthority),
          path: request.imagePath,
        }),
      ),
    );
    let remaining;
    try {
      remaining = await loopsForImage(
        state,
        request.imagePath,
        imageParentAuthority,
        imageAuthority,
      );
    } catch {
      fail("operation_outcome_uncertain");
    }
    ensure(remaining.length === 0, "operation_outcome_uncertain");
    await assertImageAuthorityCurrent(
      state,
      imageAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      mountAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      imageParentAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      mountParentAuthority,
      "operation_outcome_uncertain",
    );
    return {
      imageAuthority,
      imageParentAuthority,
      mountAuthority,
      mountParentAuthority,
    };
  } catch (error) {
    await closeAuthority(mountAuthority);
    await closeAuthority(imageAuthority);
    await closeAuthority(mountParentAuthority);
    await closeAuthority(imageParentAuthority);
    throw error;
  }
}

async function quiesceInternal(state, requestValue) {
  const request = mountRequest(requestValue);
  const authorities = await quiescePhysical(state, request);
  try {
    return frozenRecord({
      imagePath: request.imagePath,
      mountPath: request.mountPath,
      status: "quiesced",
    });
  } finally {
    await closeAuthority(authorities.mountAuthority);
    await closeAuthority(authorities.imageAuthority);
    await closeAuthority(authorities.mountParentAuthority);
    await closeAuthority(authorities.imageParentAuthority);
  }
}

async function destroyInternal(state, requestValue) {
  const request = mountRequest(requestValue);
  const authorities = await quiescePhysical(state, request);
  try {
    await assertImageAuthorityCurrent(
      state,
      authorities.imageAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      authorities.imageParentAuthority,
      "operation_outcome_uncertain",
    );
    normalizeFdStatus(
      await invokeFdOperation(
        state,
        frozenRecord({
          operation: "remove-file",
          parentDevice: authorityDevice(authorities.imageParentAuthority),
          parentInode: authorityInode(authorities.imageParentAuthority),
          path: request.imagePath,
          targetDevice: authorityDevice(authorities.imageAuthority),
          targetInode: authorityInode(authorities.imageAuthority),
        }),
      ),
    );
    try {
      await authorities.imageParentAuthority.handle.sync();
    } catch {
      fail("operation_outcome_uncertain");
    }
    await assertDirectoryAuthorityCurrent(
      state,
      authorities.imageParentAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      authorities.mountAuthority,
      "operation_outcome_uncertain",
    );
    await assertDirectoryAuthorityCurrent(
      state,
      authorities.mountParentAuthority,
      "operation_outcome_uncertain",
    );
    normalizeFdStatus(
      await invokeFdOperation(
        state,
        frozenRecord({
          operation: "remove-directory",
          parentDevice: authorityDevice(authorities.mountParentAuthority),
          parentInode: authorityInode(authorities.mountParentAuthority),
          path: request.mountPath,
          targetDevice: authorityDevice(authorities.mountAuthority),
          targetInode: authorityInode(authorities.mountAuthority),
        }),
      ),
    );
    try {
      await authorities.mountParentAuthority.handle.sync();
    } catch {
      fail("operation_outcome_uncertain");
    }
    await assertDirectoryAuthorityCurrent(
      state,
      authorities.mountParentAuthority,
      "operation_outcome_uncertain",
    );
  } finally {
    await closeAuthority(authorities.mountAuthority);
    await closeAuthority(authorities.imageAuthority);
    await closeAuthority(authorities.mountParentAuthority);
    await closeAuthority(authorities.imageParentAuthority);
  }
  return frozenRecord({
    imagePath: request.imagePath,
    mountPath: request.mountPath,
    status: "destroyed",
  });
}

async function withImageOperationLock(imagePath, operation) {
  const previous =
    mapGet(imageOperationTails, imagePath) ??
    callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, [undefined]);
  let release;
  const gate = new PromiseConstructor((resolveGate) => {
    release = resolveGate;
  });
  mapSet(imageOperationTails, imagePath, gate);
  await previous;
  try {
    return await operation();
  } finally {
    release();
    if (mapGet(imageOperationTails, imagePath) === gate) {
      mapDelete(imageOperationTails, imagePath);
    }
  }
}

async function serializeRequest(input, normalize, operation) {
  const request = normalize(input);
  return await withImageOperationLock(request.imagePath, () => operation(request));
}

export function createLinuxExt4ImageDriver(options) {
  if (arguments.length !== 1) {
    throw new TypeError("expected one options argument");
  }
  const state = normalizeOptions(options);
  let surface;
  const provision = objectFreezeIntrinsic(function provision(input) {
    if (this !== surface) throw new TypeError("invalid Linux ext4 image driver receiver");
    if (arguments.length !== 1) throw new TypeError("expected one request argument");
    return serializeRequest(input, provisionRequest, (request) =>
      provisionInternal(state, request),
    );
  });
  const observeMount = objectFreezeIntrinsic(function observeMount(input) {
    if (this !== surface) throw new TypeError("invalid Linux ext4 image driver receiver");
    if (arguments.length !== 1) throw new TypeError("expected one request argument");
    return serializeRequest(input, mountRequest, (request) =>
      observeMountInternal(state, request, true),
    );
  });
  const observeAttachmentRoot = objectFreezeIntrinsic(
    function observeAttachmentRoot(input) {
      if (this !== surface) {
        throw new TypeError("invalid Linux ext4 image driver receiver");
      }
      if (arguments.length !== 1) {
        throw new TypeError("expected one request argument");
      }
      return serializeRequest(input, attachmentRequest, (request) =>
        observeAttachmentRootInternal(state, request),
      );
    },
  );
  const remount = objectFreezeIntrinsic(function remount(input) {
    if (this !== surface) throw new TypeError("invalid Linux ext4 image driver receiver");
    if (arguments.length !== 1) throw new TypeError("expected one request argument");
    return serializeRequest(input, publicationRequest, (request) =>
      remountInternal(state, request),
    );
  });
  const ensureAttachmentRoot = objectFreezeIntrinsic(
    function ensureAttachmentRoot(input) {
      if (this !== surface) {
        throw new TypeError("invalid Linux ext4 image driver receiver");
      }
      if (arguments.length !== 1) {
        throw new TypeError("expected one request argument");
      }
      return serializeRequest(input, attachmentRequest, (request) =>
        ensureAttachmentRootInternal(state, request),
      );
    },
  );
  const ensurePublicationRoot = objectFreezeIntrinsic(
    function ensurePublicationRoot(input) {
      if (this !== surface) {
        throw new TypeError("invalid Linux ext4 image driver receiver");
      }
      if (arguments.length !== 1) {
        throw new TypeError("expected one request argument");
      }
      return serializeRequest(input, publicationRequest, (request) =>
        ensurePublicationRootInternal(state, request),
      );
    },
  );
  const syncFilesystem = objectFreezeIntrinsic(function syncFilesystem(input) {
    if (this !== surface) throw new TypeError("invalid Linux ext4 image driver receiver");
    if (arguments.length !== 1) throw new TypeError("expected one request argument");
    return serializeRequest(input, mountRequest, (request) =>
      syncFilesystemInternal(state, request),
    );
  });
  const quiesce = objectFreezeIntrinsic(function quiesce(input) {
    if (this !== surface) throw new TypeError("invalid Linux ext4 image driver receiver");
    if (arguments.length !== 1) throw new TypeError("expected one request argument");
    return serializeRequest(input, mountRequest, (request) =>
      quiesceInternal(state, request),
    );
  });
  const destroy = objectFreezeIntrinsic(function destroy(input) {
    if (this !== surface) throw new TypeError("invalid Linux ext4 image driver receiver");
    if (arguments.length !== 1) throw new TypeError("expected one request argument");
    return serializeRequest(input, mountRequest, (request) =>
      destroyInternal(state, request),
    );
  });
  surface = frozenRecord({
    contractVersion: LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
    provision,
    observeMount,
    observeAttachmentRoot,
    remount,
    ensureAttachmentRoot,
    ensurePublicationRoot,
    syncFilesystem,
    quiesce,
    destroy,
  });
  return surface;
}

objectFreezeIntrinsic(LinuxExt4ImageDriverError.prototype);
objectFreezeIntrinsic(LinuxExt4ImageDriverError);
objectFreezeIntrinsic(createLinuxExt4ImageDriver);
