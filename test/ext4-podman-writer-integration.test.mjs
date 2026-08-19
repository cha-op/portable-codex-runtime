import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import {
  retireExt4PodmanRootlessNamespaceForConformance,
  waitForExt4PodmanReadyMarker,
} from "../integration/ext4-podman-writer.mjs";

function exact(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function retirementConfiguration(values = {}) {
  return exact({
    exclusiveRootlessEngine: true,
    podmanEnvironment: exact({
      HOME: "/home/conformance",
      LANG: "C.UTF-8",
      XDG_RUNTIME_DIR: "/run/user/1000",
    }),
    podmanExecutable: "/usr/bin/podman",
    ...values,
  });
}

function commandResult(stdout, stderr = "") {
  return exact({ stderr, stdout });
}

test("ext4 workflow forwards the exclusive Podman engine proof into Node", async () => {
  const workflow = await readFile(new URL(
    "../.github/workflows/test.yml",
    import.meta.url,
  ), "utf8");
  assert.equal(
    workflow.includes('LINUX_EXT4_PODMAN_ENGINE_EXCLUSIVE: "1"'),
    true,
  );
  assert.equal(
    workflow.includes(
      'LINUX_EXT4_PODMAN_ENGINE_EXCLUSIVE="$LINUX_EXT4_PODMAN_ENGINE_EXCLUSIVE"',
    ),
    true,
  );
});

test("ext4 producer retires Podman after marker proof and before physical detach", async () => {
  const source = await readFile(new URL(
    "../integration/linux-ext4-physical-backend.mjs",
    import.meta.url,
  ), "utf8");
  const produceStart = source.indexOf("async function produce()");
  const produceEnd = source.indexOf("async function consume(", produceStart);
  assert.notEqual(produceStart, -1);
  assert.notEqual(produceEnd, -1);
  const produce = source.slice(produceStart, produceEnd);
  const launch = produce.indexOf("await runExt4PodmanWriterIntegration(");
  const markerProof = produce.indexOf(
    "assert.deepEqual(await readFile(podmanWriter.markerPath), PODMAN_MARKER)",
  );
  const retirement = produce.indexOf(
    "await retireExt4PodmanRootlessNamespaceForConformance(",
  );
  const detach = produce.indexOf(
    "await fixed.backend.lifecycleBackend.detachAttachment(",
  );
  const quiesce = produce.indexOf("await fixed.backend.quiesceStorage(");
  assert.equal(
    [launch, markerProof, retirement, detach, quiesce].every(
      (index) => index >= 0,
    ),
    true,
  );
  assert.equal(launch < markerProof, true);
  assert.equal(markerProof < retirement, true);
  assert.equal(retirement < detach, true);
  assert.equal(detach < quiesce, true);
});

test("ext4 Podman conformance retirement proves empty inventories before migrate", async () => {
  const calls = [];
  const results = [commandResult("[]\n"), commandResult("[]"), commandResult("")];
  await retireExt4PodmanRootlessNamespaceForConformance(
    retirementConfiguration(),
    async (executable, arguments_, options) => {
      calls.push({ arguments_, executable, options });
      return results[calls.length - 1];
    },
  );

  assert.equal(calls.length, 3);
  assert.deepEqual(calls.map(({ arguments_ }) => arguments_), [
    [
      "--remote=false",
      "ps",
      "--all",
      "--external",
      "--no-trunc",
      "--format=json",
    ],
    [
      "--remote=false",
      "pod",
      "ps",
      "--no-trunc",
      "--format=json",
    ],
    ["--remote=false", "system", "migrate"],
  ]);
  assert.deepEqual(calls.map(({ executable }) => executable), [
    "/usr/bin/podman",
    "/usr/bin/podman",
    "/usr/bin/podman",
  ]);
  assert.equal(calls[0].options, calls[1].options);
  assert.equal(calls[1].options, calls[2].options);
  assert.deepEqual(calls[0].options, exact({
    cwd: "/",
    encoding: "utf8",
    env: exact({
      HOME: "/home/conformance",
      LANG: "C.UTF-8",
      PATH: "/usr/bin:/bin",
      XDG_RUNTIME_DIR: "/run/user/1000",
    }),
    killSignal: "SIGKILL",
    maxBuffer: 1024 * 1024,
    shell: false,
    timeout: 30_000,
  }));
  assert.equal(Object.isFrozen(calls[0].arguments_), true);
  assert.equal(Object.isFrozen(calls[0].options), true);
  assert.equal(Object.isFrozen(calls[0].options.env), true);
});

test("ext4 Podman conformance retirement captures path resolution and avoids array iterators", {
  concurrency: false,
}, async () => {
  const iteratorDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    Symbol.iterator,
  );
  const resolveDescriptor = Object.getOwnPropertyDescriptor(path, "resolve");
  const jsonParseDescriptor = Object.getOwnPropertyDescriptor(JSON, "parse");
  const assertEqualDescriptor = Object.getOwnPropertyDescriptor(
    assert,
    "equal",
  );
  const assertNotEqualDescriptor = Object.getOwnPropertyDescriptor(
    assert,
    "notEqual",
  );
  assert.equal(typeof iteratorDescriptor?.value, "function");
  assert.equal(typeof resolveDescriptor?.value, "function");
  assert.equal(typeof jsonParseDescriptor?.value, "function");
  assert.equal(typeof assertEqualDescriptor?.value, "function");
  assert.equal(typeof assertNotEqualDescriptor?.value, "function");
  const originalIterator = iteratorDescriptor.value;
  const reflectApply = Reflect.apply;
  const configuration = retirementConfiguration();
  let callCount = 0;
  let iteratorPoisonCalls = 0;
  let jsonPoisonCalls = 0;
  let pathPoisonCalls = 0;
  let assertPoisonCalls = 0;
  let observedError = null;
  try {
    Object.defineProperty(path, "resolve", {
      ...resolveDescriptor,
      value() {
        pathPoisonCalls += 1;
        throw new Error("poisoned path resolve");
      },
    });
    syncBuiltinESMExports();
    Object.defineProperty(JSON, "parse", {
      ...jsonParseDescriptor,
      value() {
        jsonPoisonCalls += 1;
        return [];
      },
    });
    Object.defineProperty(assert, "equal", {
      ...assertEqualDescriptor,
      value() {
        assertPoisonCalls += 1;
      },
    });
    Object.defineProperty(assert, "notEqual", {
      ...assertNotEqualDescriptor,
      value() {
        assertPoisonCalls += 1;
      },
    });
    Object.defineProperty(Array.prototype, Symbol.iterator, {
      ...iteratorDescriptor,
      value: function guardedIterator() {
        let targetsRetirementDomain = false;
        for (let index = 0; index < this.length; index += 1) {
          const descriptor = Object.getOwnPropertyDescriptor(
            this,
            String(index),
          );
          const item = descriptor?.value;
          if (
            item === "exclusiveRootlessEngine" ||
            item === "stderr" ||
            item === "--remote=false"
          ) {
            targetsRetirementDomain = true;
          }
        }
        if (targetsRetirementDomain) {
          iteratorPoisonCalls += 1;
          throw new Error("poisoned retirement-domain array iterator");
        }
        return reflectApply(originalIterator, this, []);
      },
    });
    try {
      await retireExt4PodmanRootlessNamespaceForConformance(
        configuration,
        async () => {
          callCount += 1;
          return callCount < 3 ? commandResult("[]") : commandResult("");
        },
      );
    } catch (error) {
      observedError = error;
    }
  } finally {
    Object.defineProperty(
      Array.prototype,
      Symbol.iterator,
      iteratorDescriptor,
    );
    Object.defineProperty(path, "resolve", resolveDescriptor);
    Object.defineProperty(JSON, "parse", jsonParseDescriptor);
    Object.defineProperty(assert, "equal", assertEqualDescriptor);
    Object.defineProperty(assert, "notEqual", assertNotEqualDescriptor);
    syncBuiltinESMExports();
  }

  assert.equal(observedError, null);
  assert.equal(callCount, 3);
  assert.equal(assertPoisonCalls, 0);
  assert.equal(iteratorPoisonCalls, 0);
  assert.equal(jsonPoisonCalls, 0);
  assert.equal(pathPoisonCalls, 0);
});

test("ext4 Podman conformance retirement blocks migrate on uncertain inventory", async (t) => {
  const cases = [
    {
      name: "container inventory is nonempty",
      results: [commandResult('[{"Id":"container"}]')],
    },
    {
      name: "container inventory is malformed",
      results: [commandResult("{}")],
    },
    {
      name: "container inventory writes stderr",
      results: [commandResult("[]", "warning\n")],
    },
    {
      name: "pod inventory is nonempty",
      results: [commandResult("[]"), commandResult('[{"Id":"pod"}]')],
    },
    {
      name: "pod inventory is malformed",
      results: [commandResult("[]"), commandResult("not-json")],
    },
    {
      name: "pod inventory writes stderr",
      results: [commandResult("[]"), commandResult("[]", "warning\n")],
    },
    {
      name: "runner result has an extra field",
      results: [exact({ code: 0, stderr: "", stdout: "[]" })],
    },
  ];

  for (const fixture of cases) {
    await t.test(fixture.name, async () => {
      const argumentsSeen = [];
      await assert.rejects(
        retireExt4PodmanRootlessNamespaceForConformance(
          retirementConfiguration(),
          async (_executable, arguments_) => {
            argumentsSeen.push(arguments_);
            return fixture.results[argumentsSeen.length - 1];
          },
        ),
      );
      assert.equal(argumentsSeen.length, fixture.results.length);
      assert.equal(
        argumentsSeen.some((arguments_) => arguments_.includes("migrate")),
        false,
      );
    });
  }
});

test("ext4 Podman conformance retirement propagates runner failures", async (t) => {
  for (const failedCall of [1, 2, 3]) {
    await t.test(`call ${failedCall}`, async () => {
      const failure = new Error(`runner failure ${failedCall}`);
      let callCount = 0;
      await assert.rejects(
        retireExt4PodmanRootlessNamespaceForConformance(
          retirementConfiguration(),
          async () => {
            callCount += 1;
            if (callCount === failedCall) throw failure;
            return callCount < 3 ? commandResult("[]") : commandResult("");
          },
        ),
        (error) => error === failure,
      );
      assert.equal(callCount, failedCall);
    });
  }
});

test("ext4 Podman conformance retirement requires silent migrate completion", async (t) => {
  for (const fixture of [
    { name: "stdout", result: commandResult("unexpected\n") },
    { name: "stderr", result: commandResult("", "warning\n") },
    { name: "invalid result", result: null },
  ]) {
    await t.test(fixture.name, async () => {
      let callCount = 0;
      await assert.rejects(
        retireExt4PodmanRootlessNamespaceForConformance(
          retirementConfiguration(),
          async () => {
            callCount += 1;
            return callCount < 3 ? commandResult("[]") : fixture.result;
          },
        ),
      );
      assert.equal(callCount, 3);
    });
  }
});

test("ext4 Podman conformance retirement rejects untrusted configuration", async (t) => {
  const environmentWithPath = exact({
    HOME: "/home/conformance",
    LANG: "C.UTF-8",
    PATH: "/tmp/untrusted",
    XDG_RUNTIME_DIR: "/run/user/1000",
  });
  let getterCalled = false;
  const accessorEnvironment = Object.freeze({
    get HOME() {
      getterCalled = true;
      return "/home/conformance";
    },
    LANG: "C.UTF-8",
    XDG_RUNTIME_DIR: "/run/user/1000",
  });
  const configurations = [
    {
      name: "missing exclusive-engine opt-in",
      value: retirementConfiguration({ exclusiveRootlessEngine: false }),
    },
    {
      name: "PATH-resolved executable",
      value: retirementConfiguration({ podmanExecutable: "podman" }),
    },
    {
      name: "noncanonical executable",
      value: retirementConfiguration({
        podmanExecutable: "/usr/bin/../bin/podman",
      }),
    },
    {
      name: "non-lossless UTF-8 executable",
      value: retirementConfiguration({
        podmanExecutable: "/usr/bin/podman\ud800",
      }),
    },
    {
      name: "executable exceeds the UTF-8 byte bound",
      value: retirementConfiguration({
        podmanExecutable: `/${"é".repeat(2_048)}`,
      }),
    },
    {
      name: "caller-controlled PATH",
      value: retirementConfiguration({ podmanEnvironment: environmentWithPath }),
    },
    {
      name: "environment accessor",
      value: retirementConfiguration({ podmanEnvironment: accessorEnvironment }),
    },
    {
      name: "unexpected configuration field",
      value: exact({ ...retirementConfiguration(), unexpected: true }),
    },
  ];

  for (const configuration of configurations) {
    await t.test(configuration.name, async () => {
      let called = false;
      await assert.rejects(
        retireExt4PodmanRootlessNamespaceForConformance(
          configuration.value,
          async () => {
            called = true;
            return commandResult("");
          },
        ),
      );
      assert.equal(called, false);
    });
  }
  assert.equal(getterCalled, false);
});

test("ext4 Podman marker polling tolerates a published partial write", async () => {
  let firstReadObserved;
  const firstRead = new Promise((resolve) => {
    firstReadObserved = resolve;
  });
  let readCount = 0;
  async function readMarker(path, encoding) {
    assert.equal(path, "/session/podman-writer-ready");
    assert.equal(encoding, "utf8");
    readCount += 1;
    if (readCount === 1) {
      firstReadObserved();
      return "rea";
    }
    return "ready\n";
  }

  let settled = false;
  const pending = waitForExt4PodmanReadyMarker(
    "/session/podman-writer-ready",
    readMarker,
  ).finally(() => {
    settled = true;
  });
  void pending.catch(() => {});
  await firstRead;
  await delay(0);
  assert.equal(settled, false);

  await pending;
  assert.equal(settled, true);
  assert.equal(readCount, 2);
});
