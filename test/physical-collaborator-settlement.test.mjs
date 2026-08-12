import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import test from "node:test";

const SCENARIO = new URL(
  "./fixtures/physical-collaborator-settlement/scenario.mjs",
  import.meta.url,
);
const SCENARIOS = Object.freeze([
  "shape-and-normal-settlement",
  "deadline-late-settlement",
  "grace-breach",
  "stop-drain",
  "cached-promise",
  "hostile-boundaries",
  "real-timers",
]);
const MAX_OUTPUT_BYTES = 64 * 1024;

async function runScenario(name) {
  const child = spawn(process.execPath, [SCENARIO.pathname, name], {
    env: process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  const append = (chunk) => {
    output += chunk.toString("utf8");
    if (Buffer.byteLength(output, "utf8") > MAX_OUTPUT_BYTES) {
      child.kill("SIGKILL");
    }
  };
  child.stdout.on("data", append);
  child.stderr.on("data", append);
  const timeout = setTimeout(() => child.kill("SIGKILL"), 3_000);
  timeout.unref();
  const [code, signal] = await once(child, "exit");
  clearTimeout(timeout);
  assert.equal(
    code,
    0,
    `${name} failed with signal ${signal ?? "none"}:\n${output}`,
  );
  const line = output.trim().split("\n").at(-1);
  assert.deepEqual(JSON.parse(line), { scenario: name, status: "passed" });
}

for (const scenario of SCENARIOS) {
  test(`physical collaborator settlement: ${scenario}`, {
    timeout: 5_000,
  }, async () => {
    await runScenario(scenario);
  });
}
