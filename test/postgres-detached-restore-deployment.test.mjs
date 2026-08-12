import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import test from "node:test";

const REGISTER = new URL(
  "./fixtures/postgres-deployment/register-pg-hook.mjs",
  import.meta.url,
);
const SCENARIO = new URL(
  "./fixtures/postgres-deployment/scenario.mjs",
  import.meta.url,
);
const SCENARIOS = Object.freeze([
  "abnormal-pool-end-results-still-attempt-every-pool",
  "admitted-ingress-cannot-stop-its-deployment",
  "after-import-promise-constructor-getter-is-ignored",
  "after-import-promise-species-getter-fails-closed",
  "after-import-promise-try-poison-is-ignored",
  "application-name-budget",
  "checked-out-client-error-forces-fatal-shutdown",
  "zero-io-and-lifecycle",
  "partial-construction-failure",
  "stop-waits-for-pool-acknowledgements",
  "topology-failure",
  "all-pool-ends-attempted",
  "all-settlement-stops-start-before-await",
  "empty-password-blocks-ambient-fallback",
  "exact-physical-config-rejection",
  "idle-pool-error-forces-terminal-shutdown",
  "image-plan-grace-breach-forces-fatal-shutdown",
  "image-plan-reservation-ingress-is-gated-and-drained",
  "image-plan-stop-aborts-and-drains-active-provider",
  "independent-deployments-use-distinct-probe-keys",
  "invalid-client-query-still-releases-and-cleans-up",
  "object-prototype-then-cannot-forge-driver-evidence",
  "hostile-options",
  "hostile-topology-evidence-fails-closed",
  "stop-during-topology-never-reopens-ingress",
  "synchronous-connect-pool-error-uses-assigned-start-promise",
  "topology-failure-with-pool-close-failure",
  "verify-full-tls-configuration",
]);
const MAX_OUTPUT_BYTES = 64 * 1024;

function childLoaderArguments() {
  const [major, minor] = process.versions.node
    .split(".", 2)
    .map((value) => Number.parseInt(value, 10));
  if (major > 20 || (major === 20 && minor >= 6)) {
    return ["--import", REGISTER.href];
  }
  return [
    "--experimental-loader",
    new URL("./fixtures/postgres-deployment/resolve-pg.mjs", import.meta.url)
      .href,
  ];
}

async function runScenario(name) {
  const child = spawn(
    process.execPath,
    [...childLoaderArguments(), SCENARIO.pathname, name],
    {
      env: {
        ...process.env,
        PGBINARY: "1",
        PGDATABASE: "ambient-database-must-not-win",
        PGHOST: "ambient-host-must-not-win",
        PGOPTIONS: "-c search_path=ambient_must_not_win",
        PGPASSWORD: "ambient-password-must-not-win",
        PGPASSFILE: "/ambient/passfile/must-not-win",
        PGREPLICATION: "database",
        PGSSLMODE: "no-verify",
        PGUSER: "ambient-user-must-not-win",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let output = "";
  const append = (chunk) => {
    output += chunk.toString("utf8");
    if (Buffer.byteLength(output, "utf8") > MAX_OUTPUT_BYTES) {
      child.kill("SIGKILL");
    }
  };
  child.stdout.on("data", append);
  child.stderr.on("data", append);
  const timeout = setTimeout(() => child.kill("SIGKILL"), 10_000);
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
  test(`PostgreSQL deployment: ${scenario}`, { timeout: 15_000 }, async () => {
    await runScenario(scenario);
  });
}
