import { spawn } from "node:child_process";
import { writeFile, writeFileSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const [mode, markerPath, pidPath] = process.argv.slice(2);

if (mode === "grandchild") {
  process.on("SIGTERM", () => {});
  await delay(250);
  await new Promise((resolve, reject) => {
    writeFile(markerPath, "detached grandchild mutation\n", (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
  await delay(30_000);
} else {
  const fixturePath = fileURLToPath(import.meta.url);
  const grandchild = spawn(
    process.execPath,
    [fixturePath, "grandchild", markerPath, pidPath],
    { detached: true, stdio: "ignore" },
  );
  try {
    writeFileSync(pidPath, `${grandchild.pid}\n`, { mode: 0o600 });
  } catch (error) {
    process.kill(grandchild.pid, "SIGKILL");
    throw error;
  }
  grandchild.unref();
  process.stdout.write(
    `${JSON.stringify({
      method: "fixture/detached-grandchild",
      params: { pid: grandchild.pid },
    })}\n`,
  );
  process.stdin.resume();
  process.stdin.on("end", () => process.exit(0));
}
