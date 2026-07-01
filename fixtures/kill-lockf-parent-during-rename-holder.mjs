import { rename } from "node:fs/promises";
import { createInterface } from "node:readline";

if (process.platform !== "darwin") process.exit(2);

const lockfPid = process.ppid;
const input = createInterface({ input: process.stdin });
let commandReceived = false;

process.on("SIGTERM", () => {});

input.on("line", (line) => {
  if (commandReceived) return;
  commandReceived = true;
  const command = JSON.parse(line);
  setTimeout(() => process.kill(lockfPid, "SIGKILL"), 25);
  setTimeout(async () => {
    try {
      await rename(command.source, command.destination);
      process.stdout.write(`${JSON.stringify({ id: command.id, ok: true })}\n`);
    } catch (error) {
      process.stdout.write(
        `${JSON.stringify({ id: command.id, ok: false, code: error?.code ?? "unknown" })}\n`,
      );
    }
  }, 250);
});

process.stdin.on("error", () => {});
process.stdout.write("locked\n");
setInterval(() => {}, 1_000);
