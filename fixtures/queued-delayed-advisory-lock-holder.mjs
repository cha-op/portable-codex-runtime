import { rename, writeFile } from "node:fs/promises";
import { createInterface } from "node:readline";

const [receivedMarker] = process.argv.slice(2);
const input = createInterface({ input: process.stdin });

input.on("line", async (line) => {
  const command = JSON.parse(line);
  await writeFile(receivedMarker, "queued\n", { mode: 0o600 });
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

process.stdout.write("locked\n");
