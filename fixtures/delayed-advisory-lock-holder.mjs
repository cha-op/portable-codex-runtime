import { rename } from "node:fs/promises";
import { createInterface } from "node:readline";

const input = createInterface({ input: process.stdin });

input.on("line", (line) => {
  const command = JSON.parse(line);
  setTimeout(async () => {
    await rename(command.source, command.destination);
    process.stdout.write(`${JSON.stringify({ id: command.id, ok: true })}\n`);
  }, 250);
});

process.stdout.write("locked\n");
