import { rename } from "node:fs/promises";
import { createInterface } from "node:readline";

const input = createInterface({ input: process.stdin });
let operations = Promise.resolve();

async function handleCommand(line) {
  let command;
  try {
    command = JSON.parse(line);
    if (
      command?.action !== "rename" ||
      !Number.isSafeInteger(command.id) ||
      typeof command.source !== "string" ||
      typeof command.destination !== "string"
    ) {
      throw new Error("invalid lock holder command");
    }
    await rename(command.source, command.destination);
    process.stdout.write(`${JSON.stringify({ id: command.id, ok: true })}\n`);
  } catch (error) {
    const id = Number.isSafeInteger(command?.id) ? command.id : null;
    const code = typeof error?.code === "string" ? error.code : "invalid_command";
    process.stdout.write(`${JSON.stringify({ id, ok: false, code })}\n`);
  }
}

input.on("line", (line) => {
  operations = operations.then(() => handleCommand(line));
});
input.on("close", async () => {
  await operations.catch(() => {});
  process.exit(0);
});
process.stdin.on("error", () => input.close());

process.stdout.write("locked\n");
