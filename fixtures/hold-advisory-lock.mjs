import { acquireAdvisoryLock } from "../src/advisory-lock.mjs";

const lock = await acquireAdvisoryLock(process.argv[2]);
process.stdout.write("ready\n", () => process.stdin.resume());
process.stdin.on("end", async () => {
  await lock.release();
  process.exit(0);
});
