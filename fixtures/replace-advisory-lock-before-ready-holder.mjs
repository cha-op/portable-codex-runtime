import { rename, writeFile } from "node:fs/promises";

const [lockPath, displacedPath] = process.argv.slice(2);
await rename(lockPath, displacedPath);
await writeFile(lockPath, "replacement\n", { mode: 0o600 });

process.stdout.write("locked\n", () => process.stdin.resume());
process.stdin.on("end", () => process.exit(0));
