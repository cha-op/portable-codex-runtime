const mode = process.argv[2];

process.on("SIGTERM", () => {});
process.stdin.on("end", () => {});
process.stdin.on("error", () => {});
process.stdin.resume();

if (mode === "release") process.stdout.write("locked\n");
setInterval(() => {}, 1_000);
