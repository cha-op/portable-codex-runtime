if (process.platform !== "darwin") process.exit(2);

const lockfPid = process.ppid;
process.stdout.write("locked\n", () => {
  setTimeout(() => {
    process.kill(lockfPid, "SIGKILL");
    process.exit(0);
  }, 50);
});
