import { refreshManagedAuthRecord } from "../src/managed-auth-refresh.mjs";

try {
  await refreshManagedAuthRecord({ authHome: process.argv[2] });
  process.exitCode = 1;
} catch (error) {
  process.stdout.write(`${error?.code ?? "unknown"}\n`);
}
