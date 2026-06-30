#!/usr/bin/env node

import { runAppServerAuthProbe } from "../src/app-server-auth-probe.mjs";

try {
  const result = await runAppServerAuthProbe();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} catch (error) {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
}
