#!/usr/bin/env node

import { probeLiveAuthRefreshAuthority } from "../src/live-auth-refresh-authority-probe.mjs";
import { managedAuthRefreshFailureReport } from "../src/managed-auth-refresh.mjs";

try {
  const result = await probeLiveAuthRefreshAuthority();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} catch (error) {
  process.stderr.write(`${JSON.stringify(managedAuthRefreshFailureReport(error))}\n`);
  process.exitCode = 1;
}
