#!/usr/bin/env node

import { probeLiveAuthRefreshAuthority } from "../src/live-auth-refresh-authority-probe.mjs";

try {
  const result = await probeLiveAuthRefreshAuthority();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} catch (error) {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
}
