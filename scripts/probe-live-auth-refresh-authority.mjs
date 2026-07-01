#!/usr/bin/env node

import { probeLiveAuthRefreshAuthority } from "../src/live-auth-refresh-authority-probe.mjs";
import { managedAuthRefreshErrorMetadata } from "../src/managed-auth-refresh.mjs";

try {
  const result = await probeLiveAuthRefreshAuthority();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} catch (error) {
  console.error(error instanceof Error ? error.stack : error);
  const metadata = managedAuthRefreshErrorMetadata(error);
  if (Object.keys(metadata).length > 0) {
    console.error(`managed auth refresh metadata: ${JSON.stringify(metadata)}`);
  }
  process.exitCode = 1;
}
