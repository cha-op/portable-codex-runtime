#!/usr/bin/env node

import { resolve } from "node:path";

import {
  interruptedTurnRecoveryFailureReport,
  probeInterruptedTurnRecovery,
} from "../src/interrupted-turn-recovery-probe.mjs";

const writeEvidence = process.argv.slice(2).includes("--write-evidence");
const evidencePath = resolve(
  process.env.CODEX_RECOVERY_EVIDENCE ?? "evidence/interrupted-turn-recovery.json",
);

try {
  const report = await probeInterruptedTurnRecovery({
    codexBin: process.env.CODEX_BIN,
    evidencePath,
    writeEvidence,
  });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
} catch {
  process.stderr.write(`${JSON.stringify(interruptedTurnRecoveryFailureReport())}\n`);
  process.exitCode = 1;
}
