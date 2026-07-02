#!/usr/bin/env node

import {
  runInterruptedTurnRecoveryCli,
} from "../src/interrupted-turn-recovery-probe.mjs";

process.exitCode = await runInterruptedTurnRecoveryCli();
