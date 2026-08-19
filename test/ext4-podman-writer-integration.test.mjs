import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import {
  waitForExt4PodmanReadyMarker,
} from "../integration/ext4-podman-writer.mjs";

test("ext4 Podman marker polling tolerates a published partial write", async () => {
  let firstReadObserved;
  const firstRead = new Promise((resolve) => {
    firstReadObserved = resolve;
  });
  let readCount = 0;
  async function readMarker(path, encoding) {
    assert.equal(path, "/session/podman-writer-ready");
    assert.equal(encoding, "utf8");
    readCount += 1;
    if (readCount === 1) {
      firstReadObserved();
      return "rea";
    }
    return "ready\n";
  }

  let settled = false;
  const pending = waitForExt4PodmanReadyMarker(
    "/session/podman-writer-ready",
    readMarker,
  ).finally(() => {
    settled = true;
  });
  void pending.catch(() => {});
  await firstRead;
  await delay(0);
  assert.equal(settled, false);

  await pending;
  assert.equal(settled, true);
  assert.equal(readCount, 2);
});
