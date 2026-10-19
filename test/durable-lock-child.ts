/**
 * Child runner for test/durableStoreLock.test.ts (not a test itself): takes
 * the durable store lock from a separate process.
 *
 * Usage: tsx test/durable-lock-child.ts <databasePath> try|hold
 * - try:  prints `acquired` or `locked <pid>` and exits.
 * - hold: prints `held` and keeps the lock until killed.
 */

import {
  acquireDurableStoreLock,
  isDurableStoreLockedError,
} from "../src/subagent/durable-store-lock.js";

const [databasePath, mode] = process.argv.slice(2);
if (!databasePath || (mode !== "try" && mode !== "hold")) {
  console.error("usage: durable-lock-child.ts <databasePath> try|hold");
  process.exit(1);
}

try {
  const release = await acquireDurableStoreLock(databasePath);
  if (mode === "try") {
    release();
    console.log("acquired");
    process.exit(0);
  }
  console.log("held");
  setInterval(() => undefined, 60_000);
} catch (error) {
  if (!isDurableStoreLockedError(error)) throw error;
  console.log(`locked ${error.holderPid ?? ""}`.trim());
  process.exit(0);
}
