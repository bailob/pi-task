import { join } from "node:path";
import { readRecentToolCalls } from "../helpers.js";
import { readRegistry, updateRegistry } from "../conversation.js";
import type { BackgroundTask, RegistryEntry } from "../types.js";

/**
 * Per-request soft turn budget (issue #30). `task.turns` stays a lifetime
 * counter; the soft limit applies to the turns since `turnBaseline`. The
 * wrap-up anchor and its grace window stay in absolute lifetime units.
 */

/**
 * Use the stats poller's reader and arguments so both counts agree. Preserve
 * its best-effort (possibly partial) count; unexpected errors fall back to 0
 * instead of failing a launch or follow-up.
 */
export function snapshotPiTurns(
  artifactsDir: string,
  id: string,
  sessionName: string,
): number {
  try {
    return readRecentToolCalls(join(artifactsDir, "sessions", id), 12, sessionName).turns;
  } catch {
    return 0;
  }
}

function isNonNegativeFinite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

/** Validate persisted budget state; absent or invalid values fall back to the legacy meaning. */
export function restoredTurnBudget(
  entry: Pick<RegistryEntry, "turnBaseline" | "wrapUp">,
): Pick<BackgroundTask, "turnBaseline" | "wrapUp"> {
  const wrapUp = entry.wrapUp as { turnsAtStart?: unknown } | null | undefined;
  return {
    turnBaseline: isNonNegativeFinite(entry.turnBaseline) ? entry.turnBaseline : 0,
    ...(wrapUp && typeof wrapUp === "object" && isNonNegativeFinite(wrapUp.turnsAtStart)
      ? { wrapUp: { turnsAtStart: wrapUp.turnsAtStart } }
      : {}),
  };
}

/**
 * Field-level merge of budget state into the CURRENT registry entry (never a
 * whole-entry overwrite from an earlier read). `wrapUp: null` removes the
 * anchor. A task without a registry entry is left alone and the registry file
 * is not created. May throw on an unreadable/unwritable registry: callers
 * decide how best-effort the write is.
 */
export function persistTurnBudget(
  piDir: string,
  id: string,
  patch: { turnBaseline?: number; wrapUp?: { turnsAtStart: number } | null },
): void {
  if (!readRegistry(piDir).some((entry) => entry.id === id)) return;
  updateRegistry(piDir, (entries) => {
    const idx = entries.findIndex((entry) => entry.id === id);
    if (idx === -1) return entries;
    const next: RegistryEntry = { ...entries[idx]! };
    if (patch.turnBaseline !== undefined) next.turnBaseline = patch.turnBaseline;
    if (patch.wrapUp === null) delete next.wrapUp;
    else if (patch.wrapUp !== undefined) next.wrapUp = patch.wrapUp;
    entries[idx] = next;
    return entries;
  });
}

/**
 * Explicit successful follow-up to an active task: renew the soft budget from
 * the baseline snapshotted BEFORE the steer and clear any wrap-up. In-memory
 * grant always applies; the durable write is best-effort because the steer has
 * already been delivered and must not be reported as failed.
 */
export function grantFollowUpBudget(
  piDir: string,
  id: string,
  task: BackgroundTask,
  baseline: number,
): void {
  task.turnBaseline = baseline;
  delete task.wrapUp;
  try {
    persistTurnBudget(piDir, id, { turnBaseline: baseline, wrapUp: null });
  } catch (error) {
    console.error(
      `[pi-task] task ${id} follow-up turn budget was not persisted: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * State a replacement tracker inherits from the tracker it replaces (the
 * in-memory tracker is authoritative) or, when none exists, from the persisted
 * registry entry, so a failed follow-up leaves the previous budget exactly as
 * it was.
 */
export function carriedTrackerState(
  prior: BackgroundTask | undefined,
  entry: RegistryEntry | undefined,
): Pick<BackgroundTask, "toolUses" | "turns" | "recentCalls" | "turnBaseline" | "wrapUp"> {
  if (!prior) {
    return {
      toolUses: 0,
      turns: 0,
      recentCalls: [],
      ...(entry ? restoredTurnBudget(entry) : {}),
    };
  }
  return {
    toolUses: prior.toolUses,
    turns: prior.turns,
    recentCalls: prior.recentCalls,
    ...(prior.turnBaseline !== undefined ? { turnBaseline: prior.turnBaseline } : {}),
    ...(prior.wrapUp ? { wrapUp: prior.wrapUp } : {}),
  };
}
