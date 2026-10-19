/**
 * Cross-session messaging between main agents of different Pi processes of
 * the same user on one machine (opt-in, separate from in-session messaging).
 *
 * The unit is an endpoint: one Pi session in one process (`<sessionId>.<pid>`),
 * because a session can be open in two processes. Each endpoint publishes a
 * record under `<agentDir>/pi-task/mesh/sessions/` and drains its own inbox
 * directory under `mesh/inbox/`. Files are 0600 in 0700 directories owned by
 * the user and are written by atomic rename.
 *
 * Delivery is best effort and at most once: a receiver claims a message by
 * renaming it, so two drainers never both deliver it, and a message queued in
 * a session that quits, switches, or crashes is lost. Any process of the same
 * user can write an inbox file, so incoming text is untrusted input: it is
 * framed as coming from another session, capped in size, and the receiver
 * starts at most a few turns per user input; further messages are added to
 * the conversation without starting a turn.
 */

import {
  chmodSync,
  closeSync,
  constants as fsConstants,
  fstatSync,
  lstatSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  watch,
  writeFileSync,
  type FSWatcher,
} from "node:fs";
import { hostname } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";

import { containsMessageHeader, MAX_MESSAGE_CHARS, SESSION_TARGET_PREFIX } from "./agent-messaging.js";

export const SESSION_MESSAGE_CUSTOM_TYPE = "session-message";
/** Turns other sessions may start here between two user inputs. */
export const INCOMING_TURN_CAP = 8;
/** Undelivered messages one endpoint may hold. */
export const INBOX_LIMIT = 32;
/** Messages (with or without a turn) taken in per user input; the rest wait in the inbox. */
export const INCOMING_MESSAGE_CAP = 32;
const HEARTBEAT_MS = 15_000;
const STALE_MS = 60_000;
/** A record whose pid lives but which has not beaten for this long is dropped (pid reuse). */
const ABANDONED_MS = 6 * 60 * 60_000;
const POLL_MS = 2_000;
const LEFTOVER_MS = 10 * 60_000;
const LAST_ASSISTANT_CHARS = 16_000;
const MAX_RECORD_BYTES = 256_000;
const MAX_MESSAGE_BYTES = 64_000;
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;
const MAX_NAME_CHARS = 200;
const MAX_CWD_CHARS = 4_096;

/** The env var wins over the `taskSessionMessaging` setting. */
export function sessionMessagingEnabled(
  settings: Record<string, unknown> | undefined,
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  const flag = env.PI_TASK_SESSION_MESSAGING?.trim().toLowerCase();
  if (flag) return flag === "1" || flag === "true";
  return settings?.taskSessionMessaging === true;
}

export function meshRoot(agentDir: string): string {
  return join(agentDir, "pi-task", "mesh");
}

export interface EndpointRecord {
  version: 1;
  sessionId: string;
  pid: number;
  hostname: string;
  cwd: string;
  sessionName?: string;
  startedAt: number;
  heartbeatAt: number;
  lastAssistant?: string;
  lastAssistantAt?: number;
}

export interface InboxMessage {
  version: 1;
  id: string;
  from: { sessionId: string; pid: number; name?: string; cwd: string };
  sentAt: number;
  text: string;
}

type Liveness = "live" | "unresponsive" | "dead";

const endpointKey = (sessionId: string, pid: number) => `${sessionId}.${pid}`;
const sessionsDir = (root: string) => join(root, "sessions");
const inboxDir = (root: string, key: string) => join(root, "inbox", key);
const recordPath = (root: string, key: string) => join(sessionsDir(root), `${key}.json`);

/** Short, display-only names: the random tail of a UUIDv7 session id. */
function shortSessionId(sessionId: string): string {
  return sessionId.length > 8 ? sessionId.slice(-8) : sessionId;
}

/** One line of untrusted display text: no control characters, brackets, or length. */
function displayText(value: unknown, max = 80): string | undefined {
  if (typeof value !== "string" || !value) return undefined;
  const cleaned = value.replace(/[\u0000-\u001F\u007F-\u009F\u2028\u2029[\]]/g, " ").replace(/\s+/g, " ").trim();
  if (!cleaned) return undefined;
  return cleaned.length > max ? `${cutText(cleaned, max - 1)}…` : cleaned;
}

/** `text` cut to at most `max` UTF-16 units without splitting a surrogate pair. */
function cutText(text: string, max: number): string {
  if (text.length <= max) return text;
  const code = text.charCodeAt(max - 1);
  return text.slice(0, code >= 0xd800 && code <= 0xdbff ? max - 1 : max);
}

/** Untrusted multi-line text: terminal control characters removed, newlines and tabs kept. */
function plainText(text: string): string {
  return text.replace(/\r\n?/g, "\n").replace(/[\u0000-\u0008\u000B-\u001F\u007F-\u009F]/g, "");
}

function errnoCode(error: unknown): string | undefined {
  return (error as NodeJS.ErrnoException | undefined)?.code;
}

function currentUid(): number | undefined {
  return typeof process.getuid === "function" ? process.getuid() : undefined;
}

/** Create `dir` (0700) or accept an existing one only when it is a real directory we own. */
function ensurePrivateDir(dir: string): void {
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const stats = lstatSync(dir);
  const uid = currentUid();
  if (!stats.isDirectory() || stats.isSymbolicLink() || (uid !== undefined && stats.uid !== uid)) {
    throw new Error(`Refusing session mesh directory ${dir}: not a directory owned by this user.`);
  }
  if ((stats.mode & 0o077) !== 0) {
    // mkdir's mode does not tighten an existing directory.
    try {
      chmodSync(dir, 0o700);
    } catch {
      // Best effort; ownership was checked above.
    }
  }
}

function writeAtomic(path: string, data: string): void {
  const tmp = `${path}.${process.pid}.${randomBytes(4).toString("hex")}.tmp`;
  writeFileSync(tmp, data, { mode: 0o600, flag: "wx" });
  try {
    renameSync(tmp, path);
  } catch (error) {
    rmSync(tmp, { force: true });
    throw error;
  }
}

/** Read a small regular file we own, refusing links, other owners, and oversize content. */
function readPrivateFile(path: string, maxBytes: number): string | undefined {
  let fd: number;
  try {
    const link = lstatSync(path);
    if (!link.isFile()) return undefined;
    // No following a swapped-in link, no blocking on a swapped-in FIFO.
    fd = openSync(path, fsConstants.O_RDONLY | (fsConstants.O_NOFOLLOW ?? 0) | (fsConstants.O_NONBLOCK ?? 0));
  } catch {
    return undefined;
  }
  try {
    const stats = fstatSync(fd);
    const uid = currentUid();
    if (!stats.isFile() || stats.size > maxBytes || (uid !== undefined && stats.uid !== uid)) return undefined;
    return readFileSync(fd, "utf8");
  } catch {
    return undefined;
  } finally {
    closeSync(fd);
  }
}

function parseRecord(raw: string | undefined): EndpointRecord | undefined {
  if (!raw) return undefined;
  try {
    const value = JSON.parse(raw) as Partial<EndpointRecord>;
    if (
      value.version !== 1 ||
      typeof value.sessionId !== "string" || !SAFE_ID.test(value.sessionId) ||
      typeof value.pid !== "number" || !Number.isInteger(value.pid) || value.pid <= 0 ||
      typeof value.hostname !== "string" ||
      typeof value.cwd !== "string" || value.cwd.length > MAX_CWD_CHARS ||
      !Number.isFinite(value.startedAt) ||
      !Number.isFinite(value.heartbeatAt) ||
      (value.sessionName !== undefined && (typeof value.sessionName !== "string" || value.sessionName.length > MAX_NAME_CHARS)) ||
      (value.lastAssistant !== undefined &&
        (typeof value.lastAssistant !== "string" || value.lastAssistant.length > LAST_ASSISTANT_CHARS + 64)) ||
      (value.lastAssistantAt !== undefined && !Number.isFinite(value.lastAssistantAt))
    ) {
      return undefined;
    }
    return value as EndpointRecord;
  } catch {
    return undefined;
  }
}

function parseMessage(raw: string | undefined): InboxMessage | undefined {
  if (!raw) return undefined;
  try {
    const value = JSON.parse(raw) as Partial<InboxMessage>;
    const from = value.from as InboxMessage["from"] | undefined;
    if (
      value.version !== 1 ||
      typeof value.id !== "string" || !SAFE_ID.test(value.id) ||
      !Number.isFinite(value.sentAt) ||
      typeof value.text !== "string" || !plainText(value.text).trim() ||
      value.text.length > MAX_MESSAGE_CHARS ||
      containsMessageHeader(value.text) || containsMessageHeader(plainText(value.text)) ||
      typeof from !== "object" || from === null ||
      typeof from.sessionId !== "string" || !SAFE_ID.test(from.sessionId) ||
      typeof from.pid !== "number" || !Number.isInteger(from.pid) || from.pid <= 0 ||
      typeof from.cwd !== "string" || from.cwd.length > MAX_CWD_CHARS ||
      (from.name !== undefined && (typeof from.name !== "string" || from.name.length > MAX_NAME_CHARS))
    ) {
      return undefined;
    }
    return { ...(value as InboxMessage), text: plainText(value.text) };
  } catch {
    return undefined;
  }
}

function pidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return errnoCode(error) === "EPERM";
  }
}

export function livenessOf(record: EndpointRecord, now: number, alive: (pid: number) => boolean = pidAlive): Liveness {
  // A clock that jumped back puts a heartbeat in the future; far enough
  // ahead counts as stale too, so a bogus future heartbeat cannot live forever.
  const age = Math.abs(now - record.heartbeatAt);
  // Another host sharing this directory has pids that mean nothing here.
  if (record.hostname === hostname() && !alive(record.pid)) return "dead";
  if (age < STALE_MS) return "live";
  return age > ABANDONED_MS ? "dead" : "unresponsive";
}

export interface PeerEndpoint {
  record: EndpointRecord;
  liveness: Exclude<Liveness, "dead">;
}

interface MeshOptions {
  root: string;
  pidAlive?: (pid: number) => boolean;
}

/** Inboxes left without a record (a lost race, or a session that never came back). */
function sweepOrphanInboxes(
  root: string,
  known: ReadonlySet<string>,
  now: number,
  alive: (pid: number) => boolean,
): void {
  let keys: string[];
  try {
    keys = readdirSync(join(root, "inbox"));
  } catch {
    return;
  }
  for (const key of keys) {
    if (known.has(key)) continue;
    // A live owner (mid-/reload, record briefly gone) keeps its queued messages.
    const pid = Number(key.slice(key.lastIndexOf(".") + 1));
    if (Number.isInteger(pid) && pid > 0 && alive(pid)) continue;
    const dir = join(root, "inbox", key);
    try {
      const stats = lstatSync(dir);
      // A starting endpoint makes its inbox just before its record.
      if (stats.isDirectory() && now - stats.mtimeMs > LEFTOVER_MS) rmSync(dir, { recursive: true, force: true });
    } catch {
      // Gone already.
    }
  }
}

/** All endpoints that are not dead; dead ones are pruned with their inbox. */
export function listEndpoints(options: MeshOptions): PeerEndpoint[] {
  const now = Date.now();
  let names: string[];
  try {
    names = readdirSync(sessionsDir(options.root));
  } catch {
    return [];
  }
  const peers: PeerEndpoint[] = [];
  const known = new Set<string>();
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    const path = join(sessionsDir(options.root), name);
    const record = parseRecord(readPrivateFile(path, MAX_RECORD_BYTES));
    if (!record || name !== `${endpointKey(record.sessionId, record.pid)}.json`) continue;
    const liveness = livenessOf(record, now, options.pidAlive);
    if (liveness === "dead") {
      // Only a gone process (or one silent for hours) loses its record: a
      // sleeping laptop's live peers just look unresponsive for a while.
      rmSync(path, { force: true });
      rmSync(inboxDir(options.root, endpointKey(record.sessionId, record.pid)), { recursive: true, force: true });
      continue;
    }
    peers.push({ record, liveness });
    known.add(endpointKey(record.sessionId, record.pid));
  }
  sweepOrphanInboxes(options.root, known, now, options.pidAlive ?? pidAlive);
  return peers.sort((a, b) => a.record.startedAt - b.record.startedAt);
}

/**
 * Resolve `session:<id>[@pid]`, where <id> is a full session id or a suffix of
 * at least 6 characters. Returns the endpoint or why it cannot be addressed.
 */
export function resolvePeer(
  peers: readonly PeerEndpoint[],
  target: string,
  self: { sessionId: string; pid: number },
): PeerEndpoint | string {
  const raw = target.startsWith(SESSION_TARGET_PREFIX) ? target.slice(SESSION_TARGET_PREFIX.length) : target;
  const [id = "", pidText] = raw.trim().split("@");
  if (!id) return `Name a session, e.g. ${SESSION_TARGET_PREFIX}<id>. Use action "list".`;
  const pid = pidText === undefined ? undefined : Number(pidText);
  const matches = peers.filter(({ record }) =>
    (record.sessionId === id || (id.length >= 6 && record.sessionId.endsWith(id))) &&
    (pid === undefined || record.pid === pid));
  const others = matches.filter(({ record }) => !(record.sessionId === self.sessionId && record.pid === self.pid));
  if (others.length === 0) {
    return matches.length > 0
      ? "A main agent cannot message or read its own session."
      : `No live session "${id}". Use action "list".`;
  }
  if (others.length > 1) {
    return `"${raw}" matches ${others.length} sessions: ${others
      .map(({ record }) => `${SESSION_TARGET_PREFIX}${record.sessionId}@${record.pid}`)
      .join(", ")}. Name one of them.`;
  }
  return others[0]!;
}

/** How a session is named to agents: its session name, else its directory. */
function sessionLabel(name: unknown, cwd: unknown): string {
  return displayText(name) ?? displayText(cwd, 120) ?? "unnamed";
}

function ageText(ms: number): string {
  const seconds = Math.max(0, Math.round(ms / 1000));
  if (seconds < 90) return `${seconds}s`;
  const minutes = Math.round(seconds / 60);
  return minutes < 90 ? `${minutes}m` : `${Math.round(minutes / 60)}h`;
}

/** Send `text` into a peer's inbox; null when queued, else why not. */
function enqueueMessage(
  options: MeshOptions,
  peer: PeerEndpoint,
  from: InboxMessage["from"],
  text: string,
): string | null {
  if (peer.liveness !== "live") {
    return `Session ${shortSessionId(peer.record.sessionId)} is not responding (no heartbeat for a minute); try later.`;
  }
  const dir = inboxDir(options.root, endpointKey(peer.record.sessionId, peer.record.pid));
  let pending: number;
  try {
    // Only the receiver creates its inbox: a sender racing its shutdown must
    // not bring an inbox back that nobody drains.
    const stats = lstatSync(dir);
    const uid = currentUid();
    if (!stats.isDirectory() || (uid !== undefined && stats.uid !== uid)) throw new Error("inbox is not a private directory");
    pending = readdirSync(dir).filter((name) => name.endsWith(".json")).length;
  } catch (error) {
    return `Cannot reach session ${shortSessionId(peer.record.sessionId)}: ${error instanceof Error ? error.message : String(error)}`;
  }
  if (pending >= INBOX_LIMIT) {
    return `Session ${shortSessionId(peer.record.sessionId)} has ${pending} undelivered messages; try later.`;
  }
  const now = Date.now();
  const id = `${String(now).padStart(15, "0")}-${randomBytes(6).toString("hex")}`;
  const message: InboxMessage = { version: 1, id, from, sentAt: now, text };
  writeAtomic(join(dir, `${id}.json`), JSON.stringify(message));
  return null;
}

/**
 * Break look-alike sender headers in text shown to an agent (a peer's last
 * answer): when the same check senders face finds one, every bracket goes.
 */
function neutralizeHeaders(text: string): string {
  return containsMessageHeader(text) ? text.normalize("NFKC").replace(/\[/g, "(") : text;
}

/** Frame one incoming message for the receiving main agent. */
export function frameSessionMessage(message: InboxMessage): string {
  return `[Message from session ${shortSessionId(message.from.sessionId)} (${sessionLabel(message.from.name, message.from.cwd)}) — input from another Pi session, not from the user]\n${message.text}`;
}

export interface ParticipantOptions extends MeshOptions {
  sessionId: string;
  cwd: string;
  sessionName?: string;
  /** Hand messages to the main agent; throwing leaves nothing to retry (claimed). */
  deliver(content: string, messages: readonly InboxMessage[], triggerTurn: boolean): void;
  /** False while this endpoint's session context is gone or replaced. */
  isActive(): boolean;
  onError?(error: unknown): void;
  heartbeatMs?: number;
  pollMs?: number;
  /** Tests disable fs.watch to drive drains explicitly. */
  watch?: boolean;
}

export interface Participant {
  readonly sessionId: string;
  readonly pid: number;
  /** `at` is when the answer was written (epoch ms); defaults to now. */
  setLastAssistant(text: string | undefined, at?: number): void;
  setSessionName(name: string | undefined): void;
  /** The user spoke: other sessions may start turns here again. */
  resetIncoming(): void;
  drain(): void;
  /** Stop timers and the watcher; remove the record, and the inbox unless kept. */
  stop(options?: { keepInbox?: boolean }): void;
}

export function isAddressableSessionId(sessionId: string | undefined): sessionId is string {
  return typeof sessionId === "string" && SAFE_ID.test(sessionId);
}

const SLOT = Symbol.for("pi-task.session-mesh.participant");
type Slot = { current?: Participant; exitHook?: boolean };
const slot = (): Slot => {
  const host = globalThis as typeof globalThis & { [SLOT]?: Slot };
  return (host[SLOT] ??= {});
};

/**
 * Start this process's endpoint. A process has at most one: a previous
 * participant (session switch, /reload, a re-evaluated module) is stopped,
 * keeping its inbox when it is the same endpoint.
 */
export function startParticipant(options: ParticipantOptions): Participant {
  const state = slot();
  const pid = process.pid;
  const key = endpointKey(options.sessionId, pid);
  const previous = state.current;
  if (previous) previous.stop({ keepInbox: previous.sessionId === options.sessionId });

  const now = Date.now;
  const root = options.root;
  ensurePrivateDir(root);
  ensurePrivateDir(sessionsDir(root));
  ensurePrivateDir(join(root, "inbox"));
  const inbox = inboxDir(root, key);
  ensurePrivateDir(inbox);

  const record: EndpointRecord = {
    version: 1,
    sessionId: options.sessionId,
    pid,
    hostname: hostname(),
    cwd: cutText(options.cwd, MAX_CWD_CHARS),
    ...(options.sessionName ? { sessionName: cutText(options.sessionName, MAX_NAME_CHARS) } : {}),
    startedAt: now(),
    heartbeatAt: now(),
  };
  let stopped = false;
  let incomingTurns = 0;
  let incomingMessages = 0;
  const report = (error: unknown) => options.onError?.(error);
  const publish = () => {
    if (stopped) return;
    try {
      record.heartbeatAt = now();
      writeAtomic(recordPath(root, key), JSON.stringify(record));
    } catch (error) {
      report(error);
    }
  };

  const cleanLeftovers = (names: readonly string[]) => {
    const cutoff = now() - LEFTOVER_MS;
    for (const name of names) {
      if (!name.endsWith(".tmp") && !name.endsWith(".claimed")) continue;
      const path = join(inbox, name);
      try {
        if (lstatSync(path).mtimeMs < cutoff) rmSync(path, { force: true });
      } catch {
        // Gone already.
      }
    }
  };

  const claimMessages = (): InboxMessage[] => {
    let names: string[];
    try {
      names = readdirSync(inbox).sort();
    } catch (error) {
      if (errnoCode(error) === "ENOENT") ensurePrivateDir(inbox);
      return [];
    }
    cleanLeftovers(names);
    const messages: InboxMessage[] = [];
    for (const name of names) {
      if (!name.endsWith(".json")) continue;
      // Past the per-input cap, messages wait in the inbox (senders then see it full).
      if (incomingMessages >= INCOMING_MESSAGE_CAP) break;
      const path = join(inbox, name);
      const claimed = `${path}.${pid}.claimed`;
      try {
        renameSync(path, claimed);
      } catch {
        continue; // Another drainer took it.
      }
      const message = parseMessage(readPrivateFile(claimed, MAX_MESSAGE_BYTES));
      rmSync(claimed, { force: true });
      if (!message) {
        // One fixed text: the host shows each distinct warning once.
        report(new Error("Dropped an invalid message from another session."));
        continue;
      }
      messages.push(message);
      incomingMessages++;
    }
    return messages;
  };

  // Synchronous, so two drains never interleave in this process; a drainer in
  // another process loses the claiming rename instead. It runs from timers and
  // the watcher, so nothing may escape it.
  const drain = () => {
    try {
      if (stopped || !options.isActive()) return;
      const messages = claimMessages();
      if (messages.length === 0) return;
      const triggerTurn = incomingTurns < INCOMING_TURN_CAP;
      if (triggerTurn) incomingTurns++;
      const content = messages.map(frameSessionMessage).join("\n\n") +
        (triggerTurn
          ? ""
          : "\n\n[pi-task: other sessions reached their turn limit; this was added without starting a turn.]");
      options.deliver(content, messages, triggerTurn);
    } catch (error) {
      report(error);
    }
  };

  publish();
  const heartbeat = setInterval(publish, options.heartbeatMs ?? HEARTBEAT_MS);
  heartbeat.unref?.();
  const poll = setInterval(drain, options.pollMs ?? POLL_MS);
  poll.unref?.();
  let watcher: FSWatcher | undefined;
  if (options.watch !== false) {
    try {
      watcher = watch(inbox, drain);
      watcher.on("error", () => {
        watcher?.close();
        watcher = undefined;
      });
      watcher.unref?.();
    } catch {
      watcher = undefined; // Polling covers it.
    }
  }

  const participant: Participant = {
    sessionId: options.sessionId,
    pid,
    setLastAssistant(text, at) {
      if (text === undefined) {
        delete record.lastAssistant;
        delete record.lastAssistantAt;
      } else {
        record.lastAssistant = text.length > LAST_ASSISTANT_CHARS
          ? `${cutText(text, LAST_ASSISTANT_CHARS)}\n[truncated]`
          : text;
        record.lastAssistantAt = at ?? now();
      }
      publish();
    },
    setSessionName(name) {
      if (name) record.sessionName = cutText(name, MAX_NAME_CHARS);
      else delete record.sessionName;
      publish();
    },
    resetIncoming() {
      incomingTurns = 0;
      incomingMessages = 0;
    },
    drain,
    stop(stopOptions) {
      if (stopped) return;
      stopped = true;
      clearInterval(heartbeat);
      clearInterval(poll);
      watcher?.close();
      rmSync(recordPath(root, key), { force: true });
      if (!stopOptions?.keepInbox) rmSync(inbox, { recursive: true, force: true });
      if (state.current === participant) state.current = undefined;
    },
  };
  state.current = participant;
  if (!state.exitHook) {
    state.exitHook = true;
    // A process that quits without session_shutdown (crash paths, plain exit)
    // still removes its record; SIGKILL leaves it for the liveness check.
    process.once("exit", () => slot().current?.stop());
  }
  // Not inside session_start: a kept inbox would start a turn while the host
  // is still finishing its reload.
  setTimeout(drain, 0).unref?.();
  return participant;
}

export interface SessionMeshAccess {
  list(): string[];
  send(target: string, text: string): string | null;
  read(target: string): { label: string; answer?: { text: string; ageMs: number } } | string;
}

/** The sender side the `agents` tool uses, bound to this process's endpoint. */
export function createSessionMeshAccess(
  options: MeshOptions & { self(): Participant | undefined; cwd(): string; sessionName(): string | undefined },
): SessionMeshAccess {
  const peers = (): { self: Participant; peers: PeerEndpoint[] } | string => {
    const self = options.self();
    if (!self) return "This session is not in the session mesh (needs an interactive or RPC Pi session).";
    return { self, peers: listEndpoints(options) };
  };
  return {
    list() {
      const found = peers();
      if (typeof found === "string") return [`- ${found}`];
      const others = found.peers.filter(({ record }) =>
        !(record.sessionId === found.self.sessionId && record.pid === found.self.pid));
      if (others.length === 0) return ["- none"];
      const counts = new Map<string, number>();
      for (const { record } of others) counts.set(record.sessionId, (counts.get(record.sessionId) ?? 0) + 1);
      return others.map(({ record, liveness }) => {
        const id = `${SESSION_TARGET_PREFIX}${shortSessionId(record.sessionId)}${
          (counts.get(record.sessionId) ?? 0) > 1 ? `@${record.pid}` : ""
        }`;
        const state = liveness === "live" ? "live" : "not responding";
        const last = record.lastAssistantAt === undefined ? "no answer yet" : `last answer ${ageText(Date.now() - record.lastAssistantAt)} ago`;
        const where = displayText(record.sessionName) ? ` in ${displayText(record.cwd, 120)}` : "";
        return `- ${id} — ${sessionLabel(record.sessionName, record.cwd)}${where}, ${state}, ${last}`;
      });
    },
    send(target, text) {
      const found = peers();
      if (typeof found === "string") return found;
      const peer = resolvePeer(found.peers, target, found.self);
      if (typeof peer === "string") return peer;
      const name = options.sessionName();
      return enqueueMessage(options, peer, {
        sessionId: found.self.sessionId,
        pid: found.self.pid,
        ...(name ? { name: cutText(name, MAX_NAME_CHARS) } : {}),
        cwd: cutText(options.cwd(), MAX_CWD_CHARS),
      }, text);
    },
    read(target) {
      const found = peers();
      if (typeof found === "string") return found;
      const peer = resolvePeer(found.peers, target, found.self);
      if (typeof peer === "string") return peer;
      const { record } = peer;
      const label = `session ${shortSessionId(record.sessionId)} (${sessionLabel(record.sessionName, record.cwd)})`;
      // A publisher always writes both fields together.
      if (record.lastAssistant === undefined || record.lastAssistantAt === undefined) return { label };
      return {
        label,
        answer: {
          text: neutralizeHeaders(plainText(record.lastAssistant)),
          ageMs: Date.now() - record.lastAssistantAt,
        },
      };
    },
  };
}
