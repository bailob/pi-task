/**
 * Child runner for test/sessionMesh.test.ts (not a test itself): loads the
 * real extension with cross-session messaging on in a fake TUI session, drops
 * invalid messages into its inbox, and prints where warnings went.
 *
 * Usage: tsx test/session-mesh-index-child.ts <agentDir> <cwd>
 */

import { readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [agentDir, cwd] = process.argv.slice(2);
if (!agentDir || !cwd) {
  console.error("usage: session-mesh-index-child.ts <agentDir> <cwd>");
  process.exit(1);
}
process.chdir(cwd);
process.env.PI_CODING_AGENT_DIR = agentDir;
process.env.PI_TASK_SESSION_MESSAGING = "1";
delete process.env.PI_TASK_AGENT_MESSAGING;
delete process.env.PI_TASK_TOOL_DISABLED;

const stderr: string[] = [];
const notifies: { message: string; type?: string }[] = [];
console.error = (...args: unknown[]) => void stderr.push(args.map(String).join(" "));
console.warn = console.error;

const { default: registerTaskExtension } = await import("../src/index.js");
const handlers = new Map<string, ((event: unknown, ctx: unknown) => unknown)[]>();
registerTaskExtension({
  on(event: string, handler: (event: unknown, ctx: unknown) => unknown) {
    handlers.set(event, [...(handlers.get(event) ?? []), handler]);
  },
  registerFlag() {},
  getFlag() { return false; },
  registerTool() {},
  registerCommand() {},
  registerMessageRenderer() {},
  registerProvider() {},
  getAllTools() { return []; },
  getSettings() { return {}; },
  sendMessage() {},
} as never);

const ctx = {
  cwd,
  mode: "tui",
  hasUI: true,
  sessionManager: {
    getSessionId: () => "sess-index-child",
    getSessionName: () => undefined,
    getLeafId: () => null,
    getBranch: () => [],
    getSessionFile: () => undefined,
  },
  ui: { notify: (message: string, type?: string) => void notifies.push({ message, type }) },
};
for (const handler of handlers.get("session_start") ?? []) {
  await handler({ type: "session_start", reason: "startup" }, ctx);
}

const inboxRoot = join(agentDir, "pi-task", "mesh", "inbox");
const inbox = join(inboxRoot, readdirSync(inboxRoot)[0]!);
for (const name of ["1-bad.json", "2-bad.json"]) {
  writeFileSync(join(inbox, name), "{not json", { mode: 0o600 });
  await new Promise((resolve) => setTimeout(resolve, 2_500));
}
console.log(JSON.stringify({
  notifies,
  meshStderr: stderr.filter((line) => line.includes("session mesh")),
}));
process.exit(0);
