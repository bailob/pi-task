/**
 * Child runner for test/sessionMesh.test.ts (not a test itself): one Pi
 * process's endpoint that prints `ready`, then every delivery as a JSON line.
 *
 * Usage: tsx test/session-mesh-peer.ts <meshRoot> <sessionId>
 */

import { startParticipant } from "../src/session-mesh.js";

const [root, sessionId] = process.argv.slice(2);
if (!root || !sessionId) {
  console.error("usage: session-mesh-peer.ts <meshRoot> <sessionId>");
  process.exit(1);
}

startParticipant({
  root,
  sessionId,
  cwd: "/work/peer",
  sessionName: "peer",
  isActive: () => true,
  deliver: (content, _messages, triggerTurn) => console.log(JSON.stringify({ content, triggerTurn })),
  // Long poll: delivery within the test's timeout proves the watcher works.
  pollMs: 60_000,
});
console.log("ready");
setInterval(() => undefined, 60_000);
