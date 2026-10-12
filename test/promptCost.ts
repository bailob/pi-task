/**
 * Prompt budget for the model-facing `task` surface.
 *
 * The `task` tool is paid on every turn, so this prints what each part costs
 * and the total. Run with `npm run cost` before and after a change to a tool
 * description, an agent description, or the parameter schema.
 *
 * It calls the same `buildTaskToolDescription` and `discoverAgents` the
 * extension uses, so the numbers follow the real surface rather than a copy.
 * Characters/4 is a rough estimate, not a tokenizer measurement. Schema JSON
 * is compact; skill metadata is included, but its body loads only on demand.
 */

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { formatSkillsForPrompt, loadSkillsFromDir } from "@earendil-works/pi-coding-agent";
import { fileURLToPath } from "node:url";

import { buildTaskToolDescription, discoverAgents, TASK_TOOL_DESCRIPTION } from "../src/helpers.js";
import { taskParametersSchema } from "../src/tool/schema.js";

const bundledAgentDir = join(dirname(fileURLToPath(import.meta.url)), "..", "agents");
const tokens = (text: string): number => Math.round(text.length / 4);

const { agents } = discoverAgents(process.cwd(), bundledAgentDir);
const description = buildTaskToolDescription(agents);
const roster = description.slice(TASK_TOOL_DESCRIPTION.length);
const schema = JSON.stringify(taskParametersSchema());
const skillPath = join(bundledAgentDir, "..", "resources", "task", "SKILL.md");
const { skills, diagnostics } = loadSkillsFromDir({ dir: dirname(skillPath), source: "pi-task" });
if (skills.length !== 1 || diagnostics.length > 0) throw new Error("Bundled task skill did not load cleanly");
// Use a stable npm-install path so checkout path length does not skew comparisons.
const listing = formatSkillsForPrompt([{ ...skills[0], filePath: "/home/user/.pi/agent/node_modules/@heyhuynhgiabuu/pi-task/resources/task/SKILL.md" }]);
const entry = listing.slice(listing.indexOf("  <skill>"), listing.indexOf("  </skill>") + "  </skill>".length) + "\n";

const rows: Array<[string, string]> = [
	["tool description (prose)", TASK_TOOL_DESCRIPTION],
	["agent roster", roster],
	["parameter schema", schema],
	["skill listing (incremental)", entry],
];

let total = 0;
for (const [label, text] of rows) {
	const cost = tokens(text);
	total += cost;
	console.log(`${label.padEnd(26)} ${String(text.length).padStart(6)} chars  ~${String(cost).padStart(5)} tok`);
}
console.log(`${"TOTAL".padEnd(26)} ${"".padStart(6)}         ~${String(total).padStart(5)} tok`);
console.log(`If this is the only skill, shared skills header adds ~${tokens(listing) - tokens(entry)} tok.`);
console.log(`Skill body (on demand, not in total): ~${tokens(readFileSync(skillPath, "utf8"))} tok.`);
console.log();

const parameterCosts = Object.entries(taskParametersSchema().properties)
	.map(([name, value]) => [name, tokens((value as { description?: string }).description ?? "")] as const)
	.sort((a, b) => b[1] - a[1]);
console.log("per-parameter description cost:");
for (const [name, cost] of parameterCosts) {
	console.log(`  ${name.padEnd(18)} ~${String(cost).padStart(4)} tok`);
}
