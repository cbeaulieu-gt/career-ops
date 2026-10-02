import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SOURCE = fs.readFileSync(path.join(ROOT, ".github", "scripts", "gfi-handoff.mjs"), "utf8");

test("gfi handoff assigns the coding agent before changing labels or promising a draft", () => {
  const assign = SOURCE.indexOf("/assignees`");
  const removeHumanLabel = SOURCE.indexOf("/labels/${encodeURIComponent('good first issue')}");
  const addAgentLabel = SOURCE.indexOf("{ labels: ['agent-candidate'] }");
  const promiseComment = SOURCE.indexOf("Nobody claimed this one in the newcomer window");

  assert.ok(assign >= 0, "assignment request must remain present");
  assert.ok(removeHumanLabel >= 0, "good first issue removal must remain present");
  assert.ok(addAgentLabel >= 0, "agent-candidate label addition must remain present");
  assert.ok(promiseComment >= 0, "handoff comment must remain present");
  assert.ok(assign < removeHumanLabel, "assignment must succeed before removing good first issue");
  assert.ok(assign < addAgentLabel, "assignment must succeed before adding agent-candidate");
  assert.ok(assign < promiseComment, "assignment must succeed before promising an agent draft");
});
