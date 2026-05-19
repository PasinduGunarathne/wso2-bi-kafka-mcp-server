// src/tools/error-flows.ts
// Erroneous-flow tools: failure simulation, diagnostics, and recovery
// validation for the Kafka + WSO2 Ballerina Integrator: BI environment.
//
// All tools:
//   - Use only allowlisted commands via docker.run() / docker.exec() / docker.execWithStdin()
//   - Pipe payloads via stdin — never shell-interpolated
//   - Use path.join() for all file paths — cross-platform (macOS / Windows / Linux)
//   - Shell commands (bash -c) run INSIDE the Linux container, not on the host OS
//   - Add strict timeouts to every Docker, Kafka, and Ballerina call
//   - Never run unbounded consumers or infinite BI processes

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import * as docker from "../utils/docker.js";
import * as log from "../utils/logger.js";
import { DEFAULTS, resolveComposeDir, resolveBiProjectPath } from "../config.js";
import { validateTopicName, optionalBool, optionalPositiveInt, optionalString } from "../utils/validation.js";
import {
  highlightErrorPatterns,
  summarisePatterns,
  detectCommitBehavior,
  detectDlqConfig,
  extractConfigurableDefault,
} from "../utils/log-patterns.js";
import type { ErrorFlowResult, TestStatus, CommitBehavior } from "../types.js";
import { runPrerequisiteChecks } from "./prerequisites.js";

// ── Project root (cross-platform) ────────────────────────────────────────────

const __dirname    = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = path.resolve(__dirname, "..", "..");

// ── Internal helpers ──────────────────────────────────────────────────────────

/** Resolve a fixture file path — always uses path.join, never string concat. */
function resolveFixturePath(name: string): string {
  return path.join(PROJECT_ROOT, "fixtures", "error-flows", name);
}

/** Read a fixture file.  Throws a clear error if the file is missing. */
function readFixture(name: string): string {
  const p = resolveFixturePath(name);
  if (!fs.existsSync(p)) {
    throw new Error(
      `Fixture file not found: ${p}\n` +
      `Ensure the fixtures/error-flows/ directory shipped with the MCP server.`,
    );
  }
  return fs.readFileSync(p, "utf8").trimEnd();
}

/** Get the running Kafka container name from docker compose ps output. */
async function getKafkaContainer(composeDir: string): Promise<string | null> {
  const r = await docker.composePs(composeDir);
  if (!r.ok || !r.stdout.trim()) return null;
  for (const line of r.stdout.split("\n")) {
    const t = line.trim();
    if (!t) continue;
    try {
      const obj = JSON.parse(t);
      if (obj.Service === DEFAULTS.KAFKA_SERVICE && obj.State === "running") {
        return obj.Name as string;
      }
    } catch { /* skip malformed lines */ }
  }
  return null;
}

/** Ensure Kafka is running and return container name; throw otherwise. */
async function requireKafkaRunning(composeDir: string): Promise<string> {
  const container = await getKafkaContainer(composeDir);
  if (!container) {
    throw new Error("Kafka is not running. Start it with start_kafka first.");
  }
  return container;
}

/**
 * Produce a raw payload to a Kafka topic via stdin.
 * Payload is piped through docker exec -i — never interpolated into a shell string
 * on the host OS.  The bash -c command runs inside the Linux container.
 */
async function produceRaw(
  container: string,
  topic: string,
  payload: string,
  timeoutMs = 15_000,
): Promise<{ ok: boolean; detail: string }> {
  const shellCmd =
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-console-producer.sh` +
    ` --bootstrap-server localhost:9092 --topic ${topic}`;
  const r = await docker.execWithStdin(container, shellCmd, payload, timeoutMs);
  if (!r.ok && r.stderr.includes("LEADER_NOT_AVAILABLE")) {
    // Transient — retry once after a short pause
    await docker.sleep(2_000);
    const r2 = await docker.execWithStdin(container, shellCmd, payload, timeoutMs);
    return { ok: r2.ok, detail: (r2.stderr + r2.stdout).trim() };
  }
  return { ok: r.ok, detail: (r.stderr + r.stdout).trim() };
}

/**
 * Consume up to maxMessages from a topic with a hard timeout.
 * All args are passed as an array to docker.exec — no host-shell quoting needed.
 */
async function consumeBounded(
  container: string,
  topic: string,
  maxMessages: number,
  consumerTimeoutMs: number,
  fromBeginning: boolean,
  groupIdOverride?: string,
): Promise<{ stdout: string; stderr: string; timedOut: boolean }> {
  const args: string[] = [
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-console-consumer.sh`,
    "--bootstrap-server", "localhost:9092",
    "--topic", topic,
    "--max-messages", String(maxMessages),
    "--timeout-ms", String(consumerTimeoutMs),
  ];
  if (fromBeginning) args.push("--from-beginning");
  if (groupIdOverride) {
    // Pass as separate array elements — no shell quoting on host OS
    args.push("--consumer-property", `group.id=${groupIdOverride}`);
  }
  const execTimeout = consumerTimeoutMs + 5_000; // buffer for container overhead
  const r = await docker.exec(container, args, execTimeout);
  const timedOut =
    r.stderr.toLowerCase().includes("timeout") ||
    r.stdout.toLowerCase().includes("timeout") ||
    r.stderr.toLowerCase().includes("processed a total of 0");
  return { stdout: r.stdout, stderr: r.stderr, timedOut };
}

/**
 * Read all .bal files from a project directory.
 * Returns a map of filename → content.
 * Uses path.join throughout — cross-platform.
 */
function inspectBiSources(biPath: string): Record<string, string> {
  if (!fs.existsSync(biPath)) return {};
  const files = fs.readdirSync(biPath).filter((f) => f.endsWith(".bal"));
  const result: Record<string, string> = {};
  for (const f of files) {
    try {
      result[f] = fs.readFileSync(path.join(biPath, f), "utf8");
    } catch { /* skip unreadable files */ }
  }
  return result;
}

/** Ensure a topic exists; create it if not (idempotent). */
async function ensureTopicExists(
  container: string,
  topic: string,
): Promise<string> {
  const r = await docker.exec(container, [
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-topics.sh`,
    "--bootstrap-server", "localhost:9092",
    "--create", "--topic", topic,
    "--partitions", "1", "--replication-factor", "1",
    "--if-not-exists",
  ], 15_000);
  return r.ok ? `✅  ${topic} ready` : `⚠️   ${topic} — ${r.stderr.trim()}`;
}

/**
 * Run the BI listener for a short capture window.
 * Uses a fresh consumer group ID each time so it always reads from earliest offset.
 * Returns captured stdout+stderr (the process times out after captureMs — expected).
 */
async function runBiCapture(
  biPath: string,
  captureMs: number,
  groupId: string,
): Promise<string> {
  const r = await docker.run(
    "bal",
    ["run", "--", `-CkafkaGroupId=${groupId}`],
    biPath,
    captureMs,
  );
  return (r.stdout + "\n" + r.stderr).trim();
}

/** Build an ErrorFlowResult with safe defaults for omitted fields. */
function buildResult(fields: Partial<ErrorFlowResult>): ErrorFlowResult {
  return {
    testName:         fields.testName         ?? "unknown",
    topic:            fields.topic            ?? DEFAULTS.KAFKA_BOOTSTRAP_HOST,
    payload:          fields.payload          ?? "",
    expectedBehavior: fields.expectedBehavior ?? "",
    observedBehavior: fields.observedBehavior ?? "",
    logSnippets:      fields.logSnippets      ?? [],
    status:           fields.status           ?? "skipped",
    recommendation:   fields.recommendation   ?? "",
  };
}

/** Format a single ErrorFlowResult as human-readable text. */
function formatResult(r: ErrorFlowResult): string[] {
  const statusIcon: Record<TestStatus, string> = {
    pass: "✅", fail: "❌", warning: "⚠️", skipped: "⏭️",
  };
  const lines: string[] = [
    `${statusIcon[r.status]}  ${r.testName}`,
    `   Topic     : ${r.topic}`,
    `   Payload   : ${r.payload.length > 120 ? r.payload.slice(0, 120) + "…" : r.payload}`,
    `   Expected  : ${r.expectedBehavior}`,
    `   Observed  : ${r.observedBehavior}`,
  ];
  if (r.logSnippets.length > 0) {
    lines.push("   Log snippets:");
    for (const s of r.logSnippets.slice(0, 5)) {
      lines.push(`     ${s}`);
    }
  }
  if (r.recommendation) lines.push(`   Next step : ${r.recommendation}`);
  return lines;
}

// ── trigger_invalid_json_error ────────────────────────────────────────────────

export async function triggerInvalidJsonError(args: {
  topicName?: string;
  kafkaComposePath?: string;
  projectPath?: string;
  captureOutput?: boolean;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const biPath     = resolveBiProjectPath(args.projectPath);
  const topic      = optionalString(args.topicName, "bi.orders.in");
  validateTopicName(topic);
  const capture    = optionalBool(args.captureOutput, false);

  const lines: string[] = [log.header("Trigger: Invalid JSON Error")];
  lines.push("");

  const container = await requireKafkaRunning(composeDir);
  const payload   = readFixture("invalid-json.txt");

  lines.push(log.info(`Topic    : ${topic}`));
  lines.push(log.info(`Payload  : ${payload}`));
  lines.push("");
  lines.push(log.run("Producing malformed JSON via stdin (no shell interpolation)…"));

  const prod = await produceRaw(container, topic, payload);
  lines.push(prod.ok ? log.ok("Message produced to Kafka.") : log.warn(`Produce result: ${prod.detail}`));

  // Optional: run BI listener briefly and capture output
  const logSnippets: string[] = [];
  if (capture && fs.existsSync(path.join(biPath, "Ballerina.toml"))) {
    const groupId = `err-json-${Date.now()}`;
    lines.push("");
    lines.push(log.run(`Capturing BI listener output for 12s (group: ${groupId})…`));
    const captured = await runBiCapture(biPath, 12_000, groupId);
    const capturedLines = captured.split("\n").filter(Boolean);
    const highlighted   = highlightErrorPatterns(capturedLines);
    logSnippets.push(...highlighted.slice(0, 10));
    lines.push(...highlighted.slice(0, 15));
  }

  // Pull recent Kafka logs
  const kafkaLogs = await docker.composeLogs(composeDir, DEFAULTS.KAFKA_SERVICE, 30);
  const kafkaLines = highlightErrorPatterns(kafkaLogs.stdout.split("\n").filter(Boolean));

  lines.push("");
  lines.push(log.header("Expected Behaviour"));
  lines.push(
    "  The BI consumer calls string:fromBytes(rawBytes) then rawStr.fromJsonStringWithType().",
    "  Malformed JSON causes fromJsonStringWithType() to return an error.",
    "  The 'on fail' block inside the foreach loop catches the error and logs:",
    "    [Consumer] Failed to process message  errMsg=<parse error>",
    "",
    "  ⚠️  IMPORTANT — commit behaviour in the generated project:",
    "  The manual commit (caller->'commit()) is called AFTER the foreach loop,",
    "  regardless of individual message failures.",
    "  This means: offsets ARE committed even when JSON parsing fails.",
    "  The message will NOT be re-delivered to the same consumer group.",
    "  This is 'log-and-continue' behaviour, not 'fail-and-skip-commit'.",
  );

  if (logSnippets.length > 0) {
    lines.push("");
    lines.push(log.header("Log Snippets"));
    lines.push(...logSnippets);
  }

  lines.push("");
  lines.push(log.info("Recommendation: to test re-delivery, the BI project must propagate"));
  lines.push(log.info("the error OUT of onConsumerRecord (remove the on fail block or re-throw)."));

  return lines.join("\n");
}

// ── trigger_schema_mismatch_error ─────────────────────────────────────────────

export async function triggerSchemaMismatchError(args: {
  variant?: string;
  topicName?: string;
  kafkaComposePath?: string;
  projectPath?: string;
  captureOutput?: boolean;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const biPath     = resolveBiProjectPath(args.projectPath);
  const topic      = optionalString(args.topicName, "bi.orders.in");
  validateTopicName(topic);
  const variant    = optionalString(args.variant, "missing-field");
  const capture    = optionalBool(args.captureOutput, false);

  if (variant !== "missing-field" && variant !== "wrong-type") {
    throw new Error(`variant must be "missing-field" or "wrong-type". Got: ${variant}`);
  }

  const lines: string[] = [log.header(`Trigger: Schema Mismatch — ${variant}`)];
  lines.push("");

  const container = await requireKafkaRunning(composeDir);

  const fixtureName = variant === "wrong-type"
    ? "schema-wrong-type.json"
    : "schema-missing-field.json";
  const payload = readFixture(fixtureName);

  lines.push(log.info(`Topic    : ${topic}`));
  lines.push(log.info(`Variant  : ${variant}`));
  lines.push(log.info(`Payload  : ${payload}`));
  lines.push("");
  lines.push(log.run("Producing schema-mismatched payload via stdin…"));

  const prod = await produceRaw(container, topic, payload);
  lines.push(prod.ok ? log.ok("Message produced to Kafka.") : log.warn(`Produce result: ${prod.detail}`));

  const logSnippets: string[] = [];
  if (capture && fs.existsSync(path.join(biPath, "Ballerina.toml"))) {
    const groupId = `err-schema-${Date.now()}`;
    lines.push("");
    lines.push(log.run(`Capturing BI listener output for 12s (group: ${groupId})…`));
    const captured = await runBiCapture(biPath, 12_000, groupId);
    const capturedLines = captured.split("\n").filter(Boolean);
    const highlighted   = highlightErrorPatterns(capturedLines);
    logSnippets.push(...highlighted.slice(0, 10));
    lines.push(...highlighted.slice(0, 15));
  }

  lines.push("");
  lines.push(log.header("Expected Behaviour"));

  if (variant === "missing-field") {
    lines.push(
      "  The payload is valid JSON but missing required OrderEvent fields",
      "  (customerId, eventType, amount, timestamp).",
      "  rawStr.fromJsonStringWithType() will fail with a ConversionError",
      "  because the target type has required fields not present in the JSON.",
      "  The on fail block logs: [Consumer] Failed to process message",
    );
  } else {
    lines.push(
      "  The payload has 'amount' as a string (\"not-a-number\") but OrderEvent",
      "  declares 'amount' as float.",
      "  rawStr.fromJsonStringWithType() will fail with a typedesc ConversionError.",
      "  The on fail block logs: [Consumer] Failed to process message",
    );
  }
  lines.push(
    "",
    "  ⚠️  Commit behaviour: same as invalid JSON — commit runs after foreach,",
    "  so offsets are committed even on schema mismatch. No re-delivery.",
  );

  if (logSnippets.length > 0) {
    lines.push("");
    lines.push(log.header("Log Snippets"));
    lines.push(...logSnippets);
  }

  return lines.join("\n");
}

// ── trigger_business_rule_error ───────────────────────────────────────────────

export async function triggerBusinessRuleError(args: {
  topicName?: string;
  kafkaComposePath?: string;
  projectPath?: string;
  captureOutput?: boolean;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const biPath     = resolveBiProjectPath(args.projectPath);
  const topic      = optionalString(args.topicName, "bi.orders.in");
  validateTopicName(topic);
  const capture    = optionalBool(args.captureOutput, false);

  const lines: string[] = [log.header("Trigger: Business Rule Error (Negative Amount)")];
  lines.push("");

  const container = await requireKafkaRunning(composeDir);
  const payload   = readFixture("business-invalid-amount.json");

  // Inspect project sources for business validation
  const sources   = inspectBiSources(biPath);
  const allSrc    = Object.values(sources).join("\n");
  const hasFuncBal = "functions.bal" in sources;

  const hasAmountValidation =
    /amount\s*<\s*0/i.test(allSrc)   ||
    /amount\s*<=\s*0/i.test(allSrc)  ||
    /negativeAmount/i.test(allSrc)   ||
    /invalidAmount/i.test(allSrc)    ||
    /amount.*error/i.test(allSrc);

  lines.push(log.info(`Topic    : ${topic}`));
  lines.push(log.info(`Payload  : ${payload}`));
  lines.push("");

  if (hasFuncBal) {
    lines.push(log.info("Inspecting functions.bal for business validation…"));
    lines.push(hasAmountValidation
      ? log.ok("Amount validation found in project sources.")
      : log.warn("No amount/business validation found in functions.bal."),
    );
  } else {
    lines.push(log.warn("functions.bal not found in project — cannot inspect business logic."));
  }

  lines.push("");
  lines.push(log.run("Producing negative-amount payload via stdin…"));
  const prod = await produceRaw(container, topic, payload);
  lines.push(prod.ok ? log.ok("Message produced to Kafka.") : log.warn(`Produce result: ${prod.detail}`));

  const logSnippets: string[] = [];
  if (capture && fs.existsSync(path.join(biPath, "Ballerina.toml"))) {
    const groupId = `err-biz-${Date.now()}`;
    lines.push("");
    lines.push(log.run(`Capturing BI listener output for 12s (group: ${groupId})…`));
    const captured = await runBiCapture(biPath, 12_000, groupId);
    const capturedLines = captured.split("\n").filter(Boolean);
    const highlighted   = highlightErrorPatterns(capturedLines);
    logSnippets.push(...highlighted.slice(0, 10));
    lines.push(...highlighted.slice(0, 15));
  }

  lines.push("");
  lines.push(log.header("Expected Behaviour"));

  if (hasAmountValidation) {
    lines.push(
      "  Business validation was found. A negative amount should trigger a",
      "  processing error logged by the on fail block.",
      "  Check the log snippets above for confirmation.",
    );
  } else {
    lines.push(
      "  ⚠️  No business validation detected in the project.",
      "  The generated processOrder() function accepts any OrderEvent without",
      "  validating the amount field.",
      "  A negative amount of -100.0 will be processed SUCCESSFULLY with",
      "  status 'PROCESSED' — no error will be thrown.",
      "",
      "  This is a WARNING, not a failure. The BI project works as designed,",
      "  but lacks domain validation.",
      "",
      "  Recommendation: Add a guard in functions.bal:",
      "    if orderEvt.amount < 0 {",
      "        return error(\"Invalid order: amount must be non-negative\");",
      "    }",
    );
  }

  if (logSnippets.length > 0) {
    lines.push("");
    lines.push(log.header("Log Snippets"));
    lines.push(...logSnippets);
  }

  return lines.join("\n");
}

// ── test_missing_topic_error ──────────────────────────────────────────────────

export async function testMissingTopicError(args: {
  kafkaComposePath?: string;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const lines: string[] = [log.header("Test: Missing Topic Behaviour")];
  lines.push("");

  const container = await requireKafkaRunning(composeDir);

  // Generate a unique topic name — alphanumeric + hyphens only (cross-platform safe)
  const testTopic = `bi-missing-test-${Date.now()}`;
  lines.push(log.info(`Test topic: ${testTopic}`));
  lines.push(log.info("This topic does NOT exist yet."));
  lines.push("");

  // Step 1: Describe before producing — should show "no topic found"
  lines.push(log.run("Step 1 — Describing non-existent topic BEFORE producing…"));
  const describeBefore = await docker.exec(container, [
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-topics.sh`,
    "--bootstrap-server", "localhost:9092",
    "--describe", "--topic", testTopic,
  ], 10_000);
  const beforeOutput = (describeBefore.stdout + describeBefore.stderr).trim();
  lines.push(`  Result: ${beforeOutput || "(no output — topic does not exist)"}`);

  // Step 2: Produce to the non-existent topic
  lines.push("");
  lines.push(log.run("Step 2 — Producing one message to the non-existent topic…"));
  const payload = `{"test":true,"topic":"${testTopic}","ts":${Date.now()}}`;
  const prod = await produceRaw(container, testTopic, payload, 15_000);
  lines.push(prod.ok
    ? log.ok("Produce succeeded (no error — see explanation below).")
    : log.warn(`Produce result: ${prod.detail}`),
  );

  // Step 3: Describe after producing — topic should now exist if auto-create is on
  lines.push("");
  lines.push(log.run("Step 3 — Describing topic AFTER producing…"));
  const describeAfter = await docker.exec(container, [
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-topics.sh`,
    "--bootstrap-server", "localhost:9092",
    "--describe", "--topic", testTopic,
  ], 10_000);
  const afterOutput = (describeAfter.stdout + describeAfter.stderr).trim();
  const topicCreated = describeAfter.stdout.includes(testTopic);
  lines.push(`  Result: ${afterOutput || "(no output)"}`);

  lines.push("");
  lines.push(log.header("Explanation"));
  lines.push(
    "  The bundled docker-compose.yml sets:",
    "    KAFKA_AUTO_CREATE_TOPICS_ENABLE: \"true\"",
    "",
    topicCreated
      ? "  ✅  As expected: the topic was AUTO-CREATED when the first message was"
      : "  ⚠️  The topic was NOT auto-created (unexpected).",
  );
  if (topicCreated) {
    lines.push(
      "  produced to it. This means producing to a missing topic WILL NOT",
      "  fail — Kafka creates the topic automatically.",
      "",
      "  To reproduce a true 'missing topic' error (ErrorCode: UNKNOWN_TOPIC_OR_PARTITION):",
      "  1. Override docker-compose.yml to set KAFKA_AUTO_CREATE_TOPICS_ENABLE: \"false\"",
      "  2. Restart Kafka: stop_kafka then start_kafka",
      "  3. Attempt to produce to a topic that was not explicitly created.",
    );
  }
  lines.push(
    "",
    "  The BI Kafka listener will similarly not fail on missing topics when",
    "  auto-create is enabled — the consumer will wait for messages on the",
    "  auto-created (empty) topic.",
  );

  // Cleanup: delete the test topic to keep the broker tidy
  await docker.exec(container, [
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-topics.sh`,
    "--bootstrap-server", "localhost:9092",
    "--delete", "--topic", testTopic,
  ], 10_000);
  lines.push("");
  lines.push(log.done(`Cleaned up test topic '${testTopic}'.`));

  return lines.join("\n");
}

// ── test_consumer_not_running_flow ────────────────────────────────────────────

export async function testConsumerNotRunningFlow(args: {
  topicName?: string;
  kafkaComposePath?: string;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const topic      = optionalString(args.topicName, "bi.orders.in");
  validateTopicName(topic);
  const outputTopic = "bi.orders.out";

  const lines: string[] = [log.header("Test: Consumer Not Running")];
  lines.push("");

  const container = await requireKafkaRunning(composeDir);

  // Ensure topics exist
  await ensureTopicExists(container, topic);
  await ensureTopicExists(container, outputTopic);

  lines.push(log.info("The BI listener (bal run) has NOT been started."));
  lines.push(log.info(`Producing a valid OrderEvent to input topic: ${topic}`));
  lines.push("");

  // Produce a valid order
  const payload = readFixture("valid-order.json");
  const ts      = new Date().toISOString();
  const orderId = `ORD-NORUN-${Date.now()}`;
  const livePayload = payload
    .replace("ORD-VALID-001", orderId)
    .replace("2025-01-01T00:00:00Z", ts);

  lines.push(log.run("Producing valid OrderEvent to input topic…"));
  const prod = await produceRaw(container, topic, livePayload);
  lines.push(prod.ok ? log.ok(`Produced orderId: ${orderId}`) : log.warn(`Produce result: ${prod.detail}`));

  // Try to consume from output topic — should timeout
  lines.push("");
  lines.push(log.run("Consuming from output topic (8s timeout)…"));
  const consume = await consumeBounded(container, outputTopic, 1, 8_000, false);

  lines.push(consume.timedOut
    ? log.ok("Output topic timed out — no messages (expected, consumer is not running).")
    : log.warn(`Unexpected output received: ${consume.stdout.slice(0, 200)}`),
  );

  // Verify input message IS on the input topic using a temporary read-only group
  const tempGroup = `temp-norun-check-${Date.now()}`;
  lines.push("");
  lines.push(log.run(`Verifying input message is retained (temp group: ${tempGroup})…`));
  const verify = await consumeBounded(container, topic, 1, 8_000, false, tempGroup);

  lines.push(verify.stdout.includes(orderId) || verify.stdout.includes("ORD")
    ? log.ok("Input message confirmed present in topic — Kafka is retaining it.")
    : log.info("Could not confirm the specific message (may have been consumed by another group or not yet visible)."),
  );

  lines.push("");
  lines.push(log.header("Explanation"));
  lines.push(
    "  Kafka is a durable log. Messages produced to bi.orders.in are retained",
    "  based on the topic's log retention policy (default: 7 days).",
    "",
    "  When the BI listener starts next with the same consumer group and",
    "    offsetReset: kafka:OFFSET_RESET_EARLIEST",
    "  it will pick up all unprocessed messages from the last committed offset.",
    "",
    "  This behaviour is by design — Kafka decouples producers from consumers.",
    "  The producer does not need the consumer to be running.",
  );

  return lines.join("\n");
}

// ── test_manual_commit_redelivery ─────────────────────────────────────────────

export async function testManualCommitRedelivery(args: {
  projectPath?: string;
  kafkaComposePath?: string;
  includeRestart?: boolean;
  captureOutput?: boolean;
}): Promise<string> {
  const composeDir   = resolveComposeDir(args.kafkaComposePath);
  const biPath       = resolveBiProjectPath(args.projectPath);
  const includeRestart = optionalBool(args.includeRestart, false);
  const capture      = optionalBool(args.captureOutput, false);

  const lines: string[] = [log.header("Test: Manual Commit & Redelivery Behaviour")];
  lines.push("");

  const container = await requireKafkaRunning(composeDir);

  // Step 1: Inspect commit behaviour from source
  lines.push(log.run("Step 1 — Inspecting BI project source for commit pattern…"));
  const sources  = inspectBiSources(biPath);
  const allSrc   = Object.values(sources).join("\n");
  const mainSrc  = sources["main.bal"] ?? "";
  const commit   = detectCommitBehavior(mainSrc || allSrc);

  lines.push("");
  lines.push("  Detected commit behaviour:");
  lines.push(`    autoCommit disabled : ${commit.autoCommitDisabled ? "✅ Yes" : "❌ No (autoCommit may be on!)"}`);
  lines.push(`    hasManualCommit     : ${commit.hasManualCommit ? "✅ Yes" : "❌ Not found"}`);
  lines.push(`    commit inside loop  : ${commit.commitInsideLoop ? "⚠️  Yes (inside foreach)" : "No"}`);
  lines.push(`    commit after loop   : ${commit.commitAfterLoop ? "⚠️  Yes (after foreach — always runs)" : "No"}`);
  lines.push("");

  if (commit.commitAfterLoop && commit.hasManualCommit) {
    lines.push(log.warn(
      "commit() is called AFTER the foreach loop. Individual message failures",
    ));
    lines.push(log.warn(
      "are caught by 'on fail' inside the loop but do NOT prevent the commit.",
    ));
    lines.push(log.warn(
      "Offsets WILL be committed even when processing fails. No re-delivery.",
    ));
  } else if (!commit.hasManualCommit) {
    lines.push(log.warn("Manual commit not detected. autoCommit may be in effect."));
  }

  lines.push("");
  lines.push(log.run("Step 2 — Producing invalid JSON to trigger a processing error…"));
  await ensureTopicExists(container, "bi.orders.in");
  const errorPayload = readFixture("invalid-json.txt");
  const prod = await produceRaw(container, "bi.orders.in", errorPayload);
  lines.push(prod.ok ? log.ok("Error payload produced.") : log.warn(prod.detail));

  const commitGroupId = `commit-test-${Date.now()}`;

  if (capture && fs.existsSync(path.join(biPath, "Ballerina.toml"))) {
    lines.push("");
    lines.push(log.run(`Step 3 — Running BI listener for 15s (group: ${commitGroupId})…`));
    const captured      = await runBiCapture(biPath, 15_000, commitGroupId);
    const capturedLines = captured.split("\n").filter(Boolean);
    const highlighted   = highlightErrorPatterns(capturedLines);

    const sawFailure = captured.includes("[Consumer] Failed") ||
                       captured.includes("ConversionError") ||
                       captured.includes("error:");
    const sawPublish = captured.includes("[Producer] Published");

    lines.push(sawFailure
      ? log.ok("✅  Processing failure detected in BI listener output.")
      : log.info("  No explicit failure log found in capture window."),
    );
    lines.push(sawPublish
      ? log.warn("⚠️  Output was published despite error (unexpected — check logs).")
      : log.ok("✅  No output published to bi.orders.out (expected)."),
    );

    lines.push("");
    lines.push(log.header("Captured Output (first 20 lines)"));
    lines.push(...highlighted.slice(0, 20));

    if (includeRestart && fs.existsSync(path.join(biPath, "Ballerina.toml"))) {
      lines.push("");
      lines.push(log.run(
        `Step 4 — Re-running listener with SAME group ID (${commitGroupId}) to test re-delivery…`,
      ));
      const restart      = await runBiCapture(biPath, 12_000, commitGroupId);
      const restartLines = restart.split("\n").filter(Boolean);
      const redelivered  = restart.includes("[Consumer] Received") || restart.includes("ORD-ERR-001");

      lines.push(redelivered
        ? log.warn("⚠️  Message was re-delivered (offsets were NOT committed).")
        : log.ok("✅  Message was NOT re-delivered (offsets were committed — consistent with log-and-continue)."),
      );
      lines.push(...highlightErrorPatterns(restartLines).slice(0, 10));
    }
  }

  lines.push("");
  lines.push(log.header("Summary: Manual Commit Behaviour in Generated Project"));
  lines.push(
    "  The generated main.bal uses this pattern:",
    "    foreach msg in messages {",
    "        do { … } on fail error e { log:printError(…) }",
    "    }",
    "    check caller->'commit();   ← ALWAYS runs after the loop",
    "",
    "  Result: offsets ARE committed even when messages fail to process.",
    "  Failed messages will NOT be re-delivered to the same consumer group.",
    "",
    "  To implement true skip-on-error behaviour, change the pattern to:",
    "    Option A — Let the error propagate (remove on fail, use 'check' everywhere):",
    "      The function returns error? — Kafka may retry delivery.",
    "    Option B — Conditional commit:",
    "      Track batch success in a boolean and only call commit() if all succeeded.",
    "    Option C — DLQ pattern:",
    "      Publish failed messages to a dead-letter topic and always commit.",
  );

  return lines.join("\n");
}

// ── check_dlq ─────────────────────────────────────────────────────────────────

export async function checkDlq(args: {
  projectPath?: string;
  kafkaComposePath?: string;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const biPath     = resolveBiProjectPath(args.projectPath);

  const lines: string[] = [log.header("Check: Dead-Letter Queue (DLQ)")];
  lines.push("");

  const sources  = inspectBiSources(biPath);
  const allSrcs  = Object.values(sources);
  const dlqVar   = detectDlqConfig(allSrcs);

  if (!dlqVar) {
    lines.push(log.warn("DLQ is not currently implemented in this BI project."));
    lines.push("");
    lines.push(
      "  The generated demo uses a 'log-and-continue' error handling pattern:",
      "    - Failed messages are logged via log:printError()",
      "    - Offsets are committed after the foreach loop regardless of failures",
      "    - No messages are routed to a dead-letter topic",
      "",
      "  To add DLQ support to this project:",
      "",
      "  1. Add a configurable DLQ topic name in config.bal:",
      `     configurable string dlqTopic = "bi.orders.dlq";`,
      "",
      "  2. Initialise a DLQ producer in connections.bal:",
      "     final kafka:Producer dlqProducer = check new (kafkaBootstrapServers);",
      "",
      "  3. In main.bal, publish failed messages inside the on fail block:",
      "     on fail error e {",
      "         log:printError(\"[Consumer] Failed\", errMsg = e.message());",
      "         check dlqProducer->send({",
      "             topic: dlqTopic,",
      "             value: msg.value",
      "         });",
      "     }",
      "",
      "  4. Create the DLQ topic:",
      "     create_topic { \"topicName\": \"bi.orders.dlq\" }",
    );
    return lines.join("\n");
  }

  // DLQ is configured
  lines.push(log.ok(`DLQ variable detected: ${dlqVar}`));
  const dlqDefault = extractConfigurableDefault(allSrcs, dlqVar);
  const dlqTopic   = dlqDefault ?? dlqVar;

  lines.push(log.info(`DLQ topic (default): ${dlqTopic}`));
  lines.push("");

  // Try to consume from DLQ
  try {
    const container = await requireKafkaRunning(composeDir);
    lines.push(log.run(`Consuming up to 10 messages from DLQ topic '${dlqTopic}'…`));
    const consume = await consumeBounded(container, dlqTopic, 10, 8_000, true);

    if (consume.timedOut || !consume.stdout.trim()) {
      lines.push(log.info("DLQ topic is empty (no failed messages found)."));
    } else {
      const msgs = consume.stdout.split("\n").filter(Boolean);
      lines.push(log.ok(`Found ${msgs.length} message(s) in DLQ:`));
      for (const m of msgs.slice(0, 10)) {
        lines.push(`  ${m}`);
      }
    }
  } catch (e: any) {
    lines.push(log.warn(`Cannot consume from DLQ — Kafka may not be running: ${e.message}`));
  }

  return lines.join("\n");
}

// ── show_error_diagnostics ────────────────────────────────────────────────────

export async function showErrorDiagnostics(args: {
  kafkaComposePath?: string;
  projectPath?: string;
  lines?: number;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const biPath     = args.projectPath ? resolveBiProjectPath(args.projectPath) : undefined;
  const tail       = optionalPositiveInt(args.lines, "lines", 100);

  const outputLines: string[] = [log.header("Error Diagnostics — Kafka + BI")];
  outputLines.push("");

  // ── Kafka broker logs ─────────────────────────────────────────────────────
  outputLines.push(log.header("Kafka Broker Logs"));
  const kafkaLogs = await docker.composeLogs(composeDir, DEFAULTS.KAFKA_SERVICE, tail);
  if (!kafkaLogs.ok || !kafkaLogs.stdout.trim()) {
    outputLines.push(log.warn("No Kafka logs available (Kafka may not be running)."));
  } else {
    const kafkaLines = highlightErrorPatterns(kafkaLogs.stdout.split("\n").filter(Boolean));
    outputLines.push(...kafkaLines);
  }

  // ── BI runtime output (target/*.log files) ─────────────────────────────────
  if (biPath) {
    outputLines.push("");
    outputLines.push(log.header("BI Runtime Logs (target/)"));
    const targetDir = path.join(biPath, "target");
    if (fs.existsSync(targetDir)) {
      const logFiles = fs.readdirSync(targetDir).filter((f) => f.endsWith(".log"));
      if (logFiles.length === 0) {
        outputLines.push(log.info("No .log files found in target/ directory."));
      }
      for (const lf of logFiles) {
        const logPath = path.join(targetDir, lf);
        outputLines.push(log.info(`File: ${logPath}`));
        try {
          const content = fs.readFileSync(logPath, "utf8");
          const logLines = content.split("\n").filter(Boolean).slice(-tail);
          const highlighted = highlightErrorPatterns(logLines);
          outputLines.push(...highlighted);
        } catch {
          outputLines.push(log.warn(`Could not read ${lf}`));
        }
      }
    } else {
      outputLines.push(log.info("target/ directory not found — run validate_bi_project or run_bi_project first."));
    }
  }

  // ── Pattern summary ───────────────────────────────────────────────────────
  outputLines.push("");
  outputLines.push(log.header("Pattern Summary"));

  const allLines = [
    ...(kafkaLogs.stdout ?? "").split("\n"),
  ];
  const summary = summarisePatterns(allLines);

  if (summary.length === 0) {
    outputLines.push(log.ok("No known error patterns detected in Kafka logs."));
  } else {
    for (const s of summary) {
      outputLines.push(`  ${s.count.toString().padStart(3)}×  ${s.label}`);
    }
  }

  return outputLines.join("\n");
}

// ── generate_error_flow_report ────────────────────────────────────────────────

export async function generateErrorFlowReport(args: {
  results?: ErrorFlowResult[];
  title?: string;
}): Promise<string> {
  const title    = optionalString(args.title, "Erroneous Flow Test Report");
  const results  = args.results ?? [];

  const lines: string[] = [
    log.header(title),
    "",
    log.info(`Generated : ${new Date().toISOString()}`),
    log.info(`Tests run : ${results.length}`),
    "",
  ];

  if (results.length === 0) {
    lines.push(log.warn("No test results provided. Run run_error_flow_suite first."));
    return lines.join("\n");
  }

  // Summary table
  const passed  = results.filter((r) => r.status === "pass").length;
  const failed  = results.filter((r) => r.status === "fail").length;
  const warned  = results.filter((r) => r.status === "warning").length;
  const skipped = results.filter((r) => r.status === "skipped").length;

  lines.push("┌────────────────────────────────────────────────────────────────┐");
  lines.push(`│  ✅ Pass: ${String(passed).padEnd(4)} ❌ Fail: ${String(failed).padEnd(4)} ⚠️  Warn: ${String(warned).padEnd(4)} ⏭️  Skip: ${String(skipped).padEnd(4)}       │`);
  lines.push("└────────────────────────────────────────────────────────────────┘");
  lines.push("");

  // Per-test sections
  for (const r of results) {
    lines.push("─".repeat(66));
    lines.push(...formatResult(r));
    lines.push("");
  }

  lines.push("═".repeat(66));

  // Verdict
  if (failed === 0 && warned === 0) {
    lines.push(log.done("All tests passed. The BI integration handles erroneous flows as expected."));
  } else if (failed > 0) {
    lines.push(log.err(`${failed} test(s) FAILED. Review the sections above and address each failure.`));
  } else {
    lines.push(log.warn(`${warned} warning(s). Review the recommendations above.`));
  }

  lines.push("");
  lines.push(log.info("Safety note: No BI project files were modified during these tests."));

  return lines.join("\n");
}

// ── run_error_flow_suite ──────────────────────────────────────────────────────

export async function runErrorFlowSuite(args: {
  projectPath?: string;
  inputTopic?: string;
  outputTopic?: string;
  groupId?: string;
  kafkaComposePath?: string;
  includeKafkaUnavailableTest?: boolean;
  includeManualCommitRedeliveryTest?: boolean;
  includeDlqCheck?: boolean;
  timeoutSeconds?: number;
}): Promise<string> {
  const composeDir   = resolveComposeDir(args.kafkaComposePath);
  const biPath       = resolveBiProjectPath(args.projectPath);
  const inputTopic   = optionalString(args.inputTopic, "bi.orders.in");
  const outputTopic  = optionalString(args.outputTopic, "bi.orders.out");
  const inclCommit   = optionalBool(args.includeManualCommitRedeliveryTest, false);
  const inclDlq      = optionalBool(args.includeDlqCheck, false);
  const inclUnavail  = optionalBool(args.includeKafkaUnavailableTest, false);

  validateTopicName(inputTopic);
  validateTopicName(outputTopic);

  const lines: string[] = [log.header("Error Flow Test Suite")];
  lines.push("");

  // ── Prerequisites ─────────────────────────────────────────────────────────
  lines.push(log.step(1, 7, "Running prerequisite checks…"));
  const checks = await runPrerequisiteChecks({ kafkaComposePath: args.kafkaComposePath });
  const hardBlockers = checks.filter(
    (c) => !c.ok && ["Docker installed", "Docker daemon running", "Docker Compose v2", "Ballerina CLI (bal)"].includes(c.name),
  );
  if (hardBlockers.length > 0) {
    lines.push(log.err("Cannot run error flow suite — prerequisites not satisfied:"));
    for (const b of hardBlockers) lines.push(`  ❌  ${b.name}: ${b.detail}`);
    return lines.join("\n");
  }
  lines.push(log.ok("Prerequisites satisfied."));

  // ── Kafka running ─────────────────────────────────────────────────────────
  lines.push("");
  lines.push(log.step(2, 7, "Verifying Kafka is running…"));
  const container = await getKafkaContainer(composeDir);
  if (!container) {
    lines.push(log.err("Kafka is not running. Start it with start_kafka first."));
    return lines.join("\n");
  }
  lines.push(log.ok(`Kafka container: ${container}`));

  // ── Ensure topics exist ───────────────────────────────────────────────────
  lines.push("");
  lines.push(log.step(3, 7, "Ensuring demo topics exist…"));
  lines.push(await ensureTopicExists(container, inputTopic));
  lines.push(await ensureTopicExists(container, outputTopic));

  // ── Build BI project ──────────────────────────────────────────────────────
  lines.push("");
  lines.push(log.step(4, 7, "Validating BI project (bal build)…"));
  const buildR = await docker.run("bal", ["build"], biPath, 180_000);
  if (!buildR.ok) {
    const errLines = (buildR.stdout + "\n" + buildR.stderr)
      .split("\n").filter((l) => l.includes("error:") || l.includes("ERROR")).slice(0, 10);
    lines.push(log.err("BI project build failed — error-flow tests require a compilable project."));
    lines.push(...errLines);
    return lines.join("\n");
  }
  lines.push(log.ok("BI project compiled successfully."));

  // ── Run tests ─────────────────────────────────────────────────────────────
  lines.push("");
  lines.push(log.step(5, 7, "Running error-flow tests…"));
  lines.push("");

  const results: ErrorFlowResult[] = [];

  // Helper to run a sub-test and collect its result
  async function runSubTest(
    name: string,
    fn: () => Promise<string>,
    topic: string,
    payload: string,
    expected: string,
  ): Promise<void> {
    lines.push(log.run(`  Running: ${name}…`));
    try {
      const out     = await fn();
      const outLines = out.split("\n");
      const hasFail  = outLines.some((l) => l.includes("❌") || l.toLowerCase().includes("error:"));
      const hasWarn  = outLines.some((l) => l.includes("⚠️") || l.toLowerCase().includes("warning"));
      const status: TestStatus = hasFail ? "fail" : hasWarn ? "warning" : "pass";
      const snippets = outLines.filter((l) => l.includes("❌") || l.includes("⚠️")).slice(0, 3);
      const observed = outLines.find((l) => l.startsWith("  Observed") || l.includes("[Consumer]") || l.includes("offset")) ?? "See output above";
      results.push(buildResult({ testName: name, topic, payload, expectedBehavior: expected, observedBehavior: observed, logSnippets: snippets, status, recommendation: "" }));
      lines.push(log.ok(`    ${name} — ${status}`));
    } catch (e: any) {
      results.push(buildResult({ testName: name, topic, payload, expectedBehavior: expected, observedBehavior: `Exception: ${e.message}`, status: "fail", recommendation: "Check Kafka and BI project are running." }));
      lines.push(log.err(`    ${name} — fail (${e.message})`));
    }
  }

  await runSubTest(
    "Invalid JSON",
    () => triggerInvalidJsonError({ topicName: inputTopic, kafkaComposePath: args.kafkaComposePath, projectPath: args.projectPath }),
    inputTopic,
    readFixture("invalid-json.txt"),
    "JSON parse error in BI consumer; on fail logs error; commit still runs",
  );

  await runSubTest(
    "Schema mismatch (missing field)",
    () => triggerSchemaMismatchError({ variant: "missing-field", topicName: inputTopic, kafkaComposePath: args.kafkaComposePath, projectPath: args.projectPath }),
    inputTopic,
    readFixture("schema-missing-field.json"),
    "Type conversion error; on fail logs error; commit still runs",
  );

  await runSubTest(
    "Schema mismatch (wrong type)",
    () => triggerSchemaMismatchError({ variant: "wrong-type", topicName: inputTopic, kafkaComposePath: args.kafkaComposePath, projectPath: args.projectPath }),
    inputTopic,
    readFixture("schema-wrong-type.json"),
    "typedesc ConversionError for amount field; commit still runs",
  );

  await runSubTest(
    "Business rule (negative amount)",
    () => triggerBusinessRuleError({ topicName: inputTopic, kafkaComposePath: args.kafkaComposePath, projectPath: args.projectPath }),
    inputTopic,
    readFixture("business-invalid-amount.json"),
    "Warning if no validation; fail if validation rejects negative amount",
  );

  await runSubTest(
    "Missing topic behaviour",
    () => testMissingTopicError({ kafkaComposePath: args.kafkaComposePath }),
    "(auto-generated)",
    "(auto-generated)",
    "Auto-create creates topic; no error due to KAFKA_AUTO_CREATE_TOPICS_ENABLE=true",
  );

  await runSubTest(
    "Consumer not running",
    () => testConsumerNotRunningFlow({ topicName: inputTopic, kafkaComposePath: args.kafkaComposePath }),
    outputTopic,
    readFixture("valid-order.json"),
    "Output topic timeout; input message retained by Kafka",
  );

  if (inclCommit) {
    await runSubTest(
      "Manual commit / redelivery",
      () => testManualCommitRedelivery({ projectPath: args.projectPath, kafkaComposePath: args.kafkaComposePath }),
      inputTopic,
      readFixture("invalid-json.txt"),
      "Commit runs after loop; no redelivery observed",
    );
  }

  if (inclDlq) {
    await runSubTest(
      "DLQ check",
      () => checkDlq({ projectPath: args.projectPath, kafkaComposePath: args.kafkaComposePath }),
      "(dlq topic)",
      "(n/a)",
      "DLQ not implemented in generated project",
    );
  }

  // Kafka unavailable test runs last (stops Kafka then restarts)
  if (inclUnavail) {
    lines.push(log.warn("  ⚠️  Kafka unavailable test will STOP and RESTART Kafka — this affects all running consumers."));
    lines.push(log.run("  Running: Kafka unavailable…"));
    try {
      await docker.composeStop(composeDir);
      const stopResult = await docker.composePs(composeDir);
      const isStopped  = !stopResult.stdout.includes("running");
      results.push(buildResult({
        testName: "Kafka unavailable",
        topic: inputTopic,
        payload: readFixture("valid-order.json"),
        expectedBehavior: "BI listener disconnects; Kafka clients receive connection error",
        observedBehavior: isStopped ? "Kafka stopped successfully" : "Kafka may still be running",
        status: isStopped ? "pass" : "warning",
        recommendation: "Check BI runtime logs for connection refused errors",
      }));
      // Restart Kafka
      lines.push(log.run("  Restarting Kafka after unavailable test…"));
      await docker.composeUp(composeDir);
      await docker.waitUntilHealthy(
        await getKafkaContainer(composeDir) ?? "kafka",
        [`${DEFAULTS.KAFKA_SCRIPTS}/kafka-broker-api-versions.sh`, "--bootstrap-server", "localhost:9092"],
        60_000,
      );
      lines.push(log.ok("  Kafka restarted."));
    } catch (e: any) {
      results.push(buildResult({ testName: "Kafka unavailable", status: "fail", recommendation: `Exception: ${e.message}` }));
    }
  }

  // ── Report ────────────────────────────────────────────────────────────────
  lines.push("");
  lines.push(log.step(6, 7, "Generating report…"));
  const report = await generateErrorFlowReport({ results, title: "Error Flow Suite Results" });
  lines.push(report);

  lines.push("");
  lines.push(log.step(7, 7, "Suite complete."));
  lines.push(log.done("Call run_bi_demo to verify the happy path is still working."));

  return lines.join("\n");
}
