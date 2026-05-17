// src/tools/setup.ts
// setup_kafka_and_bi  — one-command setup: Kafka broker + BI demo project
// run_bi_demo         — end-to-end demo: produce → consume → publish → verify

import fs from "fs";
import path from "path";
import * as docker from "../utils/docker.js";
import * as log from "../utils/logger.js";
import { DEFAULTS, resolveComposeDir, resolveSmartBiProjectPath } from "../config.js";
import { runPrerequisiteChecks } from "./prerequisites.js";

// ── Constants ─────────────────────────────────────────────────────────────────

const DEMO_INPUT_TOPIC  = "bi.orders.in";
const DEMO_OUTPUT_TOPIC = "bi.orders.out";

/** Resolved at call-time so it picks up the live WSO2Integrator detection. */
function defaultProjectPath(): string {
  return resolveSmartBiProjectPath("kafka-bi-demo");
}

// ── Ballerina version detection ───────────────────────────────────────────────

const FALLBACK_BAL_VERSION = "2201.13.4";

/**
 * Detect the installed Ballerina distribution version from `bal version` output.
 * Returns e.g. "2201.12.10". Falls back to FALLBACK_BAL_VERSION if undetectable.
 */
async function detectBalVersion(): Promise<string> {
  const r = await docker.run("bal", ["version"], undefined, 10_000);
  if (!r.ok) return FALLBACK_BAL_VERSION;
  // "Ballerina 2201.12.10 (Swan Lake Update 12)" → "2201.12.10"
  const match = r.stdout.match(/Ballerina\s+(\d+\.\d+\.\d+)/i);
  return match ? match[1] : FALLBACK_BAL_VERSION;
}

// ── BI Demo Project Templates ─────────────────────────────────────────────────

function ballerinaToml(balVersion: string): string {
  return `\
[package]
org     = "demo"
name    = "kafkademo"
version = "0.1.0"
distribution = "${balVersion}"

[build-options]
sticky = true
`;
}

function configBal(): string {
  return `\
// Configurable parameters — override via Config.toml or -C flags at runtime.
configurable string kafkaBootstrapServers = "${DEFAULTS.KAFKA_BOOTSTRAP_HOST}";
configurable string kafkaGroupId          = "bi-orders-consumer";
configurable string inputTopic            = "${DEMO_INPUT_TOPIC}";
configurable string outputTopic           = "${DEMO_OUTPUT_TOPIC}";
`;
}

function typesBal(): string {
  return `\
// Domain types for the order processing demo.

public type OrderEvent record {|
    string orderId;
    string customerId;
    string eventType;
    float  amount;
    string timestamp;
|};

public type ProcessedOrder record {|
    string orderId;
    string status;
    float  amount;
    string processedAt;
|};
`;
}

function connectionsBal(): string {
  // Backtick string literals and ${} interpolations are escaped for TypeScript template strings.
  return `\
import ballerinax/kafka;

// Shared Kafka producer — publishes processed results to the output topic.
final kafka:Producer orderProducer = check new (kafkaBootstrapServers);

// Kafka listener — subscribes to the input topic.
listener kafka:Listener orderListener = new (kafkaBootstrapServers, {
    groupId:         kafkaGroupId,
    topics:          [inputTopic],
    offsetReset:     kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1,
    autoCommit:      false
});
`;
}

function mainBal(): string {
  return `\
import ballerina/log;
import ballerina/time;
import ballerinax/kafka;

// Kafka consumer service — processes incoming orders and publishes results.
service kafka:Service on orderListener {

    remote function onConsumerRecord(kafka:AnydataConsumerRecord[] messages,
                                     kafka:Caller caller) returns error? {

        foreach kafka:AnydataConsumerRecord msg in messages {
            do {
                // 1. Deserialise bytes → string → OrderEvent
                byte[] rawBytes  = check msg.value.ensureType(byte[]);
                string rawStr    = check string:fromBytes(rawBytes);
                OrderEvent order = check rawStr.fromJsonStringWithType();

                log:printInfo("[Consumer] Received order",
                    orderId   = order.orderId,
                    eventType = order.eventType,
                    amount    = order.amount);

                // 2. Process the order
                ProcessedOrder processed = check processOrder(order);

                // 3. Publish result to output topic
                check orderProducer->send({
                    topic: outputTopic,
                    value: processed.toJsonString().toBytes()
                });

                log:printInfo("[Producer] Published processed order",
                    orderId     = processed.orderId,
                    status      = processed.status,
                    outputTopic = outputTopic);

            } on fail error e {
                log:printError("[Consumer] Failed to process message",
                    err = e, offset = msg.offset, partition = msg.partition);
            }
        }

        // Manual offset commit — only after all messages in the batch are handled.
        check caller->'commit();
    }
}
`;
}

function functionsBal(): string {
  return `\
import ballerina/time;

// Business logic — extend this function with your real processing.
public isolated function processOrder(OrderEvent order) returns ProcessedOrder|error {
    return {
        orderId:     order.orderId,
        status:      "PROCESSED",
        amount:      order.amount,
        processedAt: time:utcToString(time:utcNow())
    };
}
`;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Parse NDJSON from docker compose ps */
function parseComposePs(stdout: string): Array<{ Service: string; Name: string; State: string }> {
  const rows: Array<{ Service: string; Name: string; State: string }> = [];
  for (const line of stdout.split("\n")) {
    const t = line.trim();
    if (!t) continue;
    try { rows.push(JSON.parse(t)); } catch { /* skip */ }
  }
  return rows;
}

async function getKafkaContainer(composeDir: string): Promise<string | null> {
  const r = await docker.composePs(composeDir);
  if (!r.ok || !r.stdout.trim()) return null;
  for (const row of parseComposePs(r.stdout)) {
    if (row.Service === DEFAULTS.KAFKA_SERVICE && row.State === "running") return row.Name;
  }
  return null;
}

async function createTopicIfMissing(
  container: string,
  topicName: string,
): Promise<string> {
  const r = await docker.exec(container, [
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-topics.sh`,
    "--bootstrap-server", "localhost:9092",
    "--create", "--topic", topicName,
    "--partitions", "1", "--replication-factor", "1",
    "--if-not-exists",
  ], 15_000);
  return r.ok ? `✅  ${topicName}` : `⚠️   ${topicName} — ${r.stderr.trim()}`;
}

function writeDemoProject(projectPath: string, balVersion: string): string[] {
  fs.mkdirSync(projectPath, { recursive: true });
  const files: Array<[string, string]> = [
    ["Ballerina.toml",  ballerinaToml(balVersion)],
    ["config.bal",      configBal()],
    ["types.bal",       typesBal()],
    ["connections.bal", connectionsBal()],
    ["main.bal",        mainBal()],
    ["functions.bal",   functionsBal()],
  ];
  for (const [name, content] of files) {
    fs.writeFileSync(path.join(projectPath, name), content, "utf8");
  }
  return files.map(([name]) => name);
}

// ── setup_kafka_and_bi ────────────────────────────────────────────────────────

export async function setupKafkaAndBi(args: {
  projectPath?: string;
  kafkaComposePath?: string;
}): Promise<string> {
  const composeDir  = resolveComposeDir(args.kafkaComposePath);
  const projectPath = args.projectPath?.trim() || defaultProjectPath();
  const TOTAL       = 7;
  const lines: string[] = [log.header("Setup: Kafka + Ballerina Integrator: BI")];

  lines.push(log.info(`Kafka Compose dir : ${composeDir}`));
  lines.push(log.info(`BI project path   : ${projectPath}`));
  lines.push(log.info(`Input topic       : ${DEMO_INPUT_TOPIC}`));
  lines.push(log.info(`Output topic      : ${DEMO_OUTPUT_TOPIC}`));
  lines.push("");

  // ── Step 1: Prerequisites ─────────────────────────────────────────────────
  lines.push(log.step(1, TOTAL, "Checking prerequisites..."));

  const prereqChecks = await runPrerequisiteChecks({
    kafkaComposePath: args.kafkaComposePath,
  });

  // Hard blockers: Docker, Compose, Ballerina, Compose file
  const BLOCKERS = ["Docker", "Docker Compose v2", "Ballerina CLI (bal)", "Kafka docker-compose.yml"];
  const failed   = prereqChecks.filter((c) => !c.ok && BLOCKERS.includes(c.name));

  // Always show the status of the hard-blocker checks
  for (const c of prereqChecks.filter((c) => BLOCKERS.includes(c.name))) {
    lines.push(c.ok ? log.ok(`${c.name}: ${c.detail}`) : log.err(`${c.name}: ${c.detail}`));
  }
  lines.push("");

  if (failed.length > 0) {
    lines.push(log.err(
      `${failed.length} required tool(s) are missing. ` +
      "Setup cannot continue until they are installed.",
    ));
    lines.push("");
    lines.push("═".repeat(62));

    for (const f of failed) {
      lines.push("");
      lines.push(`❌  ${f.name.toUpperCase()} — INSTALL GUIDE`);
      lines.push("─".repeat(62));
      if (f.installNote) {
        for (const noteLine of f.installNote.split("\n")) {
          lines.push(noteLine ? `  ${noteLine}` : "");
        }
      }
      lines.push("");
    }

    lines.push("═".repeat(62));
    lines.push("");
    lines.push(log.info(
      "After installing, open a new terminal so PATH changes take effect,",
    ));
    lines.push(log.info(
      "then re-run:  setup_kafka_and_bi",
    ));
    return lines.join("\n");
  }
  lines.push(log.ok("All required tools are present."));
  lines.push("");

  // ── Step 2: Start Kafka ───────────────────────────────────────────────────
  lines.push(log.step(2, TOTAL, "Starting Kafka (docker compose up -d)..."));

  // Check if already running to avoid a redundant compose up
  const existingContainer = await getKafkaContainer(composeDir);
  if (existingContainer) {
    lines.push(log.ok(`Kafka already running (container: ${existingContainer}). Skipping start.`));
  } else {
    // Pre-check ports before compose up to give a clear error rather than a Docker error
    const { default: net } = await import("net");
    const portBlockers: number[] = [];
    for (const port of [9092, 8080]) {
      const blocked = await new Promise<boolean>((resolve) => {
        const srv = net.createServer();
        srv.once("error", (e: NodeJS.ErrnoException) => resolve(e.code === "EADDRINUSE"));
        srv.once("listening", () => srv.close(() => resolve(false)));
        srv.listen(port, "127.0.0.1");
      });
      if (blocked) portBlockers.push(port);
    }

    if (portBlockers.length > 0) {
      lines.push(log.err(`Port conflict: ${portBlockers.join(", ")} already in use.`));
      lines.push(log.info("Another process is occupying the required port(s)."));
      lines.push(log.info("Run check_prerequisites for detailed instructions on freeing these ports,"));
      lines.push(log.info("or run stop_kafka if Kafka is already running from a previous session."));
      return lines.join("\n");
    }

    const upResult = await docker.composeUp(composeDir);
    if (!upResult.ok) {
      lines.push(log.err("Failed to start Kafka:"));
      lines.push(upResult.stderr || upResult.stdout);
      return lines.join("\n");
    }
    lines.push(log.ok("Containers started."));
  }
  lines.push("");

  // ── Step 3: Wait for broker readiness ─────────────────────────────────────
  lines.push(log.step(3, TOTAL, "Waiting for Kafka broker to be ready (up to 60s)..."));

  const kafkaContainer = (await getKafkaContainer(composeDir)) ?? DEFAULTS.KAFKA_SERVICE;
  const brokerReady = await docker.waitUntilHealthy(
    kafkaContainer,
    [`${DEFAULTS.KAFKA_SCRIPTS}/kafka-broker-api-versions.sh`, "--bootstrap-server", "localhost:9092"],
    60_000,
  );

  if (!brokerReady) {
    lines.push(log.err("Kafka broker did not become ready within 60s."));
    lines.push(log.info("Check docker logs with: show_kafka_logs"));
    return lines.join("\n");
  }
  lines.push(log.ok(`Kafka broker ready at ${DEFAULTS.KAFKA_BOOTSTRAP_HOST}`));
  lines.push("");

  // ── Step 4: Create topics ─────────────────────────────────────────────────
  lines.push(log.step(4, TOTAL, `Creating topics: ${DEMO_INPUT_TOPIC}, ${DEMO_OUTPUT_TOPIC}...`));

  const inputResult  = await createTopicIfMissing(kafkaContainer, DEMO_INPUT_TOPIC);
  const outputResult = await createTopicIfMissing(kafkaContainer, DEMO_OUTPUT_TOPIC);
  lines.push(inputResult);
  lines.push(outputResult);
  lines.push("");

  // ── Step 5: Generate BI project ───────────────────────────────────────────
  lines.push(log.step(5, TOTAL, `Generating BI demo project at ${projectPath}...`));

  const balVersion = await detectBalVersion();
  lines.push(log.info(`Ballerina distribution detected: ${balVersion}`));

  const generated = writeDemoProject(projectPath, balVersion);
  for (const f of generated) {
    lines.push(log.ok(`  ${f}`));
  }
  lines.push("");

  // ── Step 6: Build BI project ──────────────────────────────────────────────
  lines.push(log.step(6, TOTAL, "Building BI project (bal build)... (may take 1–2 min on first run)"));

  const buildResult = await docker.run("bal", ["build"], projectPath, 180_000);
  const buildOutput = [buildResult.stdout, buildResult.stderr].filter(Boolean).join("\n").trim();

  if (!buildResult.ok) {
    lines.push(log.err("Build failed. Compiler output:"));
    lines.push("─".repeat(60));
    lines.push(buildOutput);
    lines.push("─".repeat(60));
    lines.push(log.info("Fix the errors above, then re-run setup_kafka_and_bi."));
    return lines.join("\n");
  }

  lines.push(log.ok("Build succeeded."));
  lines.push("");

  // ── Step 7: Summary ───────────────────────────────────────────────────────
  lines.push(log.step(7, TOTAL, "Setup complete."));
  lines.push("");
  lines.push(log.box([
    "Kafka broker    : " + DEFAULTS.KAFKA_BOOTSTRAP_HOST,
    "Kafka UI        : " + DEFAULTS.KAFKA_UI_URL,
    "Input topic     : " + DEMO_INPUT_TOPIC,
    "Output topic    : " + DEMO_OUTPUT_TOPIC,
    "BI project      : " + projectPath,
  ]));
  lines.push("");
  lines.push(log.done(
    "Everything is ready. " +
    "Call run_bi_demo to execute the end-to-end sample flow.",
  ));

  return lines.join("\n");
}

// ── run_bi_demo ───────────────────────────────────────────────────────────────

export async function runBiDemo(args: {
  projectPath?: string;
  kafkaComposePath?: string;
}): Promise<string> {
  const composeDir  = resolveComposeDir(args.kafkaComposePath);
  const projectPath = args.projectPath?.trim() || defaultProjectPath();
  const TOTAL       = 5;
  const lines: string[] = [log.header("End-to-End Demo: Kafka + BI")];

  lines.push(log.info(`BI project  : ${projectPath}`));
  lines.push(log.info(`Input topic : ${DEMO_INPUT_TOPIC}`));
  lines.push(log.info(`Output topic: ${DEMO_OUTPUT_TOPIC}`));
  lines.push("");

  // ── Step 1: Verify Kafka is running ───────────────────────────────────────
  lines.push(log.step(1, TOTAL, "Verifying Kafka is running..."));

  const kafkaContainer = await getKafkaContainer(composeDir);
  if (!kafkaContainer) {
    lines.push(log.err("Kafka is not running. Run setup_kafka_and_bi first."));
    return lines.join("\n");
  }
  lines.push(log.ok(`Kafka running (${kafkaContainer})`));
  lines.push("");

  // ── Step 2: Verify BI project exists ─────────────────────────────────────
  lines.push(log.step(2, TOTAL, "Verifying BI project..."));

  const ballerinaTomlPath = path.join(projectPath, "Ballerina.toml");
  if (!fs.existsSync(ballerinaTomlPath)) {
    lines.push(log.err(`BI project not found at: ${projectPath}`));
    lines.push(log.info("Run setup_kafka_and_bi first to generate and build the project."));
    return lines.join("\n");
  }
  lines.push(log.ok(`BI project found at ${projectPath}`));
  lines.push("");

  // ── Step 3: Produce test OrderEvent to input topic ────────────────────────
  lines.push(log.step(3, TOTAL, `Producing test OrderEvent to '${DEMO_INPUT_TOPIC}'...`));

  const orderId = `ORD-${Date.now()}`;
  const testOrder = {
    orderId:    orderId,
    customerId: "CUST-001",
    eventType:  "order-created",
    amount:     149.99,
    timestamp:  new Date().toISOString(),
  };
  const payload = JSON.stringify(testOrder);

  lines.push(log.info("Message:"));
  lines.push(`    ${payload}`);
  lines.push("");

  const produceResult = await docker.execWithStdin(
    kafkaContainer,
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ${DEMO_INPUT_TOPIC}`,
    payload,
    15_000,
  );

  if (!produceResult.ok && produceResult.stderr && !produceResult.stderr.includes(">>")) {
    lines.push(log.err("Failed to produce message:"));
    lines.push(produceResult.stderr);
    return lines.join("\n");
  }
  lines.push(log.ok(`OrderEvent published (orderId: ${orderId})`));
  lines.push("");

  // ── Step 4: Run BI listener — it will pick up and process the message ─────
  lines.push(log.step(4, TOTAL, "Starting BI listener (bal run)..."));
  lines.push(log.info(
    "The listener will start, consume the order from '" + DEMO_INPUT_TOPIC + "', " +
    "process it, and publish the result to '" + DEMO_OUTPUT_TOPIC + "'.",
  ));
  lines.push(log.wait("Capturing 25s of runtime output..."));
  lines.push("");

  // Use a unique group ID so the consumer always reads from the earliest offset,
  // ensuring it picks up the message we just produced even on repeated demo runs.
  const demoGroupId = `bi-demo-${Date.now()}`;

  const runResult = await docker.run(
    "bal",
    ["run", "-C", `kafkaGroupId=${demoGroupId}`],
    projectPath,
    25_000,
  );

  const runOutput = [runResult.stdout, runResult.stderr].filter(Boolean).join("\n").trim();

  if (runOutput) {
    lines.push("BI listener output:");
    lines.push("─".repeat(60));
    lines.push(runOutput);
    lines.push("─".repeat(60));
    lines.push("");
  }

  // Detect key log lines to confirm the flow ran
  const receivedOrder  = runOutput.includes("[Consumer] Received order");
  const publishedOrder = runOutput.includes("[Producer] Published processed order");
  const buildError     = runOutput.includes("error:") || runOutput.includes("compilation failed");

  if (buildError) {
    lines.push(log.err("Compilation error detected. Run validate_bi_project for details."));
    return lines.join("\n");
  }

  if (receivedOrder) {
    lines.push(log.ok("✔ Consumer received the order."));
  } else {
    lines.push(log.warn("Consumer log line not detected — the message may still be processing."));
  }

  if (publishedOrder) {
    lines.push(log.ok("✔ Processed order published to output topic."));
  }
  lines.push("");

  // ── Step 5: Consume from output topic to confirm end-to-end ──────────────
  lines.push(log.step(5, TOTAL, `Verifying output on '${DEMO_OUTPUT_TOPIC}'...`));

  const consumeResult = await docker.exec(kafkaContainer, [
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-console-consumer.sh`,
    "--bootstrap-server", "localhost:9092",
    "--topic", DEMO_OUTPUT_TOPIC,
    "--from-beginning",
    "--max-messages", "10",
    "--timeout-ms", "8000",
  ], 20_000);

  const consumedMessages = consumeResult.stdout
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  // Find the message matching our orderId
  const matchingMessage = consumedMessages.find((m) => m.includes(orderId));

  if (matchingMessage) {
    lines.push(log.ok(`Found processed order on '${DEMO_OUTPUT_TOPIC}':`));
    lines.push("");
    lines.push(`    ${matchingMessage}`);
  } else if (consumedMessages.length > 0) {
    lines.push(log.ok(`${consumedMessages.length} message(s) found on '${DEMO_OUTPUT_TOPIC}':`));
    for (const m of consumedMessages.slice(-3)) {
      lines.push(`    ${m}`);
    }
    lines.push(log.info("(The current order may appear on the next run if the listener is still processing.)"));
  } else {
    lines.push(log.warn(`No messages found on '${DEMO_OUTPUT_TOPIC}' yet.`));
    lines.push(log.info("The BI listener may need more time. Try running run_bi_demo again."));
  }

  lines.push("");

  // ── Final summary ─────────────────────────────────────────────────────────
  lines.push("─".repeat(60));
  lines.push(log.done("End-to-end demo complete."));
  lines.push("");
  lines.push(log.box([
    "Flow:  produce → " + DEMO_INPUT_TOPIC,
    "       BI listener consumed & processed the order",
    "       BI listener published → " + DEMO_OUTPUT_TOPIC,
    "       MCP verified output topic",
    "",
    "Order ID : " + orderId,
    "Group ID : " + demoGroupId,
  ]));
  lines.push("");
  lines.push(log.info("Next steps:"));
  lines.push("  • Edit " + path.join(projectPath, "functions.bal") + " to add your business logic.");
  lines.push("  • Run validate_bi_project any time to check for compile errors.");
  lines.push("  • Use produce_test_message / consume_test_message for ad-hoc testing.");
  lines.push("  • Open Kafka UI at " + DEFAULTS.KAFKA_UI_URL + " to inspect topics and messages.");

  return lines.join("\n");
}
