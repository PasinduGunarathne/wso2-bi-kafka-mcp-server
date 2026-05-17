// src/tools/kafka-messages.ts
// produce_test_message, consume_test_message.

import * as docker from "../utils/docker.js";
import * as log from "../utils/logger.js";
import { validateTopicName, optionalPositiveInt, optionalBool } from "../utils/validation.js";
import { DEFAULTS, resolveComposeDir } from "../config.js";

/** Resolve the running kafka container name from compose ps output. */
async function getKafkaContainer(composeDir: string): Promise<string | null> {
  const r = await docker.composePs(composeDir);
  if (!r.ok || !r.stdout.trim()) return null;
  for (const line of r.stdout.split("\n")) {
    const t = line.trim();
    if (!t) continue;
    try {
      const obj = JSON.parse(t);
      if (obj.Service === DEFAULTS.KAFKA_SERVICE && obj.State === "running") return obj.Name as string;
    } catch { /* skip */ }
  }
  return null;
}

async function requireKafkaRunning(composeDir: string): Promise<string> {
  const container = await getKafkaContainer(composeDir);
  if (!container) throw new Error("Kafka is not running. Start it with start_kafka first.");
  return container;
}

// ── produce_test_message ──────────────────────────────────────────────────────

export async function produceTestMessage(args: {
  topicName: string;
  message?: string;
  kafkaComposePath?: string;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const topicName  = validateTopicName(args.topicName ?? "");
  const lines: string[] = [log.header(`Produce Message → ${topicName}`)];

  // Build the message payload
  let payload: string;
  if (typeof args.message === "string" && args.message.trim().length > 0) {
    payload = args.message.trim();
  } else {
    payload = JSON.stringify({
      test: true,
      topic: topicName,
      timestamp: new Date().toISOString(),
      source: "kafka-bi-mcp-server",
    });
  }

  lines.push(log.info(`Topic  : ${topicName}`));
  lines.push(log.info(`Payload: ${payload}`));
  lines.push("");

  const container = await requireKafkaRunning(composeDir);

  // Use bash -c to pipe the payload into kafka-console-producer.sh.
  // The shell command is a fixed template; the payload is passed via stdin, not interpolated.
  const shellCmd = `${DEFAULTS.KAFKA_SCRIPTS}/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ${topicName}`;

  lines.push(log.run("Sending message..."));

  const r = await docker.execWithStdin(container, shellCmd, payload, 15_000);

  if (!r.ok && r.stderr && !r.stderr.includes(">>")) {
    lines.push(log.err("Failed to produce message:"));
    lines.push(r.stderr);
    return lines.join("\n");
  }

  lines.push(log.ok("Message produced successfully."));
  lines.push("");
  lines.push(log.info(`Consume it with: consume_test_message { "topicName": "${topicName}" }`));

  return lines.join("\n");
}

// ── consume_test_message ──────────────────────────────────────────────────────

export async function consumeTestMessage(args: {
  topicName: string;
  maxMessages?: number;
  fromBeginning?: boolean;
  kafkaComposePath?: string;
}): Promise<string> {
  const composeDir   = resolveComposeDir(args.kafkaComposePath);
  const topicName    = validateTopicName(args.topicName ?? "");
  const maxMessages  = optionalPositiveInt(args.maxMessages, "maxMessages", 5);
  const fromBeginning = optionalBool(args.fromBeginning, true);
  const timeoutMs    = 10_000; // fixed; protect against hanging consumers

  const lines: string[] = [log.header(`Consume Messages ← ${topicName}`)];

  lines.push(log.info(`Topic        : ${topicName}`));
  lines.push(log.info(`Max messages : ${maxMessages}`));
  lines.push(log.info(`From         : ${fromBeginning ? "beginning" : "latest"}`));
  lines.push(log.info(`Timeout      : ${timeoutMs}ms`));
  lines.push("");

  const container = await requireKafkaRunning(composeDir);

  const cmd = [
    `${DEFAULTS.KAFKA_SCRIPTS}/kafka-console-consumer.sh`,
    "--bootstrap-server", "localhost:9092",
    "--topic", topicName,
    "--max-messages", String(maxMessages),
    "--timeout-ms", String(timeoutMs),
  ];
  if (fromBeginning) cmd.push("--from-beginning");

  lines.push(log.run(`Consuming up to ${maxMessages} message(s)...`));

  const r = await docker.exec(container, cmd, timeoutMs + 10_000);

  const output = r.stdout.trim();
  const stdErr = r.stderr.trim();

  if (!output && !r.ok) {
    // Distinguish timeout (no messages) from actual error
    if (stdErr.includes("Processed a total of 0 messages") || stdErr.includes("Timeout")) {
      lines.push(log.warn(`No messages found on topic '${topicName}' within ${timeoutMs}ms.`));
      lines.push(log.info("Produce a message first with produce_test_message, then try again."));
    } else {
      lines.push(log.err("Consumer failed:"));
      lines.push(stdErr || "(no output)");
    }
    return lines.join("\n");
  }

  const messages = output.split("\n").filter((l) => l.trim().length > 0);
  lines.push(log.ok(`Received ${messages.length} message(s):`));
  lines.push("");
  lines.push("─".repeat(64));
  for (let i = 0; i < messages.length; i++) {
    lines.push(`[${i + 1}] ${messages[i]}`);
  }
  lines.push("─".repeat(64));

  // Print the consumer summary from stderr (shows total processed count)
  if (stdErr) {
    const summaryLine = stdErr
      .split("\n")
      .find((l) => l.includes("Processed a total of"));
    if (summaryLine) lines.push(log.info(summaryLine.trim()));
  }

  return lines.join("\n");
}
