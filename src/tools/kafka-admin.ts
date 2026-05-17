// src/tools/kafka-admin.ts
// Kafka topic administration: list_topics, create_topic.

import * as docker from "../utils/docker.js";
import * as log from "../utils/logger.js";
import { validateTopicName, optionalPositiveInt } from "../utils/validation.js";
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

/** Ensure Kafka is running; return container name or throw. */
async function requireKafkaRunning(composeDir: string): Promise<string> {
  const container = await getKafkaContainer(composeDir);
  if (!container) {
    throw new Error(
      "Kafka is not running. Start it with start_kafka first.",
    );
  }
  return container;
}

// ── list_topics ───────────────────────────────────────────────────────────────

export async function listTopics(args: { kafkaComposePath?: string }): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const lines: string[] = [log.header("Kafka Topics")];

  const container = await requireKafkaRunning(composeDir);

  lines.push(log.run("Listing topics..."));

  const r = await docker.exec(
    container,
    [`${DEFAULTS.KAFKA_SCRIPTS}/kafka-topics.sh`, "--bootstrap-server", "localhost:9092", "--list"],
    15_000,
  );

  if (!r.ok) {
    lines.push(log.err("Failed to list topics:"));
    lines.push(r.stderr || r.stdout || "(no output)");
    return lines.join("\n");
  }

  const topics = r.stdout
    .split("\n")
    .map((t) => t.trim())
    .filter((t) => t.length > 0);

  // Filter out internal Kafka topics unless empty result
  const userTopics    = topics.filter((t) => !t.startsWith("__"));
  const internalTopics = topics.filter((t) => t.startsWith("__"));

  lines.push("");
  if (userTopics.length === 0) {
    lines.push(log.warn("No user-defined topics found."));
    lines.push(log.info("Create one with create_topic."));
  } else {
    lines.push(`User Topics (${userTopics.length}):`);
    for (const t of userTopics) {
      lines.push(`  • ${t}`);
    }
  }

  if (internalTopics.length > 0) {
    lines.push("");
    lines.push(`Internal Topics (${internalTopics.length}):`);
    for (const t of internalTopics) {
      lines.push(`  • ${t}`);
    }
  }

  lines.push("");
  lines.push(log.info(`Bootstrap: ${DEFAULTS.KAFKA_BOOTSTRAP_HOST}`));

  return lines.join("\n");
}

// ── create_topic ──────────────────────────────────────────────────────────────

export async function createTopic(args: {
  topicName: string;
  partitions?: number;
  replicationFactor?: number;
  kafkaComposePath?: string;
}): Promise<string> {
  const composeDir       = resolveComposeDir(args.kafkaComposePath);
  const topicName        = validateTopicName(args.topicName ?? "");
  const partitions       = optionalPositiveInt(args.partitions, "partitions", 1);
  const replicationFactor = optionalPositiveInt(args.replicationFactor, "replicationFactor", 1);

  const lines: string[] = [log.header(`Create Kafka Topic: ${topicName}`)];

  const container = await requireKafkaRunning(composeDir);

  lines.push(log.info(`Topic             : ${topicName}`));
  lines.push(log.info(`Partitions        : ${partitions}`));
  lines.push(log.info(`Replication factor: ${replicationFactor}`));
  lines.push("");
  lines.push(log.run("Creating topic..."));

  const r = await docker.exec(
    container,
    [
      `${DEFAULTS.KAFKA_SCRIPTS}/kafka-topics.sh`,
      "--bootstrap-server", "localhost:9092",
      "--create",
      "--topic", topicName,
      "--partitions", String(partitions),
      "--replication-factor", String(replicationFactor),
      "--if-not-exists",
    ],
    15_000,
  );

  if (!r.ok) {
    // Check for "already exists" in stderr — treat as success
    if (r.stderr?.includes("already exists")) {
      lines.push(log.warn(`Topic '${topicName}' already exists.`));
      return lines.join("\n");
    }
    lines.push(log.err("Failed to create topic:"));
    lines.push(r.stderr || r.stdout || "(no output)");
    return lines.join("\n");
  }

  lines.push(log.ok(`Topic '${topicName}' created successfully.`));
  lines.push("");
  lines.push(log.info("You can now produce messages to this topic with produce_test_message."));

  return lines.join("\n");
}
