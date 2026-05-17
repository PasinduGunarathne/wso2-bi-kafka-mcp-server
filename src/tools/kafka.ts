// src/tools/kafka.ts
// Kafka lifecycle tools: start_kafka, stop_kafka, kafka_status, show_kafka_logs.

import * as docker from "../utils/docker.js";
import * as log from "../utils/logger.js";
import { DEFAULTS, resolveComposeDir } from "../config.js";

// ── Helpers ──────────────────────────────────────────────────────────────────

/** Parse NDJSON output from `docker compose ps --format json`. */
function parseComposePs(stdout: string): Array<{ Service: string; Name: string; State: string; Status: string }> {
  const rows: Array<{ Service: string; Name: string; State: string; Status: string }> = [];
  for (const line of stdout.split("\n")) {
    const t = line.trim();
    if (!t) continue;
    try {
      rows.push(JSON.parse(t));
    } catch {
      // ignore malformed lines
    }
  }
  return rows;
}

/** Resolve the actual container name for the kafka service at runtime. */
async function resolveKafkaContainer(composeDir: string): Promise<string> {
  const r = await docker.composePs(composeDir);
  if (r.ok) {
    const rows = parseComposePs(r.stdout);
    const found = rows.find((row) => row.Service === DEFAULTS.KAFKA_SERVICE);
    if (found?.Name) return found.Name;
  }
  // Fallback: use service name directly (works when Docker names it `kafka`)
  return DEFAULTS.KAFKA_SERVICE;
}

// ── start_kafka ──────────────────────────────────────────────────────────────

export async function startKafka(args: { kafkaComposePath?: string }): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const lines: string[] = [log.header("Start Kafka")];

  lines.push(log.info(`Compose directory : ${composeDir}`));
  lines.push(log.info(`Bootstrap server  : ${DEFAULTS.KAFKA_BOOTSTRAP_HOST}`));
  lines.push(log.info(`Kafka UI          : ${DEFAULTS.KAFKA_UI_URL}`));
  lines.push("");

  // Verify compose file exists
  const { default: fs } = await import("fs");
  const composeFile = `${composeDir}/${DEFAULTS.KAFKA_COMPOSE_FILE}`;
  if (!fs.existsSync(composeFile)) {
    lines.push(log.err(`Compose file not found: ${composeFile}`));
    lines.push(log.info("Check your kafkaComposePath argument or update DEFAULTS.KAFKA_COMPOSE_DIR in config.ts."));
    return lines.join("\n");
  }

  lines.push(log.run("Starting Kafka containers (docker compose up -d)..."));
  const r = await docker.composeUp(composeDir);

  if (!r.ok) {
    lines.push(log.err("Failed to start Kafka:"));
    lines.push(r.stderr || r.stdout);
    if (r.stderr?.includes("bind: address already in use") || r.stderr?.includes("port is already allocated")) {
      lines.push("");
      lines.push(log.warn("A port conflict was detected. Check that nothing else is using ports 9092 or 8080."));
    }
    return lines.join("\n");
  }

  lines.push(log.ok("Containers started. Waiting for Kafka broker to be ready..."));

  // Wait for broker health (up to 60s)
  const kafkaContainer = await resolveKafkaContainer(composeDir);
  const healthy = await docker.waitUntilHealthy(
    kafkaContainer,
    [`${DEFAULTS.KAFKA_SCRIPTS}/kafka-broker-api-versions.sh`, "--bootstrap-server", "localhost:9092"],
    60_000,
  );

  if (!healthy) {
    lines.push(log.warn("Kafka broker did not respond within 60s. It may still be starting."));
    lines.push(log.info("Run kafka_status to check current state."));
  } else {
    lines.push(log.ok("Kafka broker is ready."));
  }

  // Check Kafka UI
  const uiCheck = await docker.httpGet(DEFAULTS.KAFKA_UI_URL, 5_000);
  if (uiCheck.ok) {
    lines.push(log.ok(`Kafka UI is up: ${DEFAULTS.KAFKA_UI_URL}`));
  } else {
    lines.push(log.warn(`Kafka UI not yet reachable at ${DEFAULTS.KAFKA_UI_URL} — it may still be starting.`));
  }

  lines.push("");
  lines.push(log.done("Kafka is running."));
  lines.push(log.info(`Bootstrap server : ${DEFAULTS.KAFKA_BOOTSTRAP_HOST}`));
  lines.push(log.info(`Kafka UI         : ${DEFAULTS.KAFKA_UI_URL}`));

  return lines.join("\n");
}

// ── stop_kafka ───────────────────────────────────────────────────────────────

export async function stopKafka(args: {
  kafkaComposePath?: string;
  deleteVolumes?: boolean;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const deleteVols = Boolean(args.deleteVolumes);
  const lines: string[] = [log.header("Stop Kafka")];

  if (deleteVols) {
    lines.push(log.warn("deleteVolumes=true — containers AND volumes will be removed."));
    lines.push("");
  }

  lines.push(log.run(deleteVols ? "Running docker compose down -v..." : "Running docker compose stop..."));

  const r = deleteVols
    ? await docker.composeDown(composeDir)
    : await docker.composeStop(composeDir);

  if (!r.ok) {
    lines.push(log.err("Stop command failed:"));
    lines.push(r.stderr || r.stdout);
    return lines.join("\n");
  }

  lines.push(log.ok(deleteVols ? "Containers and volumes removed." : "Containers stopped (volumes preserved)."));
  if (!deleteVols) {
    lines.push(log.info("To also delete volumes, call stop_kafka with deleteVolumes=true."));
  }

  return lines.join("\n");
}

// ── kafka_status ─────────────────────────────────────────────────────────────

export async function kafkaStatus(args: { kafkaComposePath?: string }): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const lines: string[] = [log.header("Kafka Status")];

  // Container status
  const psResult = await docker.composePs(composeDir);
  if (!psResult.ok) {
    lines.push(log.err("Could not read container status:"));
    lines.push(psResult.stderr || "(no output)");
    lines.push(log.info("Is Docker running? Is the compose directory correct?"));
    return lines.join("\n");
  }

  const rows = parseComposePs(psResult.stdout);
  if (rows.length === 0) {
    lines.push(log.warn("No containers found. Run start_kafka to start the stack."));
    return lines.join("\n");
  }

  lines.push("");
  lines.push("Container Status:");
  lines.push("─".repeat(64));
  for (const row of rows) {
    const stateIcon = (row.State === "running") ? "✅" : "❌";
    lines.push(`  ${stateIcon}  ${(row.Service ?? "").padEnd(14)} ${(row.Name ?? "").padEnd(32)} ${row.Status ?? row.State}`);
  }
  lines.push("─".repeat(64));

  // Broker liveness
  const kafkaContainer = await resolveKafkaContainer(composeDir);
  const brokerCheck = await docker.exec(
    kafkaContainer,
    [`${DEFAULTS.KAFKA_SCRIPTS}/kafka-broker-api-versions.sh`, "--bootstrap-server", "localhost:9092"],
    10_000,
  );
  lines.push("");
  if (brokerCheck.ok) {
    lines.push(log.ok(`Kafka broker reachable at ${DEFAULTS.KAFKA_BOOTSTRAP_HOST}`));
  } else {
    lines.push(log.err(`Kafka broker not responding on ${DEFAULTS.KAFKA_BOOTSTRAP_HOST}`));
  }

  // Kafka UI
  const uiCheck = await docker.httpGet(DEFAULTS.KAFKA_UI_URL, 5_000);
  if (uiCheck.ok) {
    lines.push(log.ok(`Kafka UI reachable at ${DEFAULTS.KAFKA_UI_URL}`));
  } else {
    lines.push(log.warn(`Kafka UI not reachable at ${DEFAULTS.KAFKA_UI_URL}`));
  }

  lines.push("");
  lines.push(log.info(`Bootstrap server : ${DEFAULTS.KAFKA_BOOTSTRAP_HOST}`));
  lines.push(log.info(`Kafka UI         : ${DEFAULTS.KAFKA_UI_URL}`));

  return lines.join("\n");
}

// ── show_kafka_logs ──────────────────────────────────────────────────────────

const SENSITIVE_PATTERNS = [/password\s*=\s*\S+/gi, /secret\s*=\s*\S+/gi, /token\s*=\s*\S+/gi];

function stripSensitive(line: string): string {
  let out = line;
  for (const pattern of SENSITIVE_PATTERNS) {
    out = out.replace(pattern, "[REDACTED]");
  }
  return out;
}

export async function showKafkaLogs(args: {
  kafkaComposePath?: string;
  service?: string;
  lines?: number;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const service    = (typeof args.service === "string" && args.service.trim()) ? args.service.trim() : undefined;
  const tail       = (typeof args.lines === "number" && args.lines > 0) ? Math.min(args.lines, 500) : 50;

  const header  = service ? `Kafka Logs — ${service}` : "Kafka Logs — all services";
  const logLines: string[] = [log.header(header)];

  const r = await docker.composeLogs(composeDir, service, tail);

  if (!r.ok && !r.stdout.trim()) {
    logLines.push(log.err("Could not retrieve logs:"));
    logLines.push(r.stderr || "(no output)");
    return logLines.join("\n");
  }

  const output = r.stdout || r.stderr || "(no log output)";
  const cleaned = output
    .split("\n")
    .map(stripSensitive)
    .join("\n");

  logLines.push(cleaned);
  logLines.push("");
  logLines.push(log.info(`Showing last ${tail} lines${service ? ` of service '${service}'` : ""}.`));

  return logLines.join("\n");
}
