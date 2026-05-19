// src/tools/replica-manager.ts
// Local multi-replica tools: manage multiple `bal run` processes that share a
// Kafka consumer group so Kafka distributes partitions across them.
//
// The process registry is in-memory — replicas are tied to the MCP server
// session and are automatically killed when the server process exits
// (execa cleanup:true).  This is intentional for local development use.

import path from "path";
import type { Subprocess } from "execa";
import * as docker from "../utils/docker.js";
import * as log from "../utils/logger.js";
import { resolveBiProjectPath, resolveComposeDir } from "../config.js";
import type { ReplicaInfo, ReplicaStatus } from "../types.js";

// ── In-memory registry ────────────────────────────────────────────────────────

interface LiveReplica extends ReplicaInfo {
  handle: Subprocess;
}

const registry = new Map<string, LiveReplica>();

const MAX_LOG_LINES = 50;

function pushLog(replica: LiveReplica, line: string): void {
  replica.recentLogs.push(line);
  if (replica.recentLogs.length > MAX_LOG_LINES) {
    replica.recentLogs.shift();
  }
}

function uptimeStr(startedAt: string): string {
  const ms = Date.now() - new Date(startedAt).getTime();
  const s  = Math.floor(ms / 1000);
  if (s < 60)  return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

function statusEmoji(status: ReplicaStatus): string {
  switch (status) {
    case "starting": return "⏳";
    case "running":  return "✅";
    case "stopped":  return "⏹️";
    case "error":    return "❌";
  }
}

// ── assertBiProject helper ────────────────────────────────────────────────────

import fs from "fs";

function assertBiProject(biPath: string): void {
  const toml = path.join(biPath, "Ballerina.toml");
  if (!fs.existsSync(toml)) {
    throw new Error(
      `No Ballerina.toml found at ${biPath}. ` +
      "Run setup_kafka_and_bi or generate_bi_kafka_sample to create a project first.",
    );
  }
}

function assertJarExists(biPath: string): void {
  const binDir = path.join(biPath, "target", "bin");
  if (!fs.existsSync(binDir)) {
    throw new Error(
      `No target/bin directory found at ${biPath}. ` +
      "Run validate_bi_project first to compile the project.",
    );
  }
  const jars = fs.readdirSync(binDir).filter((f) => f.endsWith(".jar"));
  if (jars.length === 0) {
    throw new Error(
      `No JAR found in ${binDir}. ` +
      "Run validate_bi_project first to compile the project.",
    );
  }
}

// ── Tool 1: start_bi_replica ─────────────────────────────────────────────────

interface StartBiReplicaArgs {
  projectPath?: string;
  groupId?: string;
  instanceId?: string;
  configOverrides?: Record<string, string>;
}

export async function startBiReplica(args: StartBiReplicaArgs): Promise<string> {
  const lines: string[] = [log.header("Start BI Replica (local process)")];

  const biPath  = resolveBiProjectPath(args.projectPath);
  const groupId = args.groupId ?? "order-processor";
  const id      = args.instanceId ?? `bi-replica-${Date.now()}`;

  try {
    assertBiProject(biPath);
    assertJarExists(biPath);
  } catch (e: any) {
    lines.push(log.err(e.message));
    return lines.join("\n");
  }

  if (registry.has(id)) {
    lines.push(log.err(`Instance ID "${id}" is already running. Choose a different instanceId.`));
    return lines.join("\n");
  }

  // Build `bal run -- -Ckey=value ...` args
  const runArgs: string[] = ["run", "--", `-CkafkaGroupId=${groupId}`];
  if (args.configOverrides) {
    for (const [k, v] of Object.entries(args.configOverrides)) {
      runArgs.push(`-C${k}=${v}`);
    }
  }

  lines.push(log.info(`Instance ID  : ${id}`));
  lines.push(log.info(`Project path : ${biPath}`));
  lines.push(log.info(`Consumer group: ${groupId}`));
  lines.push(log.info(`Args         : ${runArgs.slice(1).join(" ")}`));
  lines.push("");

  const replica: LiveReplica = {
    instanceId:  id,
    pid:         undefined,
    projectPath: biPath,
    groupId,
    startedAt:   new Date().toISOString(),
    status:      "starting",
    recentLogs:  [],
    handle:      undefined as any, // filled below
  };

  const handle = docker.spawnBackground("bal", runArgs, biPath);
  replica.handle = handle;
  registry.set(id, replica);

  // Capture PID once the process spawns
  handle.on("spawn", () => {
    replica.pid = handle.pid;
    replica.status = "running";
  });

  // Buffer stdout/stderr into the rolling log
  handle.stdout?.on("data", (chunk: Buffer) => {
    const text = chunk.toString();
    if (replica.status === "starting") replica.status = "running";
    for (const line of text.split("\n")) {
      if (line.trim()) pushLog(replica, line);
    }
  });
  handle.stderr?.on("data", (chunk: Buffer) => {
    const text = chunk.toString();
    for (const line of text.split("\n")) {
      if (line.trim()) pushLog(replica, line);
    }
  });

  // Update status on exit
  handle.on("exit", (code: number | null) => {
    if (replica.status !== "stopped") {
      replica.status = code === 0 || code === null ? "stopped" : "error";
    }
    registry.delete(id);
  });

  lines.push(log.ok(`Replica "${id}" started successfully.`));
  lines.push(log.info("The process is running in the background."));
  lines.push(log.info("Use list_bi_replicas to monitor, stop_bi_replica to stop."));
  lines.push("");
  lines.push(log.info("Kafka consumer group behaviour:"));
  lines.push("  All replicas sharing the same groupId will have partitions distributed");
  lines.push("  across them automatically. Kafka rebalances on each new replica.");

  return lines.join("\n");
}

// ── Tool 2: stop_bi_replica ──────────────────────────────────────────────────

interface StopBiReplicaArgs {
  instanceId: string;
}

export async function stopBiReplica(args: StopBiReplicaArgs): Promise<string> {
  const lines: string[] = [log.header("Stop BI Replica")];
  const { instanceId } = args;

  const replica = registry.get(instanceId);
  if (!replica) {
    lines.push(log.err(`No running replica found with instanceId "${instanceId}".`));
    lines.push(log.info("Use list_bi_replicas to see running instances."));
    return lines.join("\n");
  }

  lines.push(log.run(`Stopping replica "${instanceId}" (SIGTERM)…`));

  replica.status = "stopped";
  replica.handle.kill("SIGTERM");

  // Give the process 5 seconds to exit cleanly, then SIGKILL
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      try { replica.handle.kill("SIGKILL"); } catch { /* already dead */ }
      resolve();
    }, 5_000);
    replica.handle.on("exit", () => { clearTimeout(timer); resolve(); });
  });

  registry.delete(instanceId);

  lines.push(log.ok(`Replica "${instanceId}" stopped.`));

  if (replica.recentLogs.length > 0) {
    lines.push("");
    lines.push("Last log lines:");
    lines.push("─".repeat(48));
    for (const l of replica.recentLogs.slice(-5)) lines.push(`  ${l}`);
    lines.push("─".repeat(48));
  }

  return lines.join("\n");
}

// ── Tool 3: stop_all_bi_replicas ─────────────────────────────────────────────

interface StopAllBiReplicasArgs {
  projectPath?: string;
}

export async function stopAllBiReplicas(args: StopAllBiReplicasArgs): Promise<string> {
  const lines: string[] = [log.header("Stop All BI Replicas")];

  const biPath = args.projectPath ? resolveBiProjectPath(args.projectPath) : undefined;

  const targets = [...registry.entries()].filter(([, r]) =>
    biPath ? r.projectPath === biPath : true,
  );

  if (targets.length === 0) {
    lines.push(log.info(biPath
      ? `No running replicas found for project: ${biPath}`
      : "No local BI replicas are currently running.",
    ));
    return lines.join("\n");
  }

  lines.push(log.run(`Stopping ${targets.length} replica(s)…`));

  await Promise.all(
    targets.map(async ([id, replica]) => {
      replica.status = "stopped";
      replica.handle.kill("SIGTERM");
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          try { replica.handle.kill("SIGKILL"); } catch { /* already dead */ }
          resolve();
        }, 5_000);
        replica.handle.on("exit", () => { clearTimeout(timer); resolve(); });
      });
      registry.delete(id);
      lines.push(log.ok(`  Stopped: ${id}`));
    }),
  );

  lines.push("");
  lines.push(log.ok(`${targets.length} replica(s) stopped.`));

  return lines.join("\n");
}

// ── Tool 4: list_bi_replicas ─────────────────────────────────────────────────

export async function listBiReplicas(_args: Record<string, never>): Promise<string> {
  const lines: string[] = [log.header("Local BI Replicas")];

  if (registry.size === 0) {
    lines.push(log.info("No local BI replicas are currently running."));
    lines.push("");
    lines.push("Start replicas with: start_bi_replica");
    return lines.join("\n");
  }

  lines.push(`${registry.size} replica(s) running:\n`);

  // Header row
  const COL = [22, 7, 20, 10, 10];
  const header = [
    "INSTANCE ID".padEnd(COL[0]),
    "PID".padEnd(COL[1]),
    "GROUP ID".padEnd(COL[2]),
    "UPTIME".padEnd(COL[3]),
    "STATUS",
  ].join("  ");
  lines.push(header);
  lines.push("─".repeat(header.length));

  for (const [, r] of registry) {
    const row = [
      r.instanceId.padEnd(COL[0]),
      (r.pid ? String(r.pid) : "—").padEnd(COL[1]),
      r.groupId.padEnd(COL[2]),
      uptimeStr(r.startedAt).padEnd(COL[3]),
      `${statusEmoji(r.status)}  ${r.status}`,
    ].join("  ");
    lines.push(row);

    if (r.recentLogs.length > 0) {
      const tail = r.recentLogs.slice(-3);
      for (const l of tail) lines.push(`  ${r.instanceId}  │  ${l}`);
      lines.push("");
    }
  }

  return lines.join("\n");
}

// ── Tool 5: inspect_consumer_group ───────────────────────────────────────────

interface InspectConsumerGroupArgs {
  groupId: string;
  kafkaComposePath?: string;
}

export async function inspectConsumerGroup(args: InspectConsumerGroupArgs): Promise<string> {
  const lines: string[] = [log.header("Kafka Consumer Group Inspection")];
  const { groupId } = args;
  const composeDir  = resolveComposeDir(args.kafkaComposePath);

  lines.push(log.info(`Group ID: ${groupId}`));
  lines.push("");

  // Resolve the Kafka container name
  const container = await docker.resolveContainerName(composeDir, "kafka");
  if (!container) {
    lines.push(log.err("Kafka container is not running. Start it with start_kafka."));
    return lines.join("\n");
  }

  lines.push(log.run("Querying consumer group details…"));

  const r = await docker.exec(container, [
    "/opt/kafka/bin/kafka-consumer-groups.sh",
    "--bootstrap-server", "localhost:9092",
    "--group", groupId,
    "--describe",
  ], 20_000);

  if (!r.ok || !r.stdout.trim()) {
    // Group may not exist yet (no consumer has connected)
    const combinedOutput = (r.stdout + r.stderr).trim();
    if (combinedOutput.toLowerCase().includes("does not exist") ||
        combinedOutput.toLowerCase().includes("group id") ||
        !combinedOutput) {
      lines.push(log.warn(`Consumer group "${groupId}" does not exist yet.`));
      lines.push(log.info("Start at least one BI replica with start_bi_replica to create the group."));
    } else {
      lines.push(log.err("Failed to query consumer group."));
      lines.push(combinedOutput);
    }
    return lines.join("\n");
  }

  const rawLines = r.stdout.trim().split("\n");

  // The first non-empty line is usually the header; the rest are data rows.
  // kafka-consumer-groups.sh output format:
  //   GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
  lines.push("─".repeat(80));
  for (const l of rawLines) lines.push(l);
  lines.push("─".repeat(80));
  lines.push("");

  // Parse summary metrics
  const dataRows = rawLines.filter((l) => !l.startsWith("GROUP") && l.trim() !== "");
  let totalLag = 0;
  const consumerIds = new Set<string>();
  let assignedPartitions = 0;

  for (const row of dataRows) {
    const cols = row.trim().split(/\s+/);
    // LAG is column index 5 (0-based: GROUP=0, TOPIC=1, PARTITION=2, CURRENT=3, END=4, LAG=5, CONSUMER=6)
    if (cols.length >= 7) {
      const lag = parseInt(cols[5], 10);
      if (!isNaN(lag)) totalLag += lag;
      if (cols[6] && cols[6] !== "-") {
        consumerIds.add(cols[6]);
        assignedPartitions++;
      }
    }
  }

  lines.push(log.info(`Total lag         : ${totalLag}`));
  lines.push(log.info(`Active consumers  : ${consumerIds.size}`));
  lines.push(log.info(`Assigned partitions: ${assignedPartitions}`));

  if (totalLag > 0) {
    lines.push("");
    lines.push(log.warn(`${totalLag} message(s) are waiting to be consumed.`));
  } else if (dataRows.length > 0) {
    lines.push(log.ok("Consumer group is fully caught up (lag = 0)."));
  }

  return lines.join("\n");
}
