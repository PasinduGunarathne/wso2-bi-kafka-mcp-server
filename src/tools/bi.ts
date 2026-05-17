// src/tools/bi.ts
// BI / Ballerina tools: validate_bi_project, run_bi_project,
//   inspect_bi_kafka_config, generate_bi_kafka_sample.

import fs from "fs";
import path from "path";
import * as docker from "../utils/docker.js";
import * as log from "../utils/logger.js";
import { requireString, optionalString, maskSecret } from "../utils/validation.js";
import { DEFAULTS, resolveBiProjectPath } from "../config.js";

// ── Helpers ──────────────────────────────────────────────────────────────────

function assertBiProject(biPath: string): void {
  if (!fs.existsSync(biPath)) {
    throw new Error(`BI project directory not found: ${biPath}`);
  }
  const toml = path.join(biPath, "Ballerina.toml");
  if (!fs.existsSync(toml)) {
    throw new Error(`Ballerina.toml not found in: ${biPath}. Is this a Ballerina project?`);
  }
}

/** Extract `configurable TYPE NAME = DEFAULT;` lines from a .bal file. */
function parseConfigurables(src: string): Array<{ name: string; type: string; defaultValue: string }> {
  const result: Array<{ name: string; type: string; defaultValue: string }> = [];
  // Matches: configurable string foo = "bar"; or configurable int count = 5;
  const re = /^\s*configurable\s+(\S+)\s+(\w+)\s*=\s*(.+?)\s*;/gm;
  let m: RegExpExecArray | null;
  while ((m = re.exec(src)) !== null) {
    result.push({ name: m[2], type: m[1], defaultValue: m[3].replace(/^"|"$/g, "") });
  }
  return result;
}

/** Parse `[package]` section from Ballerina.toml (simple key=value). */
function parseTomlPackage(src: string): Record<string, string> {
  const result: Record<string, string> = {};
  let inPackage = false;
  for (const line of src.split("\n")) {
    const trimmed = line.trim();
    if (trimmed === "[package]") { inPackage = true; continue; }
    if (trimmed.startsWith("[") && trimmed !== "[package]") { inPackage = false; continue; }
    if (inPackage) {
      const eq = trimmed.indexOf("=");
      if (eq !== -1) {
        const key = trimmed.slice(0, eq).trim();
        const val = trimmed.slice(eq + 1).trim().replace(/^"|"$/g, "");
        result[key] = val;
      }
    }
  }
  return result;
}

// ── validate_bi_project ───────────────────────────────────────────────────────

export async function validateBiProject(args: { biProjectPath?: string }): Promise<string> {
  const biPath = resolveBiProjectPath(args.biProjectPath);
  const lines: string[] = [log.header("Validate BI Project (bal build)")];

  lines.push(log.info(`Project path: ${biPath}`));
  lines.push("");

  assertBiProject(biPath);

  lines.push(log.run("Running 'bal build'... (this may take up to 2 minutes on first run)"));

  const r = await docker.run("bal", ["build"], biPath, 180_000);

  const combinedOutput = [r.stdout, r.stderr].filter(Boolean).join("\n").trim();

  lines.push("");
  if (combinedOutput) {
    lines.push("Build output:");
    lines.push("─".repeat(64));
    lines.push(combinedOutput);
    lines.push("─".repeat(64));
  }

  lines.push("");
  if (r.ok) {
    lines.push(log.ok("Build succeeded."));
    lines.push(log.info("Run the project with: run_bi_project"));
  } else {
    lines.push(log.err("Build failed. Review the errors above."));
    const errorLines = combinedOutput
      .split("\n")
      .filter((l) => l.includes("error:") || l.includes("ERROR"));
    if (errorLines.length > 0) {
      lines.push("");
      lines.push("Compilation errors:");
      for (const el of errorLines.slice(0, 20)) {
        lines.push(`  ${el.trim()}`);
      }
    }
  }

  return lines.join("\n");
}

// ── run_bi_project ────────────────────────────────────────────────────────────

export async function runBiProject(args: { biProjectPath?: string }): Promise<string> {
  const biPath = resolveBiProjectPath(args.biProjectPath);
  const lines: string[] = [log.header("Run BI Project (bal run)")];

  lines.push(log.info(`Project path: ${biPath}`));
  lines.push("");

  assertBiProject(biPath);

  // Safety: never overwrite Config.toml
  const configToml = path.join(biPath, "Config.toml");
  if (fs.existsSync(configToml)) {
    lines.push(log.info("Config.toml found — using existing configuration (not modified)."));
  }

  lines.push(log.run("Starting 'bal run'..."));
  lines.push(log.info("The Kafka listener runs indefinitely. Capturing startup output (15s)..."));
  lines.push("");

  // Run with a fixed timeout to capture startup output; the process will be
  // killed after the timeout, which is expected behaviour for a listener service.
  const r = await docker.run("bal", ["run"], biPath, 15_000);

  const combinedOutput = [r.stdout, r.stderr].filter(Boolean).join("\n").trim();

  if (combinedOutput) {
    lines.push("Startup output:");
    lines.push("─".repeat(64));
    lines.push(combinedOutput);
    lines.push("─".repeat(64));
    lines.push("");
  }

  // `bal run` for a listener exits only on timeout or error.
  // A timeout exit is normal; an error exit (e.g. compilation error, port conflict) is not.
  const isCompileError = combinedOutput.includes("error:") || combinedOutput.includes("compilation failed");
  const isPortConflict = combinedOutput.toLowerCase().includes("address already in use");

  if (isCompileError) {
    lines.push(log.err("Compilation errors detected. Run validate_bi_project to see full errors."));
  } else if (isPortConflict) {
    lines.push(log.err("Port conflict: another process may already be listening."));
  } else {
    lines.push(log.ok("BI project started successfully (15s startup window captured)."));
    lines.push(log.info("The Kafka listener is running in the background."));
    lines.push(log.info("To run it manually in a terminal:"));
    lines.push(`    cd ${biPath} && bal run`);
  }

  return lines.join("\n");
}

// ── inspect_bi_kafka_config ───────────────────────────────────────────────────

export async function inspectBiKafkaConfig(args: { biProjectPath?: string }): Promise<string> {
  const biPath = resolveBiProjectPath(args.biProjectPath);
  const lines: string[] = [log.header("BI Kafka Configuration")];

  lines.push(log.info(`Project path: ${biPath}`));
  lines.push("");

  assertBiProject(biPath);

  // ── Ballerina.toml ────────────────────────────────────────────────────────
  const ballerinaTomlPath = path.join(biPath, "Ballerina.toml");
  const ballerinaTomlSrc  = fs.readFileSync(ballerinaTomlPath, "utf8");
  const pkg = parseTomlPackage(ballerinaTomlSrc);

  lines.push("Package:");
  lines.push(`  org     : ${pkg["org"] ?? "(not set)"}`);
  lines.push(`  name    : ${pkg["name"] ?? "(not set)"}`);
  lines.push(`  version : ${pkg["version"] ?? "(not set)"}`);
  if (pkg["distribution"]) lines.push(`  dist    : ${pkg["distribution"]}`);
  lines.push("");

  // ── config.bal — configurable declarations ────────────────────────────────
  const configBalPath = path.join(biPath, "config.bal");
  const configurables: Array<{ name: string; type: string; defaultValue: string }> = [];

  if (fs.existsSync(configBalPath)) {
    const src = fs.readFileSync(configBalPath, "utf8");
    configurables.push(...parseConfigurables(src));
  }

  // Also scan other .bal files for any additional configurables
  const balFiles = fs.readdirSync(biPath).filter((f) => f.endsWith(".bal") && f !== "config.bal");
  for (const f of balFiles) {
    const src = fs.readFileSync(path.join(biPath, f), "utf8");
    const found = parseConfigurables(src);
    for (const c of found) {
      if (!configurables.find((e) => e.name === c.name)) {
        configurables.push(c);
      }
    }
  }

  if (configurables.length > 0) {
    lines.push("Configurable Parameters (from .bal files):");
    lines.push("─".repeat(64));
    lines.push(
      `  ${"Name".padEnd(30)} ${"Type".padEnd(12)} Default`,
    );
    lines.push("─".repeat(64));
    for (const c of configurables) {
      const maskedDefault = maskSecret(c.name, c.defaultValue);
      lines.push(`  ${c.name.padEnd(30)} ${c.type.padEnd(12)} ${maskedDefault}`);
    }
    lines.push("─".repeat(64));
    lines.push("");
  } else {
    lines.push(log.warn("No configurable parameters found in .bal files."));
    lines.push("");
  }

  // ── Config.toml — current overrides ──────────────────────────────────────
  const configTomlPath = path.join(biPath, "Config.toml");
  if (fs.existsSync(configTomlPath)) {
    lines.push("Config.toml (active overrides — secrets masked):");
    lines.push("─".repeat(64));
    const tomlSrc = fs.readFileSync(configTomlPath, "utf8");
    for (const line of tomlSrc.split("\n")) {
      const eq = line.indexOf("=");
      if (eq !== -1) {
        const key = line.slice(0, eq).trim();
        const val = line.slice(eq + 1).trim().replace(/^"|"$/g, "");
        lines.push(`  ${key} = ${maskSecret(key, val)}`);
      } else {
        lines.push(`  ${line}`);
      }
    }
    lines.push("─".repeat(64));
  } else {
    lines.push(log.info("No Config.toml found — defaults from .bal files are in effect."));
    lines.push(log.info("Create a Config.toml to override values without modifying source."));
    lines.push("");
    lines.push("Example Config.toml:");
    lines.push("─".repeat(64));
    for (const c of configurables) {
      lines.push(`${c.name} = "${c.defaultValue}"`);
    }
    lines.push("─".repeat(64));
  }

  lines.push("");
  lines.push(log.info("Override values using Config.toml or -C flags: bal run -C kafkaTopic=my-topic"));

  return lines.join("\n");
}

// ── generate_bi_kafka_sample ──────────────────────────────────────────────────

const BALLERINA_TOML_TEMPLATE = (org: string, name: string) => `\
[package]
org = "${org}"
name = "${name}"
version = "0.1.0"
distribution = "2201.13.4"

[build-options]
sticky = true
`;

const CONFIG_BAL_TEMPLATE = (bootstrapServers: string, groupId: string, topicName: string) => `\
import ballerina/log;

configurable string kafkaBootstrapServers = "${bootstrapServers}";
configurable string kafkaGroupId          = "${groupId}";
configurable string kafkaTopic            = "${topicName}";
`;

const TYPES_BAL_TEMPLATE = () => `\
// Domain types for Kafka message payloads.

public type Event record {|
    string id;
    string type;
    string timestamp;
    map<string> data?;
|};
`;

const CONNECTIONS_BAL_TEMPLATE = () => `\
import ballerinax/kafka;

// Global Kafka producer — reused across calls.
final kafka:Producer kafkaProducer = check new (string \`\${kafkaBootstrapServers}\`);
`;

const MAIN_BAL_TEMPLATE = (topicName: string) => `\
import ballerina/log;
import ballerinax/kafka;

// Kafka listener — starts polling when the service is attached.
listener kafka:Listener kafkaListener = new (kafkaBootstrapServers, {
    groupId: kafkaGroupId,
    topics: [kafkaTopic],
    offsetReset: kafka:OFFSET_RESET_LATEST,
    pollingInterval: 1,
    autoCommit: false
});

service kafka:Service on kafkaListener {
    remote function onConsumerRecord(kafka:AnydataConsumerRecord[] messages, kafka:Caller caller) returns error? {
        foreach kafka:AnydataConsumerRecord msg in messages {
            do {
                byte[] rawBytes = check msg.value.ensureType(byte[]);
                string payload  = check string:fromBytes(rawBytes);
                log:printInfo("Received message", topic = "${topicName}", payload = payload);
                check processEvent(payload);
            } on fail error e {
                log:printError("Failed to process message", err = e, offset = msg.offset);
            }
        }
        check caller->'commit();
    }
}
`;

const FUNCTIONS_BAL_TEMPLATE = () => `\
import ballerina/log;

public isolated function processEvent(string payload) returns error? {
    // TODO: implement your business logic here
    log:printInfo("Processing event", payload = payload);
}
`;

export async function generateBiKafkaSample(args: {
  targetPath: string;
  packageName?: string;
  orgName?: string;
  topicName?: string;
  groupId?: string;
  bootstrapServers?: string;
}): Promise<string> {
  const lines: string[] = [log.header("Generate BI Kafka Sample Project")];

  // targetPath is required
  const targetPath = requireString(args.targetPath, "targetPath");

  if (!path.isAbsolute(targetPath)) {
    throw new Error(`'targetPath' must be an absolute path. Got: ${targetPath}`);
  }

  // Safety: refuse to write inside the existing BI project
  const existingBiPath = resolveBiProjectPath();
  if (targetPath.startsWith(existingBiPath)) {
    throw new Error(
      `'targetPath' must not be inside the existing BI project (${existingBiPath}). ` +
      `Choose a different directory.`,
    );
  }

  const packageName    = optionalString(args.packageName, "kafkasample");
  const orgName        = optionalString(args.orgName, "myorg");
  const topicName      = optionalString(args.topicName, "events");
  const groupId        = optionalString(args.groupId, `${packageName}-consumer`);
  const bootstrapServers = optionalString(args.bootstrapServers, DEFAULTS.KAFKA_BOOTSTRAP_HOST);

  lines.push(log.info(`Output directory  : ${targetPath}`));
  lines.push(log.info(`Package           : ${orgName}/${packageName}`));
  lines.push(log.info(`Topic             : ${topicName}`));
  lines.push(log.info(`Group ID          : ${groupId}`));
  lines.push(log.info(`Bootstrap servers : ${bootstrapServers}`));
  lines.push("");

  // Create target directory
  fs.mkdirSync(targetPath, { recursive: true });

  const files: Array<{ name: string; content: string }> = [
    { name: "Ballerina.toml", content: BALLERINA_TOML_TEMPLATE(orgName, packageName) },
    { name: "config.bal",     content: CONFIG_BAL_TEMPLATE(bootstrapServers, groupId, topicName) },
    { name: "types.bal",      content: TYPES_BAL_TEMPLATE() },
    { name: "connections.bal",content: CONNECTIONS_BAL_TEMPLATE() },
    { name: "main.bal",       content: MAIN_BAL_TEMPLATE(topicName) },
    { name: "functions.bal",  content: FUNCTIONS_BAL_TEMPLATE() },
  ];

  lines.push(log.run("Writing files..."));
  const generated: string[] = [];

  for (const file of files) {
    const filePath = path.join(targetPath, file.name);
    fs.writeFileSync(filePath, file.content, "utf8");
    generated.push(filePath);
    lines.push(log.ok(`  ${file.name}`));
  }

  lines.push("");
  lines.push(log.done("Sample project generated."));
  lines.push("");
  lines.push("Next steps:");
  lines.push(`  cd ${targetPath}`);
  lines.push("  bal build          # compile the project");
  lines.push("  bal run            # start the Kafka listener");
  lines.push(`  bal run -C kafkaTopic=other-topic   # override topic at runtime`);
  lines.push("");
  lines.push(log.info("The listener uses manual offset commit (autoCommit: false)."));
  lines.push(log.info("Edit functions.bal to implement your business logic."));

  return lines.join("\n");
}
