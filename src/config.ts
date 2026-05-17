// src/config.ts
// Default paths and constants for the local Kafka + BI environment.
// Every tool accepts overrides via its arguments; these are the portable defaults
// that work on any developer's machine without prior configuration.

import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

// Resolve the project root regardless of whether we're running from src/ (tsx)
// or from dist/ (compiled). Both resolve to the same project root.
const __dirname   = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = path.resolve(__dirname, "..");

// ── WSO2 Integrator root detection ───────────────────────────────────────────

/**
 * Detect the WSO2 Integrator workspace root on the current machine.
 *
 * Convention used by WSO2 Integrator (VS Code extension):
 *   macOS / Linux : ~/WSO2Integrator/
 *   Windows       : %USERPROFILE%\WSO2Integrator\
 *
 * Returns the absolute path if it exists, or null otherwise.
 */
export function detectWso2IntegratorRoot(): string | null {
  const candidate = path.join(os.homedir(), "WSO2Integrator");
  return fs.existsSync(candidate) ? candidate : null;
}

/**
 * Resolve the best BI project path for a new project on this machine.
 *
 * Resolution order:
 *   1. If ~/WSO2Integrator/ exists  → ~/WSO2Integrator/<workspaceName>/kafkaintegration/
 *   2. Otherwise (macOS/Windows)    → ~/Documents/BallerinaProjects/<workspaceName>/kafkaintegration/
 *   3. Otherwise (Linux)            → ~/BallerinaProjects/<workspaceName>/kafkaintegration/
 *
 * @param workspaceName  Top-level workspace / project folder name.
 *                       Defaults to "kafka-bi-demo".
 */
export function resolveSmartBiProjectPath(workspaceName = "kafka-bi-demo"): string {
  const wso2Root = detectWso2IntegratorRoot();
  if (wso2Root) {
    return path.join(wso2Root, workspaceName, "kafkaintegration");
  }

  switch (process.platform) {
    case "darwin":
    case "win32":
      return path.join(os.homedir(), "Documents", "BallerinaProjects", workspaceName, "kafkaintegration");
    default: // linux + anything else
      return path.join(os.homedir(), "BallerinaProjects", workspaceName, "kafkaintegration");
  }
}

// ── Constants ─────────────────────────────────────────────────────────────────

export const DEFAULTS = {
  // Bundled Kafka Docker Compose — ships with the MCP server, works out of the box.
  // Users with an existing Kafka setup can override via kafkaComposePath argument.
  KAFKA_COMPOSE_DIR:  path.join(PROJECT_ROOT, "resources", "docker"),
  KAFKA_COMPOSE_FILE: "docker-compose.yml",

  // BI demo project — auto-located by resolveSmartBiProjectPath() at runtime.
  // Developers with an existing Ballerina project can override via biProjectPath argument.
  get BI_PROJECT_PATH() { return resolveSmartBiProjectPath(); },

  // Kafka connection
  KAFKA_BOOTSTRAP_HOST:     "localhost:9092",
  KAFKA_BOOTSTRAP_INTERNAL: "kafka:9093",
  KAFKA_UI_URL:             "http://localhost:8080",

  // Docker Compose service/container names (must match docker-compose.yml)
  KAFKA_SERVICE:    "kafka",
  KAFKA_UI_SERVICE: "kafka-ui",

  // Kafka script path inside the apache/kafka:4.0.0 image
  KAFKA_SCRIPTS: "/opt/kafka/bin",

  // Ports to check in check_prerequisites
  REQUIRED_PORTS: [9092, 8080] as number[],

  // Default sample project output location (generate_bi_kafka_sample)
  SAMPLE_OUTPUT_BASE: path.join(os.homedir(), "bi-kafka-samples"),
} as const;

/** Resolve the Compose directory from an optional override path. */
export function resolveComposeDir(override?: string): string {
  return override ?? DEFAULTS.KAFKA_COMPOSE_DIR;
}

/** Resolve the BI project path from an optional override. */
export function resolveBiProjectPath(override?: string): string {
  return override ?? DEFAULTS.BI_PROJECT_PATH;
}
