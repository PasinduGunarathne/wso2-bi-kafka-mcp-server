// src/tools/prerequisites.ts
// check_prerequisites — verify Docker (installed + daemon running), Compose v2,
// Node.js, Ballerina CLI, project paths, and required ports.
// When anything is missing, returns platform-specific install instructions.

import fs from "fs";
import net from "net";
import * as docker from "../utils/docker.js";
import * as log from "../utils/logger.js";
import {
  DEFAULTS,
  resolveComposeDir,
  resolveBiProjectPath,
  detectWso2IntegratorRoot,
  resolveSmartBiProjectPath,
} from "../config.js";
import type { PrerequisiteCheck } from "../types.js";

// ── Platform detection ────────────────────────────────────────────────────────

type Platform = "macos" | "linux" | "windows" | "unknown";

function currentPlatform(): Platform {
  switch (process.platform) {
    case "darwin": return "macos";
    case "linux":  return "linux";
    case "win32":  return "windows";
    default:       return "unknown";
  }
}

// ── Install / fix notes (platform-aware) ─────────────────────────────────────

function dockerInstallNote(platform: Platform): string {
  const lines = [
    "Docker is required to run the Kafka broker in a container.",
    "",
  ];
  switch (platform) {
    case "macos":
      lines.push(
        "  Option A — Docker Desktop (GUI, easiest):",
        "    https://www.docker.com/products/docker-desktop/",
        "    Or via Homebrew:  brew install --cask docker",
        "",
        "  Option B — Rancher Desktop (open-source alternative):",
        "    https://rancherdesktop.io/",
        "    Or via Homebrew:  brew install --cask rancher",
        "",
        "  After installing, open the app and wait for the engine status to go green.",
        "  Verify:  docker --version",
      );
      break;
    case "linux":
      lines.push(
        "  Option A — Official install script (recommended):",
        "    curl -fsSL https://get.docker.com | sh",
        "    sudo usermod -aG docker $USER   # run docker without sudo",
        "    newgrp docker                   # apply group change in current shell",
        "",
        "  Option B — Rancher Desktop:",
        "    https://rancherdesktop.io/",
        "",
        "  Verify:  docker --version",
      );
      break;
    case "windows":
      lines.push(
        "  Option A — Docker Desktop (requires WSL 2):",
        "    https://www.docker.com/products/docker-desktop/",
        "    Pre-requisite: WSL 2 — run in PowerShell (admin):  wsl --install",
        "",
        "  Option B — Rancher Desktop:",
        "    https://rancherdesktop.io/",
        "",
        "  Verify (PowerShell):  docker --version",
      );
      break;
    default:
      lines.push("  Download: https://docs.docker.com/get-docker/");
  }
  return lines.join("\n");
}

function dockerDaemonNote(platform: Platform): string {
  const lines = [
    "Docker is installed but the daemon is not running.",
    "You need to start Docker Desktop (or Rancher Desktop) before using this MCP server.",
    "",
  ];
  switch (platform) {
    case "macos":
      lines.push(
        "  Start Docker Desktop:",
        "    Open Applications → Docker   (or Rancher Desktop)",
        "    Wait for the whale icon in the menu bar to stop animating.",
        "",
        "  Or via CLI:  open -a Docker",
        "",
        "  Verify:  docker info",
      );
      break;
    case "linux":
      lines.push(
        "  Start the Docker service:",
        "    sudo systemctl start docker",
        "    sudo systemctl enable docker   # auto-start on boot",
        "",
        "  Verify:  docker info",
      );
      break;
    case "windows":
      lines.push(
        "  Start Docker Desktop from the Start menu or system tray.",
        "  Wait for the Docker icon in the taskbar to show 'Docker Desktop is running'.",
        "",
        "  Verify (PowerShell):  docker info",
      );
      break;
    default:
      lines.push("  Start your Docker daemon and verify with:  docker info");
  }
  return lines.join("\n");
}

function dockerComposeInstallNote(platform: Platform): string {
  const lines = [
    "Docker Compose v2 (the 'docker compose' plugin) is required to manage the Kafka stack.",
    "Note: use 'docker compose' (space) not 'docker-compose' (hyphen).",
    "",
  ];
  switch (platform) {
    case "macos":
      lines.push(
        "  Docker Compose v2 is bundled with Docker Desktop 4.x+.",
        "  Update Docker Desktop:  Docker Desktop menu → Check for Updates",
        "",
        "  Verify:  docker compose version",
      );
      break;
    case "linux":
      lines.push(
        "  Option A — Package manager:",
        "    Ubuntu/Debian:  sudo apt-get install docker-compose-plugin",
        "    Fedora/RHEL:    sudo dnf install docker-compose-plugin",
        "",
        "  Option B — Manual binary install:",
        "    VER=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep tag_name | cut -d'\"' -f4)",
        `    sudo curl -SL "https://github.com/docker/compose/releases/download/\${VER}/docker-compose-linux-$(uname -m)" \\`,
        "         -o /usr/local/lib/docker/cli-plugins/docker-compose",
        "    sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose",
        "",
        "  Verify:  docker compose version",
      );
      break;
    case "windows":
      lines.push(
        "  Docker Compose v2 is bundled with Docker Desktop 4.x+.",
        "  Update Docker Desktop:  Docker Desktop → Settings → Software updates",
        "",
        "  Verify:  docker compose version",
      );
      break;
    default:
      lines.push("  See: https://docs.docker.com/compose/install/");
  }
  return lines.join("\n");
}

function nodeInstallNote(platform: Platform): string {
  const lines = [
    "Node.js 18+ is required to run this MCP server.",
    "",
  ];
  switch (platform) {
    case "macos":
      lines.push(
        "  Option A — Homebrew (recommended):",
        "    brew install node",
        "",
        "  Option B — Official LTS installer (.pkg):",
        "    https://nodejs.org/en/download/",
        "",
        "  Option C — NVM (manage multiple versions):",
        "    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash",
        "    source ~/.zshrc   # or ~/.bashrc",
        "    nvm install --lts",
        "",
        "  Verify:  node --version   (must be v18+)",
      );
      break;
    case "linux":
      lines.push(
        "  Option A — NodeSource LTS repository:",
        "    curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -",
        "    sudo apt-get install -y nodejs",
        "",
        "  Option B — NVM:",
        "    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash",
        "    source ~/.bashrc",
        "    nvm install --lts",
        "",
        "  Verify:  node --version",
      );
      break;
    case "windows":
      lines.push(
        "  Option A — Official LTS installer (.msi):",
        "    https://nodejs.org/en/download/",
        "",
        "  Option B — Chocolatey:  choco install nodejs-lts",
        "",
        "  Option C — NVM for Windows:",
        "    https://github.com/coreybutler/nvm-windows/releases",
        "    nvm install lts && nvm use lts",
        "",
        "  Verify:  node --version",
      );
      break;
    default:
      lines.push("  Download: https://nodejs.org/en/download/");
  }
  return lines.join("\n");
}

function ballerinaInstallNote(platform: Platform): string {
  const lines = [
    "The Ballerina CLI (bal) is required to build and run the BI integration project.",
    "Install Swan Lake Update 12 (2201.12.x) or later.",
    "",
  ];
  switch (platform) {
    case "macos":
      lines.push(
        "  Option A — Homebrew (recommended):",
        "    brew install ballerina",
        "",
        "  Option B — Official macOS installer (.pkg):",
        "    https://ballerina.io/downloads/",
        "    Download the .pkg and run it.",
        "",
        "  After installing, open a NEW terminal window (PATH is updated in /etc/paths.d/).",
        "  Verify:  bal version",
      );
      break;
    case "linux":
      lines.push(
        "  Option A — Debian/Ubuntu (.deb):",
        "    wget https://dist.ballerina.io/downloads/<version>/ballerina-<version>-swan-lake-linux-x64.deb",
        "    sudo dpkg -i ballerina-<version>-swan-lake-linux-x64.deb",
        "    (Get the latest version from https://ballerina.io/downloads/)",
        "",
        "  Option B — RPM (Fedora/RHEL):",
        "    wget https://dist.ballerina.io/downloads/<version>/ballerina-<version>-swan-lake-linux-x64.rpm",
        "    sudo rpm -i ballerina-<version>-swan-lake-linux-x64.rpm",
        "",
        "  Verify:  bal version",
      );
      break;
    case "windows":
      lines.push(
        "  Download the Windows installer (.msi):",
        "    https://ballerina.io/downloads/",
        "    Run the .msi and follow the wizard.",
        "",
        "  After installing, open a NEW PowerShell / CMD window.",
        "  Verify:  bal version",
      );
      break;
    default:
      lines.push("  Download: https://ballerina.io/downloads/");
  }
  return lines.join("\n");
}

function composeFileMissingNote(composeFile: string): string {
  return [
    `Expected Kafka docker-compose.yml not found at:`,
    `  ${composeFile}`,
    "",
    "This file ships with the MCP server inside resources/docker/.",
    "If it is missing, the repository may be incomplete.",
    "",
    "Fix options:",
    "",
    "  Option A — Re-clone or re-download the MCP server repository.",
    "",
    "  Option B — Point to your own existing Kafka Compose directory:",
    `    setup_kafka_and_bi { "kafkaComposePath": "/path/to/your/kafka-dir" }`,
    `    check_prerequisites { "kafkaComposePath": "/path/to/your/kafka-dir" }`,
  ].join("\n");
}

function biProjectMissingNote(biPath: string): string {
  return [
    `BI project directory not found at: ${biPath}`,
    "",
    "This is expected on a fresh install — the project is generated automatically.",
    "",
    "Fix: Run setup_kafka_and_bi to generate the full BI demo project:",
    `    setup_kafka_and_bi`,
    `    (or with a custom path: setup_kafka_and_bi { "projectPath": "${biPath}" })`,
  ].join("\n");
}

function portConflictNote(port: number, platform: Platform): string {
  const lines = [
    `Port ${port} is already in use by another process.`,
    "This will prevent Kafka from starting on this port.",
    "",
    "Find and stop the process using this port:",
    "",
  ];
  switch (platform) {
    case "macos":
    case "linux":
      lines.push(
        `  # Find the process:`,
        `  lsof -i :${port}`,
        `  # or:`,
        `  ss -tulpn | grep :${port}`,
        "",
        `  # Stop it (replace <PID> with the actual process ID):`,
        `  kill -9 <PID>`,
      );
      break;
    case "windows":
      lines.push(
        `  # Find the process (PowerShell):`,
        `  netstat -ano | findstr :${port}`,
        "",
        `  # Stop it (replace <PID> with the process ID from above):`,
        `  taskkill /PID <PID> /F`,
      );
      break;
    default:
      lines.push(`  Find and stop the process listening on port ${port}.`);
  }
  lines.push(
    "",
    "  If it is Kafka from a previous run, stop it with:  stop_kafka",
    "  Then re-run:  setup_kafka_and_bi",
  );
  return lines.join("\n");
}

// ── Port check ────────────────────────────────────────────────────────────────

async function checkPort(port: number): Promise<{ inUse: boolean }> {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.once("error", (err: NodeJS.ErrnoException) => {
      resolve({ inUse: err.code === "EADDRINUSE" });
    });
    server.once("listening", () => {
      server.close(() => resolve({ inUse: false }));
    });
    server.listen(port, "127.0.0.1");
  });
}

// ── Shared checker used by setup.ts ──────────────────────────────────────────

/** Run all prerequisite checks and return the raw result list. */
export async function runPrerequisiteChecks(args: {
  biProjectPath?: string;
  kafkaComposePath?: string;
}): Promise<PrerequisiteCheck[]> {
  const composeDir  = resolveComposeDir(args.kafkaComposePath);
  const biPath      = resolveBiProjectPath(args.biProjectPath);
  const composeFile = `${composeDir}/${DEFAULTS.KAFKA_COMPOSE_FILE}`;
  const platform    = currentPlatform();
  const checks: PrerequisiteCheck[] = [];

  // ── 1. Docker installed ──────────────────────────────────────────────────
  const dockerVer = await docker.run("docker", ["--version"], undefined, 5_000);
  const dockerInstalled = dockerVer.ok;
  checks.push({
    name: "Docker installed",
    ok: dockerInstalled,
    detail: dockerInstalled ? dockerVer.stdout.trim() : "Docker CLI not found.",
    installNote: dockerInstalled ? undefined : dockerInstallNote(platform),
  });

  // ── 2. Docker daemon running (only if installed) ─────────────────────────
  const daemonResult = dockerInstalled
    ? await docker.dockerDaemonRunning()
    : { ok: false, detail: "Skipped — Docker is not installed." };
  checks.push({
    name: "Docker daemon running",
    ok: daemonResult.ok,
    detail: daemonResult.ok ? "Docker daemon is responsive." : daemonResult.detail,
    installNote: (!daemonResult.ok && dockerInstalled)
      ? dockerDaemonNote(platform)
      : undefined,
  });

  // ── 3. Docker Compose v2 ─────────────────────────────────────────────────
  const composeVer = await docker.run("docker", ["compose", "version"], undefined, 5_000);
  checks.push({
    name: "Docker Compose v2",
    ok: composeVer.ok,
    detail: composeVer.ok ? composeVer.stdout.trim() : "Docker Compose v2 plugin not found.",
    installNote: composeVer.ok ? undefined : dockerComposeInstallNote(platform),
  });

  // ── 4. Node.js ───────────────────────────────────────────────────────────
  const nodeVer = await docker.run("node", ["--version"], undefined, 5_000);
  checks.push({
    name: "Node.js (≥18)",
    ok: nodeVer.ok,
    detail: nodeVer.ok ? nodeVer.stdout.trim() : "Node.js not found.",
    installNote: nodeVer.ok ? undefined : nodeInstallNote(platform),
  });

  // ── 5. Ballerina CLI ─────────────────────────────────────────────────────
  const balVer = await docker.run("bal", ["version"], undefined, 10_000);
  checks.push({
    name: "Ballerina CLI (bal)",
    ok: balVer.ok,
    detail: balVer.ok
      ? balVer.stdout.split("\n")[0].trim()
      : "Ballerina CLI not found.",
    installNote: balVer.ok ? undefined : ballerinaInstallNote(platform),
  });

  // ── 6. Kafka docker-compose.yml ──────────────────────────────────────────
  const composeExists = fs.existsSync(composeFile);
  checks.push({
    name: "Kafka docker-compose.yml",
    ok: composeExists,
    detail: composeExists ? composeFile : `Not found: ${composeFile}`,
    installNote: composeExists ? undefined : composeFileMissingNote(composeFile),
  });

  // ── 7. BI project directory ──────────────────────────────────────────────
  const biExists = fs.existsSync(biPath);
  checks.push({
    name: "BI project directory",
    ok: biExists,
    detail: biExists ? biPath : `Not found: ${biPath}`,
    installNote: biExists ? undefined : biProjectMissingNote(biPath),
  });

  // ── 8. Ballerina.toml in BI project ─────────────────────────────────────
  const tomlPath   = `${biPath}/Ballerina.toml`;
  const tomlExists = biExists && fs.existsSync(tomlPath);
  checks.push({
    name: "Ballerina.toml in BI project",
    ok: tomlExists,
    detail: tomlExists
      ? tomlPath
      : biExists
        ? `Missing in ${biPath}`
        : "Follows from BI project directory — fix that first.",
    installNote:
      !tomlExists
        ? biExists
          // Project dir exists but no Ballerina.toml — regenerate
          ? [
              `Ballerina.toml is missing from: ${biPath}`,
              "",
              "Run setup_kafka_and_bi to regenerate the project:",
              `    setup_kafka_and_bi { "projectPath": "${biPath}" }`,
            ].join("\n")
          // Project dir doesn't exist — cascading; point to the parent fix
          : "Resolve the 'BI project directory' issue above — this will be fixed automatically."
        : undefined,
  });

  // ── 9. Required ports (blocker if in use by a non-Kafka process) ─────────
  for (const port of DEFAULTS.REQUIRED_PORTS) {
    const label  = port === 9092 ? "9092 (Kafka broker)" : `${port} (Kafka UI)`;
    const result = await checkPort(port);

    if (result.inUse) {
      // The port is in use — it might be Kafka already running (OK) or a conflict.
      // We flag it as a warning (ok=false) with resolution guidance.
      checks.push({
        name: `Port ${label}`,
        ok: false,
        detail: `Port ${port} is already in use. Kafka may already be running, or another process is occupying this port.`,
        installNote: portConflictNote(port, platform),
      });
    } else {
      checks.push({
        name: `Port ${label}`,
        ok: true,
        detail: "Available",
      });
    }
  }

  return checks;
}

// ── Environment layout block ──────────────────────────────────────────────────

interface EnvLayoutArgs {
  platform: Platform;
  wso2Root: string | null;
  smartPath: string;
  effectiveBiPath: string;
  usingOverride: boolean;
  overridePath?: string;
}

/**
 * Build a human-readable block that tells the user exactly where new BI projects
 * will be created on their machine, and how to change it.
 */
function buildEnvLayoutBlock(a: EnvLayoutArgs): string[] {
  const lines: string[] = [];

  lines.push("┌─────────────────────────────────────────────────────────────┐");
  lines.push("│            🗂  Environment Layout                           │");
  lines.push("└─────────────────────────────────────────────────────────────┘");
  lines.push("");

  // ── WSO2 Integrator detection ────────────────────────────────────────────
  if (a.wso2Root) {
    lines.push(`  ✅  WSO2 Integrator root detected: ${a.wso2Root}`);
    lines.push(`       New BI projects will be created inside this workspace.`);
  } else {
    lines.push(`  ℹ️   WSO2 Integrator root not found (~/WSO2Integrator/ missing).`);
    switch (a.platform) {
      case "macos":
      case "windows":
        lines.push(`       Using Documents/BallerinaProjects/ as the default workspace.`);
        break;
      default:
        lines.push(`       Using ~/BallerinaProjects/ as the default workspace.`);
    }
    lines.push(`       (Install WSO2 Integrator to auto-place projects there instead.)`);
  }

  lines.push("");

  // ── Effective project path ────────────────────────────────────────────────
  if (a.usingOverride) {
    lines.push(`  📌  BI project path  (override): ${a.overridePath}`);
  } else {
    lines.push(`  📁  BI project path  (auto-detected): ${a.smartPath}`);
  }

  lines.push("");

  // ── How to use an existing project ───────────────────────────────────────
  lines.push(`  💡  Using an existing Ballerina project? Pass its path directly:`);
  lines.push(`       check_prerequisites { "biProjectPath": "/path/to/your/project" }`);
  lines.push(`       setup_kafka_and_bi  { "projectPath":   "/path/to/your/project" }`);

  lines.push("");

  // ── OS-specific path cheat-sheet ─────────────────────────────────────────
  lines.push(`  📋  Default paths per platform:`);
  lines.push(`       macOS / Windows (with WSO2I)  : ~/WSO2Integrator/<name>/kafkaintegration/`);
  lines.push(`       macOS / Windows (without WSO2I): ~/Documents/BallerinaProjects/<name>/kafkaintegration/`);
  lines.push(`       Linux (with WSO2I)            : ~/WSO2Integrator/<name>/kafkaintegration/`);
  lines.push(`       Linux (without WSO2I)         : ~/BallerinaProjects/<name>/kafkaintegration/`);

  return lines;
}

// ── check_prerequisites (MCP tool handler) ────────────────────────────────────

export async function checkPrerequisites(args: {
  biProjectPath?: string;
  kafkaComposePath?: string;
}): Promise<string> {
  const composeDir = resolveComposeDir(args.kafkaComposePath);
  const biPath     = resolveBiProjectPath(args.biProjectPath);
  const platform   = currentPlatform();

  // ── Environment layout detection ──────────────────────────────────────────
  const wso2Root      = detectWso2IntegratorRoot();
  const smartPath     = resolveSmartBiProjectPath("kafka-bi-demo");
  const usingOverride = !!args.biProjectPath;

  const envLayoutLines = buildEnvLayoutBlock({
    platform,
    wso2Root,
    smartPath,
    effectiveBiPath: biPath,
    usingOverride,
    overridePath: args.biProjectPath,
  });

  const lines: string[] = [
    log.header("Prerequisite Check — Kafka + BI Environment"),
    "",
    log.info(`Platform          : ${platform}`),
    log.info(`Kafka Compose dir : ${composeDir}`),
    log.info(`BI project path   : ${biPath}`),
    "",
    ...envLayoutLines,
    "",
  ];

  const checks = await runPrerequisiteChecks(args);

  // Status table
  for (const c of checks) {
    lines.push(c.ok ? log.ok(c.name) : log.err(c.name));
    lines.push(`    ${c.detail}`);
  }

  const failed = checks.filter((c) => !c.ok);
  lines.push("");

  if (failed.length === 0) {
    lines.push(log.done(
      "All prerequisites satisfied. " +
      "Run setup_kafka_and_bi to get started.",
    ));
    lines.push("");
    lines.push(log.info(`Bootstrap server : ${DEFAULTS.KAFKA_BOOTSTRAP_HOST}`));
    lines.push(log.info(`Kafka UI         : ${DEFAULTS.KAFKA_UI_URL}`));
    return lines.join("\n");
  }

  // ── Install / fix guides for each failure ────────────────────────────────
  lines.push(log.warn(`${failed.length} issue(s) found. Fix guides below.`));
  lines.push("");
  lines.push("═".repeat(62));

  for (const f of failed) {
    lines.push("");
    lines.push(`❌  ${f.name.toUpperCase()}`);
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
    "After fixing, open a new terminal (so PATH changes take effect),",
  ));
  lines.push(log.info(
    "then re-run check_prerequisites to confirm everything passes.",
  ));
  lines.push("");
  lines.push(log.info("Once all checks pass, run:  setup_kafka_and_bi"));

  return lines.join("\n");
}
