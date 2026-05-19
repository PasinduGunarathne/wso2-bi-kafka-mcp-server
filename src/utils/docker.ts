// src/utils/docker.ts
// Wrappers around docker / docker compose CLI calls.
// Adapted from wso2-mi-kafka-mcp-server — MI-specific parts removed.

import os from "os";
import path from "path";
import { execa, ExecaError } from "execa";
import type { Subprocess } from "execa";
import * as log from "./logger.js";

/**
 * Extra bin directories prepended to PATH for every child process.
 * Covers Docker, Rancher Desktop, AND Ballerina across all platforms so that
 * tools work even when the MCP server subprocess doesn't inherit a login-shell PATH.
 */
function extraPaths(): string[] {
  const home = os.homedir();
  const paths: string[] = [
    path.join(home, ".rd", "bin"), // Rancher Desktop — all platforms
  ];

  switch (process.platform) {
    case "darwin":
      // Docker / Rancher Desktop
      paths.push("/Applications/Rancher Desktop.app/Contents/Resources/resources/darwin/bin");
      paths.push("/usr/local/bin");           // Docker Desktop (Intel) + Homebrew (Intel)
      paths.push("/opt/homebrew/bin");        // Homebrew on Apple Silicon

      // Ballerina — pkg installer puts the distribution switcher here
      paths.push("/Library/Ballerina/bin");
      // Homebrew-installed bal also lands in the paths above (/opt/homebrew/bin or /usr/local/bin)
      break;

    case "win32":
      // Docker / Rancher Desktop
      paths.push(path.join(home, "AppData", "Local", "Programs", "Rancher Desktop", "resources", "resources", "win32", "bin"));
      paths.push("C:\\Program Files\\Docker\\Docker\\resources\\bin");

      // Ballerina — the Windows installer adds to PATH automatically, but try the default location too
      paths.push("C:\\Program Files\\Ballerina\\bin");
      break;

    case "linux":
      // Docker
      paths.push("/usr/bin");
      paths.push("/usr/local/bin");
      paths.push(path.join(home, ".local", "bin"));

      // Ballerina — deb/rpm package creates /usr/bin/bal (already covered by /usr/bin above)
      // zip-based install: user must add manually, but try a common custom location
      paths.push("/usr/lib/ballerina/bin");
      break;
  }

  return paths;
}

function extendedPath(): string {
  const current = process.env.PATH ?? "";
  const dirs = extraPaths().filter((d) => !current.split(path.delimiter).includes(d));
  return dirs.length > 0 ? dirs.join(path.delimiter) + path.delimiter + current : current;
}

export interface RunResult {
  stdout: string;
  stderr: string;
  ok: boolean;
}

/** Run a command; never throws — errors are captured in RunResult. */
export async function run(
  cmd: string,
  args: string[],
  cwd?: string,
  timeoutOrEnv?: number | Record<string, string>,
  extraEnv?: Record<string, string>,
): Promise<RunResult> {
  let timeoutMs: number | undefined;
  let env: Record<string, string> = {};
  if (typeof timeoutOrEnv === "number") {
    timeoutMs = timeoutOrEnv;
    if (extraEnv) env = extraEnv;
  } else if (timeoutOrEnv && typeof timeoutOrEnv === "object") {
    env = timeoutOrEnv;
  }

  try {
    const result = await execa(cmd, args, {
      cwd: cwd || undefined,
      reject: false,
      all: true,
      env: { PATH: extendedPath(), ...env },
      timeout: timeoutMs,
    });
    return {
      stdout: String(result.stdout ?? ""),
      stderr: String(result.stderr ?? ""),
      ok: result.exitCode === 0,
    };
  } catch (e) {
    const ex = e as ExecaError;
    return { stdout: String(ex.stdout ?? ""), stderr: String(ex.stderr ?? ex.message), ok: false };
  }
}

/** Check if a binary is on PATH. */
export async function which(bin: string): Promise<boolean> {
  const cmd = process.platform === "win32" ? "where" : "which";
  const r = await run(cmd, [bin], undefined, 5_000);
  return r.ok;
}

/**
 * Check whether the Docker daemon is actually running (not just installed).
 * `docker --version` succeeds even when Docker Desktop is closed.
 * `docker info` requires the daemon to be responsive.
 */
export async function dockerDaemonRunning(): Promise<{ ok: boolean; detail: string }> {
  const r = await run("docker", ["info"], undefined, 8_000);
  if (r.ok) return { ok: true, detail: "Docker daemon is running." };

  const msg = (r.stderr + r.stdout).toLowerCase();
  if (msg.includes("cannot connect") || msg.includes("is the docker daemon running") ||
      msg.includes("no such file") || msg.includes("connection refused")) {
    return {
      ok: false,
      detail: "Docker is installed but the daemon is not running. Start Docker Desktop (or Rancher Desktop).",
    };
  }
  return { ok: false, detail: r.stderr.trim() || "docker info failed." };
}

/** docker compose up -d (no --build; the Kafka image is pre-built). */
export async function composeUp(projectDir: string, file = "docker-compose.yml"): Promise<RunResult> {
  return run("docker", ["compose", "-f", file, "up", "-d", "--remove-orphans"], projectDir, 120_000);
}

/** docker compose down -v (deletes volumes). */
export async function composeDown(projectDir: string, file = "docker-compose.yml"): Promise<RunResult> {
  return run("docker", ["compose", "-f", file, "down", "-v", "--remove-orphans"], projectDir, 60_000);
}

/** docker compose stop (preserves volumes). */
export async function composeStop(projectDir: string, file = "docker-compose.yml"): Promise<RunResult> {
  return run("docker", ["compose", "-f", file, "stop"], projectDir, 60_000);
}

/** docker compose ps --format json. */
export async function composePs(projectDir: string, file = "docker-compose.yml"): Promise<RunResult> {
  return run("docker", ["compose", "-f", file, "ps", "--format", "json"], projectDir, 15_000);
}

/** docker compose logs [--tail N] [service]. */
export async function composeLogs(
  projectDir: string,
  service?: string,
  tail = 50,
  file = "docker-compose.yml",
): Promise<RunResult> {
  const args = ["compose", "-f", file, "logs", "--tail", String(tail)];
  if (service) args.push(service);
  return run("docker", args, projectDir, 30_000);
}

/** Execute a command inside a running container. */
export async function exec(
  container: string,
  cmd: string[],
  timeoutMs = 30_000,
): Promise<RunResult> {
  return run("docker", ["exec", container, ...cmd], undefined, timeoutMs);
}

/** Execute a command inside a container with stdin piped from a string. */
export async function execWithStdin(
  container: string,
  shellCmd: string,
  stdin: string,
  timeoutMs = 30_000,
): Promise<RunResult> {
  try {
    const result = await execa(
      "docker",
      ["exec", "-i", container, "bash", "-c", shellCmd],
      {
        reject: false,
        input: stdin,
        env: { PATH: extendedPath() },
        timeout: timeoutMs,
      },
    );
    return {
      stdout: String(result.stdout ?? ""),
      stderr: String(result.stderr ?? ""),
      ok: result.exitCode === 0,
    };
  } catch (e) {
    const ex = e as ExecaError;
    return { stdout: "", stderr: String(ex.stderr ?? ex.message), ok: false };
  }
}

/**
 * Poll until a docker exec check succeeds or timeout.
 * checkCmd is run inside the named container.
 */
export async function waitUntilHealthy(
  container: string,
  checkCmd: string[],
  timeoutMs = 60_000,
  intervalMs = 3_000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const r = await run("docker", ["exec", container, ...checkCmd]);
    if (r.ok) return true;
    await sleep(intervalMs);
  }
  return false;
}

/** Resolve the actual container name for a Compose service. */
export async function resolveContainerName(
  projectDir: string,
  serviceName: string,
  file = "docker-compose.yml",
): Promise<string | null> {
  const r = await composePs(projectDir, file);
  if (!r.ok || !r.stdout.trim()) return null;

  // composePs returns one JSON object per line (NDJSON)
  for (const line of r.stdout.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const obj = JSON.parse(trimmed);
      if (obj.Service === serviceName && obj.Name) return obj.Name as string;
    } catch {
      // ignore malformed lines
    }
  }
  return null;
}

/**
 * Spawn a long-running background process and return its handle immediately
 * (not awaited). The caller stores the ExecaChildProcess and calls .kill()
 * when it wants to stop the process.
 *
 * Uses the same extended PATH as run() so `bal`, `docker`, etc. are found
 * on all platforms. cleanup:true ensures the child is killed when the MCP
 * server process exits (no zombie processes).
 */
export function spawnBackground(
  cmd: string,
  args: string[],
  cwd?: string,
  extraEnv?: Record<string, string>,
): Subprocess {
  return execa(cmd, args, {
    cwd:     cwd ?? undefined,
    env:     { PATH: extendedPath(), ...(extraEnv ?? {}) },
    reject:  false,   // never throws — errors surface via exit events
    cleanup: true,    // killed automatically when the parent process exits
    all:     true,    // merge stdout+stderr into .all stream
  });
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** HTTP GET with timeout using native fetch (Node 18+). */
export async function httpGet(url: string, timeoutMs = 5_000): Promise<RunResult> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: controller.signal });
    const body = await res.text();
    return { stdout: body, stderr: "", ok: res.ok };
  } catch (e: any) {
    return { stdout: "", stderr: e.message, ok: false };
  } finally {
    clearTimeout(timer);
  }
}
