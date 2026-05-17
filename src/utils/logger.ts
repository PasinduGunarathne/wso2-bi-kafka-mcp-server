// src/utils/logger.ts
// Pretty-print progress steps for MCP tool output.

export function step(n: number, total: number, msg: string): string {
  return `[${n}/${total}] ${msg}`;
}

export function header(title: string): string {
  const line = "═".repeat(60);
  return `\n╔${line}╗\n║  ${title.padEnd(58)}║\n╚${line}╝`;
}

export function ok(msg: string): string   { return `✅  ${msg}`; }
export function warn(msg: string): string  { return `⚠️   ${msg}`; }
export function err(msg: string): string   { return `❌  ${msg}`; }
export function info(msg: string): string  { return `ℹ️   ${msg}`; }
export function run(msg: string): string   { return `🔧  ${msg}`; }
export function done(msg: string): string  { return `🎉  ${msg}`; }
export function wait(msg: string): string  { return `⏳  ${msg}`; }

export function box(lines: string[]): string {
  const maxLen = Math.max(...lines.map((l) => l.length), 0);
  const border = "─".repeat(maxLen + 4);
  const body   = lines.map((l) => `│  ${l.padEnd(maxLen)}  │`).join("\n");
  return `┌${border}┐\n${body}\n└${border}┘`;
}
