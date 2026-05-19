// src/utils/log-patterns.ts
// Pure-function utilities for detecting known error patterns in Kafka and
// Ballerina log output, and for analysing BI project source behaviour.
// No I/O — all functions take strings and return strings/objects.

import type { CommitBehavior } from "../types.js";

// ── Known error patterns ──────────────────────────────────────────────────────

interface PatternDef {
  /** Substring to search for (case-insensitive). */
  match: string;
  /** Emoji prefix to prepend when matched. */
  emoji: "❌" | "⚠️";
  /** Short label for the pattern summary. */
  label: string;
}

export const KNOWN_PATTERNS: PatternDef[] = [
  // Ballerina consumer errors
  { match: "[Consumer] Failed",            emoji: "❌", label: "Consumer processing failure" },
  { match: "[Producer] Failed",            emoji: "❌", label: "Producer send failure" },
  { match: "compilation failed",           emoji: "❌", label: "Ballerina compilation failure" },
  { match: "ConversionError",              emoji: "❌", label: "JSON type conversion error" },
  { match: "typedesc",                     emoji: "⚠️", label: "Typed conversion issue" },
  { match: "incompatible types",           emoji: "❌", label: "Type mismatch" },
  { match: "fromJsonStringWithType",       emoji: "⚠️", label: "JSON parse/conversion" },
  { match: "string:fromBytes",             emoji: "⚠️", label: "Bytes-to-string conversion" },
  // Kafka broker errors
  { match: "LEADER_NOT_AVAILABLE",         emoji: "⚠️", label: "Kafka leader not available" },
  { match: "Connection refused",           emoji: "❌", label: "Kafka connection refused" },
  { match: "kafka: Connection",            emoji: "⚠️", label: "Kafka connection issue" },
  { match: "UnknownTopicOrPartition",      emoji: "❌", label: "Unknown topic or partition" },
  { match: "TopicAuthorizationException",  emoji: "❌", label: "Topic authorization error" },
  // Ballerina runtime / general
  { match: "error:",                       emoji: "❌", label: "Ballerina error" },
  { match: "panic:",                       emoji: "❌", label: "Ballerina panic" },
  { match: "address already in use",       emoji: "❌", label: "Port conflict" },
  { match: "commit skipped",               emoji: "⚠️", label: "Offset commit skipped" },
  { match: "OFFSET_OUT_OF_RANGE",          emoji: "⚠️", label: "Offset out of range" },
  { match: "GroupAuthorizationException",  emoji: "❌", label: "Consumer group auth error" },
];

/**
 * Scan an array of log lines and prefix any line that matches a known error
 * pattern with the appropriate emoji.  Lines that don't match are returned
 * unchanged.  Matching is case-insensitive substring search.
 */
export function highlightErrorPatterns(lines: string[]): string[] {
  return lines.map((line) => {
    for (const p of KNOWN_PATTERNS) {
      if (line.toLowerCase().includes(p.match.toLowerCase())) {
        return `${p.emoji}  ${line}`;
      }
    }
    return line;
  });
}

/**
 * Count how many times each known pattern appears across all lines.
 * Returns only patterns with count > 0.
 */
export function summarisePatterns(
  lines: string[],
): Array<{ label: string; count: number }> {
  const combined = lines.join("\n").toLowerCase();
  return KNOWN_PATTERNS
    .map((p) => {
      let count = 0;
      let idx = 0;
      const needle = p.match.toLowerCase();
      while ((idx = combined.indexOf(needle, idx)) !== -1) {
        count++;
        idx += needle.length;
      }
      return { label: p.label, count };
    })
    .filter((r) => r.count > 0);
}

// ── Commit behaviour detection ────────────────────────────────────────────────

/**
 * Analyse a Ballerina source string (usually main.bal or connections.bal) to
 * determine how offset commits are handled.
 *
 * This uses heuristic pattern matching on the source text — it does not parse
 * the Ballerina AST.  Results are indicative, not definitive.
 */
export function detectCommitBehavior(src: string): CommitBehavior {
  const autoCommitDisabled =
    /autoCommit\s*:\s*false/i.test(src);

  const hasManualCommit =
    /caller\s*->\s*['`]?commit\s*\(/i.test(src) ||
    /caller\s*->\s*commit\s*\(/i.test(src);

  // Heuristic: if "commit(" appears inside a "foreach" block but before the
  // closing "}" of that block, call it "inside loop".  We look for
  // "foreach" ... "commit" with no intervening "foreach" end patterns.
  // This is approximate — nested structures can fool it.
  const foreachIdx   = src.indexOf("foreach ");
  const commitIdx    = src.search(/caller\s*->\s*['`]?commit/);

  // Simplified: if there's a foreach AND commit appears, try to determine
  // relative position by checking whether "commit" falls between foreach
  // and the closing "}" at the same nesting level.
  // Instead, use a text-proximity heuristic:
  //   - "inside loop": commit appears between "foreach" and the NEXT blank
  //     line / closing brace sequence that looks like loop end, OR commit
  //     is indented more than the foreach line.
  // For reliability, we use the simpler rule: if commit appears BEFORE the
  // last "}" in the function body that follows the foreach, it's inside;
  // otherwise after.  In practice the generated code always has commit AFTER
  // the foreach.

  let commitInsideLoop = false;
  let commitAfterLoop  = false;

  if (hasManualCommit && foreachIdx !== -1 && commitIdx !== -1) {
    // Find the end of the foreach block: look for the closing "}" of the
    // foreach by counting braces from foreachIdx.
    let depth = 0;
    let foreachEnd = -1;
    for (let i = foreachIdx; i < src.length; i++) {
      if (src[i] === "{") depth++;
      else if (src[i] === "}") {
        depth--;
        if (depth === 0) {
          foreachEnd = i;
          break;
        }
      }
    }

    if (foreachEnd !== -1) {
      commitInsideLoop = commitIdx < foreachEnd;
      commitAfterLoop  = commitIdx > foreachEnd;
    } else {
      // Cannot determine — assume after loop (safe default)
      commitAfterLoop = true;
    }
  } else if (hasManualCommit) {
    // No foreach found — commit is standalone
    commitAfterLoop = true;
  }

  return { autoCommitDisabled, hasManualCommit, commitInsideLoop, commitAfterLoop };
}

// ── DLQ detection ─────────────────────────────────────────────────────────────

const DLQ_VARIABLE_PATTERNS = [
  /configurable\s+string\s+(dlqTopic)\s*=/i,
  /configurable\s+string\s+(deadLetterTopic)\s*=/i,
  /configurable\s+string\s+(errorTopic)\s*=/i,
  /configurable\s+string\s+(dlq)\s*=/i,
];

const DLQ_PRODUCER_PATTERNS = [
  /->send\s*\(\s*\{[^}]*topic\s*:\s*dlqTopic/i,
  /->send\s*\(\s*\{[^}]*topic\s*:\s*deadLetterTopic/i,
  /->send\s*\(\s*\{[^}]*topic\s*:\s*errorTopic/i,
];

/**
 * Scan an array of Ballerina source strings for DLQ-related configurable
 * variables and producer send calls.  Returns the variable name if found,
 * or null if no DLQ pattern is detected.
 */
export function detectDlqConfig(sources: string[]): string | null {
  const combined = sources.join("\n");

  for (const pattern of DLQ_VARIABLE_PATTERNS) {
    const m = combined.match(pattern);
    if (m) {
      // Also confirm a producer send uses this variable
      const varName = m[1];
      const sendPattern = new RegExp(
        `->send\\s*\\(\\s*\\{[^}]*topic\\s*:\\s*${varName}`,
        "i",
      );
      if (sendPattern.test(combined)) return varName;

      // Variable exists but no matching send — still return it as a candidate
      return varName;
    }
  }

  // Check for producer send patterns even without explicit configurable
  for (const pattern of DLQ_PRODUCER_PATTERNS) {
    if (pattern.test(combined)) return "dlqTopic";
  }

  return null;
}

/**
 * Extract the default value of a configurable variable from Ballerina source.
 * Returns undefined if not found.
 */
export function extractConfigurableDefault(
  sources: string[],
  varName: string,
): string | undefined {
  const combined = sources.join("\n");
  const pattern = new RegExp(
    `configurable\\s+\\S+\\s+${varName}\\s*=\\s*"([^"]*)"`,
    "i",
  );
  const m = combined.match(pattern);
  return m ? m[1] : undefined;
}
