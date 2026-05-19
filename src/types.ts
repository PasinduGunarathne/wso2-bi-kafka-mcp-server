// src/types.ts
// Shared domain types for the Kafka + BI MCP server.

export interface PrerequisiteCheck {
  name: string;
  ok: boolean;
  detail: string;
  /** Shown only when ok=false — platform-aware install instructions. */
  installNote?: string;
}

export interface KafkaContainerInfo {
  name: string;
  service: string;
  status: string;
  running: boolean;
}

export interface BiProjectInfo {
  path: string;
  packageOrg: string;
  packageName: string;
  packageVersion: string;
  ballerinaDistribution: string;
  configurables: BiConfigurable[];
}

export interface BiConfigurable {
  name: string;
  type: string;
  defaultValue: string;
  currentOverride?: string;
}

// ── Error-flow types ──────────────────────────────────────────────────────────

export type TestStatus = "pass" | "fail" | "warning" | "skipped";

export interface ErrorFlowResult {
  testName: string;
  topic: string;
  payload: string;
  expectedBehavior: string;
  observedBehavior: string;
  logSnippets: string[];
  status: TestStatus;
  recommendation: string;
}

export interface CommitBehavior {
  /** Whether autoCommit is explicitly set to false in listener config. */
  autoCommitDisabled: boolean;
  /** Whether caller->commit() appears anywhere in the source. */
  hasManualCommit: boolean;
  /** Whether commit appears inside a foreach / do block (inside loop). */
  commitInsideLoop: boolean;
  /** Whether commit appears after the foreach loop (outside loop). */
  commitAfterLoop: boolean;
}

// ── Multi-replica deployment types ───────────────────────────────────────────

export type ReplicaStatus = "starting" | "running" | "stopped" | "error";

export interface ReplicaInfo {
  instanceId: string;
  /** OS process ID — undefined until the process emits its first output. */
  pid?: number;
  projectPath: string;
  groupId: string;
  /** ISO 8601 timestamp of when the replica was started. */
  startedAt: string;
  status: ReplicaStatus;
  /** Rolling buffer of the last 50 log lines from stdout+stderr. */
  recentLogs: string[];
}
