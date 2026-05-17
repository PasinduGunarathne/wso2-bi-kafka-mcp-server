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
