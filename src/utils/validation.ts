// src/utils/validation.ts
// Input validation helpers for MCP tool arguments.

export class ValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ValidationError";
  }
}

/** Ensure a required string field is present and non-empty. */
export function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new ValidationError(`'${field}' is required and must be a non-empty string.`);
  }
  return value.trim();
}

/** Parse an optional string, returning a default if absent. */
export function optionalString(value: unknown, defaultVal: string): string {
  if (typeof value === "string" && value.trim().length > 0) return value.trim();
  return defaultVal;
}

/** Parse an optional positive integer, falling back to a default. */
export function optionalPositiveInt(value: unknown, field: string, defaultVal: number): number {
  if (value === undefined || value === null) return defaultVal;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 1 || !Number.isInteger(n)) {
    throw new ValidationError(`'${field}' must be a positive integer.`);
  }
  return n;
}

/** Parse an optional boolean, falling back to a default. */
export function optionalBool(value: unknown, defaultVal: boolean): boolean {
  if (value === undefined || value === null) return defaultVal;
  return Boolean(value);
}

/** Validate a Kafka topic name (basic sanity). */
export function validateTopicName(name: string): string {
  const trimmed = requireString(name, "topicName");
  if (!/^[a-zA-Z0-9._-]+$/.test(trimmed)) {
    throw new ValidationError(
      `Invalid topic name '${trimmed}'. Use only letters, digits, '.', '_', or '-'.`,
    );
  }
  if (trimmed.length > 249) {
    throw new ValidationError("Topic name must be 249 characters or fewer.");
  }
  return trimmed;
}

/** Validate an absolute filesystem path (must not be empty). */
export function validatePath(value: unknown, field: string): string {
  const p = requireString(value, field);
  if (!p.startsWith("/") && !p.match(/^[A-Za-z]:\\/)) {
    throw new ValidationError(`'${field}' must be an absolute path.`);
  }
  return p;
}

/** Mask values that look like secrets (passwords, tokens, keys). */
export function maskSecret(key: string, value: string): string {
  const lower = key.toLowerCase();
  if (lower.includes("password") || lower.includes("secret") ||
      lower.includes("token") || lower.includes("key") ||
      lower.includes("credential")) {
    return value.length > 0 ? "***" : "(empty)";
  }
  return value;
}
