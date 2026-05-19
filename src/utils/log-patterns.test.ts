// src/utils/log-patterns.test.ts
// Unit tests for the pure-function utilities in log-patterns.ts.
// These tests require no Docker, no Ballerina, and no network access.

import { describe, it, expect } from "vitest";
import {
  highlightErrorPatterns,
  summarisePatterns,
  detectCommitBehavior,
  detectDlqConfig,
  extractConfigurableDefault,
} from "./log-patterns.js";

// ── highlightErrorPatterns ─────────────────────────────────────────────────────

describe("highlightErrorPatterns", () => {
  it("prefixes a known Ballerina consumer error with ❌", () => {
    const lines = ["[Consumer] Failed to process message: unexpected token"];
    const result = highlightErrorPatterns(lines);
    expect(result[0]).toMatch(/^❌/);
    expect(result[0]).toContain("[Consumer] Failed to process message");
  });

  it("prefixes a Kafka connection error with ❌", () => {
    const lines = ["WARN  Connection refused to broker localhost:9092"];
    const result = highlightErrorPatterns(lines);
    expect(result[0]).toMatch(/^❌/);
  });

  it("prefixes a LEADER_NOT_AVAILABLE warning with ⚠️", () => {
    const lines = ["WARN  LEADER_NOT_AVAILABLE for partition orders-0"];
    const result = highlightErrorPatterns(lines);
    expect(result[0]).toMatch(/^⚠️/);
  });

  it("prefixes a ConversionError line with ❌", () => {
    const lines = ["error: ConversionError — cannot cast string to float"];
    const result = highlightErrorPatterns(lines);
    expect(result[0]).toMatch(/^❌/);
  });

  it("leaves a normal log line unchanged", () => {
    const line = "INFO  Service started on port 9092";
    const result = highlightErrorPatterns([line]);
    expect(result[0]).toBe(line);
  });

  it("is case-insensitive when matching", () => {
    const lines = ["ballerina PANIC: null pointer"];
    const result = highlightErrorPatterns(lines);
    expect(result[0]).toMatch(/^❌/);
  });

  it("handles an empty array", () => {
    expect(highlightErrorPatterns([])).toEqual([]);
  });

  it("processes multiple lines independently", () => {
    const lines = [
      "INFO  Consumer started",
      "[Consumer] Failed to process message: bad json",
      "INFO  Listener ready",
      "error: panic at runtime",
    ];
    const result = highlightErrorPatterns(lines);
    expect(result[0]).toBe("INFO  Consumer started");
    expect(result[1]).toMatch(/^❌/);
    expect(result[2]).toBe("INFO  Listener ready");
    expect(result[3]).toMatch(/^❌/);
  });
});

// ── summarisePatterns ─────────────────────────────────────────────────────────

describe("summarisePatterns", () => {
  it("returns only patterns with count > 0", () => {
    const lines = [
      "[Consumer] Failed once",
      "[Consumer] Failed twice",
      "INFO  nothing bad here",
    ];
    const summary = summarisePatterns(lines);
    const labels = summary.map((s) => s.label);
    expect(labels).toContain("Consumer processing failure");
    // Unmatched patterns should not appear
    expect(labels).not.toContain("Ballerina compilation failure");
  });

  it("counts multiple occurrences correctly", () => {
    const lines = [
      "error: first problem",
      "error: second problem",
      "error: third problem",
    ];
    const summary = summarisePatterns(lines);
    const errEntry = summary.find((s) => s.label === "Ballerina error");
    expect(errEntry).toBeDefined();
    expect(errEntry!.count).toBeGreaterThanOrEqual(3);
  });

  it("returns an empty array when no patterns match", () => {
    const lines = ["INFO  Everything is fine", "DEBUG  Processing record 1"];
    expect(summarisePatterns(lines)).toEqual([]);
  });

  it("handles an empty array", () => {
    expect(summarisePatterns([])).toEqual([]);
  });
});

// ── detectCommitBehavior ──────────────────────────────────────────────────────

describe("detectCommitBehavior", () => {
  it("detects autoCommit: false in source", () => {
    const src = `
      kafka:ListenerConfiguration config = {
        groupId: "test",
        autoCommit: false
      };
    `;
    const result = detectCommitBehavior(src);
    expect(result.autoCommitDisabled).toBe(true);
  });

  it("returns autoCommitDisabled: false when autoCommit is not set to false", () => {
    const src = `
      kafka:ListenerConfiguration config = { groupId: "test" };
    `;
    const result = detectCommitBehavior(src);
    expect(result.autoCommitDisabled).toBe(false);
  });

  it("detects manual commit call", () => {
    const src = `
      remote function onConsumerRecord(kafka:Caller caller, ...) returns error? {
        foreach var msg in messages {
          // process
        }
        check caller->'commit();
      }
    `;
    const result = detectCommitBehavior(src);
    expect(result.hasManualCommit).toBe(true);
  });

  it("detects commit AFTER the foreach loop", () => {
    // Commit sits outside the foreach block (standard generated pattern)
    const src = `
      remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] messages) returns error? {
        foreach kafka:AnydataConsumerRecord msg in messages {
          do {
            string rawStr = check string:fromBytes(check msg.value.ensureType());
            OrderEvent order = check rawStr.fromJsonStringWithType();
            ProcessedOrder processed = processOrder(order);
            check producer->send({ topic: outputTopic, value: processed.toJsonString().toBytes() });
          } on fail error e {
            log:printError("[Consumer] Failed to process message", 'error = e);
          }
        }
        check caller->'commit();
      }
    `;
    const result = detectCommitBehavior(src);
    expect(result.autoCommitDisabled).toBe(false); // not set in this snippet
    expect(result.hasManualCommit).toBe(true);
    expect(result.commitAfterLoop).toBe(true);
    expect(result.commitInsideLoop).toBe(false);
  });

  it("detects commit INSIDE the foreach loop", () => {
    const src = `
      remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] messages) returns error? {
        foreach kafka:AnydataConsumerRecord msg in messages {
          // process
          check caller->'commit();
        }
      }
    `;
    const result = detectCommitBehavior(src);
    expect(result.hasManualCommit).toBe(true);
    expect(result.commitInsideLoop).toBe(true);
    expect(result.commitAfterLoop).toBe(false);
  });

  it("returns all false for empty source", () => {
    const result = detectCommitBehavior("");
    expect(result.autoCommitDisabled).toBe(false);
    expect(result.hasManualCommit).toBe(false);
    expect(result.commitInsideLoop).toBe(false);
    expect(result.commitAfterLoop).toBe(false);
  });

  it("handles source with commit but no foreach", () => {
    const src = `
      remote function onConsumerRecord(kafka:Caller caller, ...) returns error? {
        check caller->'commit();
      }
    `;
    const result = detectCommitBehavior(src);
    expect(result.hasManualCommit).toBe(true);
    expect(result.commitInsideLoop).toBe(false);
    expect(result.commitAfterLoop).toBe(true);
  });
});

// ── detectDlqConfig ───────────────────────────────────────────────────────────

describe("detectDlqConfig", () => {
  it("returns null when no DLQ pattern is present", () => {
    const src = [
      `configurable string kafkaTopic = "orders";`,
      `configurable string outputTopic = "orders.out";`,
    ];
    expect(detectDlqConfig(src)).toBeNull();
  });

  it("detects a dlqTopic configurable variable", () => {
    const src = [
      `configurable string dlqTopic = "orders.dlq";`,
      `check dlqProducer->send({ topic: dlqTopic, value: payload });`,
    ];
    expect(detectDlqConfig(src)).toBe("dlqTopic");
  });

  it("detects a deadLetterTopic variable", () => {
    const src = [
      `configurable string deadLetterTopic = "errors.dlq";`,
      `check dlqProducer->send({ topic: deadLetterTopic, value: payload });`,
    ];
    expect(detectDlqConfig(src)).toBe("deadLetterTopic");
  });

  it("detects an errorTopic variable", () => {
    const src = [
      `configurable string errorTopic = "order-errors";`,
    ];
    // Variable exists even without a send — should still return the variable name
    expect(detectDlqConfig(src)).toBe("errorTopic");
  });

  it("handles an empty array", () => {
    expect(detectDlqConfig([])).toBeNull();
  });

  it("works across multiple source files", () => {
    const config = `configurable string dlqTopic = "dead-letters";`;
    const main   = `check errorProducer->send({ topic: dlqTopic, value: errPayload });`;
    expect(detectDlqConfig([config, main])).toBe("dlqTopic");
  });
});

// ── extractConfigurableDefault ────────────────────────────────────────────────

describe("extractConfigurableDefault", () => {
  it("extracts the default value of a known configurable", () => {
    const src = [`configurable string kafkaTopic = "orders";`];
    expect(extractConfigurableDefault(src, "kafkaTopic")).toBe("orders");
  });

  it("returns undefined when variable is not present", () => {
    const src = [`configurable string anotherVar = "foo";`];
    expect(extractConfigurableDefault(src, "kafkaTopic")).toBeUndefined();
  });

  it("handles multiple sources", () => {
    const srcs = [
      `configurable string groupId = "order-processor";`,
      `configurable string kafkaTopic = "orders";`,
    ];
    expect(extractConfigurableDefault(srcs, "kafkaTopic")).toBe("orders");
    expect(extractConfigurableDefault(srcs, "groupId")).toBe("order-processor");
  });

  it("returns undefined for empty sources", () => {
    expect(extractConfigurableDefault([], "kafkaTopic")).toBeUndefined();
  });
});
