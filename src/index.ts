#!/usr/bin/env node
/**
 * kafka-bi-mcp-server
 * ───────────────────
 * MCP Server for a local Kafka + WSO2 Ballerina Integrator: BI environment.
 *
 * Setup & demo:    setup_kafka_and_bi, run_bi_demo
 * Prerequisites:   check_prerequisites
 * Kafka lifecycle: start_kafka, stop_kafka, kafka_status, show_kafka_logs
 * Kafka admin:     list_topics, create_topic
 * Message ops:     produce_test_message, consume_test_message
 * BI tools:        validate_bi_project, run_bi_project,
 *                  inspect_bi_kafka_config, generate_bi_kafka_sample
 * Error flows:     run_error_flow_suite, trigger_invalid_json_error,
 *                  trigger_schema_mismatch_error, trigger_business_rule_error,
 *                  test_missing_topic_error, test_consumer_not_running_flow,
 *                  test_manual_commit_redelivery, check_dlq,
 *                  show_error_diagnostics, generate_error_flow_report
 * Local replicas:  start_bi_replica, stop_bi_replica, stop_all_bi_replicas,
 *                  list_bi_replicas, inspect_consumer_group
 * Containerized:   build_bi_docker_image, generate_bi_docker_compose,
 *                  start_bi_replicas_containerized, stop_bi_replicas_containerized,
 *                  scale_bi_replicas
 */

import { Server }               from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

// ── Tool handlers ────────────────────────────────────────────────────────────
import { setupKafkaAndBi, runBiDemo, chooseDeploymentMode } from "./tools/setup.js";
import { checkPrerequisites }  from "./tools/prerequisites.js";
import { startKafka, stopKafka, kafkaStatus, showKafkaLogs } from "./tools/kafka.js";
import { listTopics, createTopic } from "./tools/kafka-admin.js";
import { produceTestMessage, consumeTestMessage } from "./tools/kafka-messages.js";
import {
  validateBiProject,
  runBiProject,
  inspectBiKafkaConfig,
  generateBiKafkaSample,
} from "./tools/bi.js";
import {
  runErrorFlowSuite,
  triggerInvalidJsonError,
  triggerSchemaMismatchError,
  triggerBusinessRuleError,
  testMissingTopicError,
  testConsumerNotRunningFlow,
  testManualCommitRedelivery,
  checkDlq,
  showErrorDiagnostics,
  generateErrorFlowReport,
} from "./tools/error-flows.js";
import {
  startBiReplica,
  stopBiReplica,
  stopAllBiReplicas,
  listBiReplicas,
  inspectConsumerGroup,
} from "./tools/replica-manager.js";
import {
  buildBiDockerImage,
  generateBiDockerCompose,
  startBiReplicasContainerized,
  stopBiReplicasContainerized,
  scaleBiReplicas,
} from "./tools/containerized.js";

// ── Handler registry ─────────────────────────────────────────────────────────
const HANDLERS: Record<string, (args: any) => Promise<string>> = {
  // Setup & demo
  choose_deployment_mode: chooseDeploymentMode,
  setup_kafka_and_bi:     setupKafkaAndBi,
  run_bi_demo:            runBiDemo,
  // Prerequisites
  check_prerequisites:    checkPrerequisites,
  // Kafka lifecycle
  start_kafka:            startKafka,
  stop_kafka:             stopKafka,
  kafka_status:           kafkaStatus,
  show_kafka_logs:        showKafkaLogs,
  // Kafka admin
  list_topics:            listTopics,
  create_topic:           createTopic,
  // Message operations
  produce_test_message:   produceTestMessage,
  consume_test_message:   consumeTestMessage,
  // BI tools
  validate_bi_project:    validateBiProject,
  run_bi_project:         runBiProject,
  inspect_bi_kafka_config: inspectBiKafkaConfig,
  generate_bi_kafka_sample: generateBiKafkaSample,
  // Error-flow tools
  run_error_flow_suite:           runErrorFlowSuite,
  trigger_invalid_json_error:     triggerInvalidJsonError,
  trigger_schema_mismatch_error:  triggerSchemaMismatchError,
  trigger_business_rule_error:    triggerBusinessRuleError,
  test_missing_topic_error:       testMissingTopicError,
  test_consumer_not_running_flow: testConsumerNotRunningFlow,
  test_manual_commit_redelivery:  testManualCommitRedelivery,
  check_dlq:                      checkDlq,
  show_error_diagnostics:         showErrorDiagnostics,
  generate_error_flow_report:     generateErrorFlowReport,
  // Local process replicas
  start_bi_replica:               startBiReplica,
  stop_bi_replica:                stopBiReplica,
  stop_all_bi_replicas:           stopAllBiReplicas,
  list_bi_replicas:               listBiReplicas,
  inspect_consumer_group:         inspectConsumerGroup,
  // Containerized replicas
  build_bi_docker_image:              buildBiDockerImage,
  generate_bi_docker_compose:         generateBiDockerCompose,
  start_bi_replicas_containerized:    startBiReplicasContainerized,
  stop_bi_replicas_containerized:     stopBiReplicasContainerized,
  scale_bi_replicas:                  scaleBiReplicas,
};

// ── Tool definitions (MCP schema) ────────────────────────────────────────────
const TOOLS = [

  // ── Deployment Mode Wizard ───────────────────────────────────────────────────
  {
    name: "choose_deployment_mode",
    description:
      "Interactive wizard that helps the user decide between the three deployment modes " +
      "for the Ballerina Kafka integration: " +
      "(1) Standalone — single Kafka broker + single listener, best for development; " +
      "(2) Local multi-replica — multiple `bal run` processes sharing a consumer group, best for testing partition distribution; " +
      "(3) Containerized multi-replica — N Docker containers, best for production-like validation. " +
      "Called with NO arguments: returns a side-by-side comparison of all three modes with trade-offs " +
      "and decision questions to present to the user. " +
      "Called with mode='standalone'|'local-replicas'|'containerized': returns the exact ordered " +
      "step-by-step tool sequence for that mode. " +
      "ALWAYS call this tool (with no arguments) at the beginning of a Kafka + BI setup conversation " +
      "BEFORE calling setup_kafka_and_bi, unless the user has already stated which mode they want. " +
      "Trigger phrases: 'set up kafka', 'get started', 'help me choose', 'what mode should I use', " +
      "'standalone vs replicas', 'how do I deploy'.",
    inputSchema: {
      type: "object",
      properties: {
        mode: {
          type: "string",
          enum: ["standalone", "local-replicas", "containerized"],
          description:
            "The deployment mode to get a step-by-step guide for. " +
            "Omit to get the comparison wizard that helps the user choose.",
        },
      },
    },
  },

  // ── Setup & Demo ────────────────────────────────────────────────────────────
  {
    name: "setup_kafka_and_bi",
    description:
      "One-command setup: starts a local Kafka broker, creates the required topics, " +
      "generates a Ballerina BI demo project with both consumer and producer flows, " +
      "and compiles it with 'bal build'. " +
      "The project is placed in the best location for this machine: " +
      "~/WSO2Integrator/kafka-bi-demo/kafkaintegration/ if WSO2 Integrator is installed, " +
      "~/Documents/BallerinaProjects/kafka-bi-demo/kafkaintegration/ on macOS/Windows, " +
      "or ~/BallerinaProjects/kafka-bi-demo/kafkaintegration/ on Linux. " +
      "Supply projectPath to override. " +
      "Trigger phrases: 'setup kafka and ballerina', 'setup kafka and bi', " +
      "'set up the environment'. " +
      "If deploymentMode is NOT provided, the tool returns a deployment mode selection table " +
      "and stops — the user must choose a mode before setup proceeds. " +
      "When setup is complete for standalone mode, call run_bi_demo next. " +
      "For local-replicas mode, call start_bi_replica. " +
      "For containerized mode, call build_bi_docker_image.",
    inputSchema: {
      type: "object",
      properties: {
        deploymentMode: {
          type: "string",
          enum: ["standalone", "local-replicas", "containerized"],
          description:
            "Required before setup proceeds. " +
            "standalone: single listener process. " +
            "local-replicas: multiple bal run processes sharing a consumer group. " +
            "containerized: N Docker containers on the kafka-local network. " +
            "If omitted, the tool returns the mode selection table and waits.",
        },
        projectPath: {
          type: "string",
          description:
            "Directory where the BI demo project will be generated. " +
            "Auto-detected based on OS and whether WSO2 Integrator is installed. " +
            "Provide an absolute path to override (e.g. an existing Ballerina project).",
        },
        kafkaComposePath: {
          type: "string",
          description: "Directory containing the Kafka docker-compose.yml (uses default if omitted).",
        },
      },
    },
  },
  {
    name: "run_bi_demo",
    description:
      "Execute the end-to-end sample flow: produce a test OrderEvent to the input topic, " +
      "start the BI Kafka listener (which consumes and processes the order), " +
      "then verify the processed result appears on the output topic. " +
      "Call this ONLY after setup_kafka_and_bi has completed successfully and the user has approved. " +
      "Set includeNegativeTests=true to also run invalid-JSON and schema-mismatch tests after the happy path. " +
      "Trigger phrases: 'run the demo', 'test the flow', 'execute the sample', 'yes run it'.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath: {
          type: "string",
          description:
            "BI demo project directory. Auto-detected (same smart-path logic as setup_kafka_and_bi). " +
            "Override if you used a custom projectPath during setup.",
        },
        kafkaComposePath: {
          type: "string",
          description: "Kafka Compose directory (uses default if omitted).",
        },
        includeNegativeTests: {
          type: "boolean",
          description: "Also run invalid-JSON and schema-mismatch error tests after the happy-path demo. Default: false.",
        },
      },
    },
  },

  // ── Prerequisites ───────────────────────────────────────────────────────────
  {
    name: "check_prerequisites",
    description:
      "Verify that all required tools and paths are present: Docker, Docker Compose v2, " +
      "Node.js, Ballerina CLI, Kafka Compose file, and BI project directory. " +
      "Also checks that ports 9092 and 8080 are available. " +
      "Displays an environment layout section showing the auto-detected project path " +
      "(WSO2 Integrator workspace or OS-appropriate BallerinaProjects directory) " +
      "and instructions for using an existing Ballerina project. Run this first.",
    inputSchema: {
      type: "object",
      properties: {
        biProjectPath: {
          type: "string",
          description:
            "Path to an existing Ballerina BI project. " +
            "If omitted, the path is auto-detected: ~/WSO2Integrator/kafka-bi-demo/kafkaintegration/ " +
            "(if WSO2 Integrator is installed), ~/Documents/BallerinaProjects/kafka-bi-demo/kafkaintegration/ " +
            "(macOS/Windows), or ~/BallerinaProjects/kafka-bi-demo/kafkaintegration/ (Linux).",
        },
        kafkaComposePath: { type: "string", description: "Directory containing docker-compose.yml for Kafka (default: built-in path)." },
      },
    },
  },

  // ── Kafka Lifecycle ─────────────────────────────────────────────────────────
  {
    name: "start_kafka",
    description:
      "Start local Kafka using the existing Docker Compose setup (KRaft mode, no ZooKeeper). " +
      "Starts kafka (port 9092) and kafka-ui (port 8080). Waits up to 60s for broker readiness. " +
      "Returns bootstrap server and Kafka UI URL.",
    inputSchema: {
      type: "object",
      properties: {
        kafkaComposePath: { type: "string", description: "Directory containing docker-compose.yml (uses default if omitted)." },
      },
    },
  },
  {
    name: "stop_kafka",
    description:
      "Stop Kafka containers. Preserves data volumes by default. " +
      "Pass deleteVolumes=true to also remove the kafka-storage volume (destructive — requires re-init on next start).",
    inputSchema: {
      type: "object",
      properties: {
        kafkaComposePath: { type: "string" },
        deleteVolumes:    { type: "boolean", description: "Delete Kafka data volumes. Default: false." },
      },
    },
  },
  {
    name: "kafka_status",
    description:
      "Show the current state of all Kafka containers, broker liveness, and Kafka UI availability. " +
      "Use this to confirm Kafka is running before other operations.",
    inputSchema: {
      type: "object",
      properties: {
        kafkaComposePath: { type: "string" },
      },
    },
  },
  {
    name: "show_kafka_logs",
    description:
      "Show recent logs from Kafka containers. Filter by service name (kafka or kafka-ui). " +
      "Sensitive values are automatically redacted.",
    inputSchema: {
      type: "object",
      properties: {
        kafkaComposePath: { type: "string" },
        service: { type: "string", enum: ["kafka", "kafka-ui"], description: "Service to show logs for. Omit for all." },
        lines:   { type: "number", description: "Number of log lines to show (default: 50, max: 500)." },
      },
    },
  },

  // ── Kafka Admin ─────────────────────────────────────────────────────────────
  {
    name: "list_topics",
    description:
      "List all Kafka topics. Separates user-defined topics from internal (__consumer_offsets, etc.). " +
      "Kafka must be running.",
    inputSchema: {
      type: "object",
      properties: {
        kafkaComposePath: { type: "string" },
      },
    },
  },
  {
    name: "create_topic",
    description:
      "Create a new Kafka topic. Validates topic name (letters, digits, '.', '_', '-' only). " +
      "Defaults: partitions=1, replicationFactor=1 (single-node local setup). " +
      "Idempotent — will not fail if the topic already exists.",
    inputSchema: {
      type: "object",
      properties: {
        topicName:         { type: "string",  description: "Topic name to create." },
        partitions:        { type: "number",  description: "Number of partitions (default: 1)." },
        replicationFactor: { type: "number",  description: "Replication factor (default: 1)." },
        kafkaComposePath:  { type: "string" },
      },
      required: ["topicName"],
    },
  },

  // ── Message Operations ──────────────────────────────────────────────────────
  {
    name: "produce_test_message",
    description:
      "Produce a JSON test message to a Kafka topic. " +
      "If no message is provided, sends a default JSON payload with test=true and a timestamp. " +
      "Topic must exist (or auto-create must be enabled on the broker).",
    inputSchema: {
      type: "object",
      properties: {
        topicName:        { type: "string", description: "Target topic name." },
        message:          { type: "string", description: "Message payload (any string/JSON). Omit for default test payload." },
        kafkaComposePath: { type: "string" },
      },
      required: ["topicName"],
    },
  },
  {
    name: "consume_test_message",
    description:
      "Consume a limited number of messages from a Kafka topic. " +
      "Always uses a timeout to prevent hanging. Defaults: maxMessages=5, fromBeginning=true, timeout=10s.",
    inputSchema: {
      type: "object",
      properties: {
        topicName:        { type: "string",  description: "Topic to consume from." },
        maxMessages:      { type: "number",  description: "Maximum messages to read (default: 5)." },
        fromBeginning:    { type: "boolean", description: "Start from earliest offset (default: true)." },
        kafkaComposePath: { type: "string" },
      },
      required: ["topicName"],
    },
  },

  // ── BI Tools ────────────────────────────────────────────────────────────────
  {
    name: "validate_bi_project",
    description:
      "Compile the existing BI Kafka project with 'bal build'. " +
      "Does NOT modify any files. Reports compilation errors clearly. " +
      "Run this before run_bi_project to catch issues early.",
    inputSchema: {
      type: "object",
      properties: {
        biProjectPath: { type: "string", description: "Path to the BI Ballerina project (uses default if omitted)." },
      },
    },
  },
  {
    name: "run_bi_project",
    description:
      "Start the BI Kafka project with 'bal run'. Captures 15 seconds of startup output. " +
      "The Kafka listener runs indefinitely as a background process. " +
      "Does NOT overwrite Config.toml or any project files.",
    inputSchema: {
      type: "object",
      properties: {
        biProjectPath: { type: "string" },
      },
    },
  },
  {
    name: "inspect_bi_kafka_config",
    description:
      "Read and display the BI project's configuration: Ballerina.toml package info, " +
      "all configurable parameters (bootstrap servers, topic, group ID, etc.), " +
      "and active Config.toml overrides. Secrets are masked. Does NOT modify any files.",
    inputSchema: {
      type: "object",
      properties: {
        biProjectPath: { type: "string" },
      },
    },
  },
  {
    name: "generate_bi_kafka_sample",
    description:
      "Generate a new Ballerina BI Kafka project skeleton at a specified directory. " +
      "Creates: Ballerina.toml, config.bal, types.bal, connections.bal, main.bal, functions.bal. " +
      "Uses the same patterns as the existing working BI project. " +
      "REQUIRES targetPath — will NOT write inside the existing BI project.",
    inputSchema: {
      type: "object",
      properties: {
        targetPath:       { type: "string", description: "Absolute path to generate the project in (required, must not overlap existing BI project)." },
        packageName:      { type: "string", description: "Ballerina package name (default: kafkasample)." },
        orgName:          { type: "string", description: "Ballerina org name (default: myorg)." },
        topicName:        { type: "string", description: "Kafka topic to subscribe to (default: events)." },
        groupId:          { type: "string", description: "Kafka consumer group ID (default: <packageName>-consumer)." },
        bootstrapServers: { type: "string", description: "Kafka bootstrap servers (default: localhost:9092)." },
      },
      required: ["targetPath"],
    },
  },

  // ── Error-Flow Tools ────────────────────────────────────────────────────────
  {
    name: "run_error_flow_suite",
    description:
      "Run a complete set of safe erroneous-flow tests against the BI Kafka project. " +
      "Checks prerequisites, ensures topics exist, builds the project, then runs: " +
      "invalid JSON, schema mismatch (×2), business rule, missing topic, and consumer-not-running tests. " +
      "Optional: manual commit/redelivery, DLQ check, and Kafka unavailable test. " +
      "Returns a structured pass/fail/warning report. " +
      "Works on macOS, Windows, and Linux — no host-shell commands.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath:                     { type: "string",  description: "BI project path (auto-detected if omitted)." },
        inputTopic:                      { type: "string",  description: "Input topic (default: bi.orders.in)." },
        outputTopic:                     { type: "string",  description: "Output topic (default: bi.orders.out)." },
        kafkaComposePath:                { type: "string",  description: "Kafka Compose directory (uses default if omitted)." },
        includeManualCommitRedeliveryTest: { type: "boolean", description: "Include manual commit / redelivery test. Default: false." },
        includeDlqCheck:                 { type: "boolean", description: "Include DLQ inspection. Default: false." },
        includeKafkaUnavailableTest:     { type: "boolean", description: "Include Kafka unavailable test (stops then restarts Kafka). Default: false." },
      },
    },
  },
  {
    name: "trigger_invalid_json_error",
    description:
      "Produce a malformed JSON payload to the BI input topic and report the expected consumer behaviour. " +
      "Safe — payload is piped via stdin, no shell interpolation. " +
      "Set captureOutput=true to also run the BI listener briefly and capture live log output.",
    inputSchema: {
      type: "object",
      properties: {
        topicName:       { type: "string",  description: "Target topic (default: bi.orders.in)." },
        kafkaComposePath: { type: "string", description: "Kafka Compose directory." },
        projectPath:     { type: "string",  description: "BI project path (for live capture)." },
        captureOutput:   { type: "boolean", description: "Run BI listener briefly and capture output. Default: false." },
      },
    },
  },
  {
    name: "trigger_schema_mismatch_error",
    description:
      "Produce a valid JSON payload that does not match the expected OrderEvent type. " +
      "Variants: 'missing-field' (default) sends a payload with required fields absent; " +
      "'wrong-type' sends amount as a string instead of float. " +
      "Reports the expected Ballerina ConversionError / typedesc failure.",
    inputSchema: {
      type: "object",
      properties: {
        variant:         { type: "string",  enum: ["missing-field", "wrong-type"], description: "Which schema mismatch to test (default: missing-field)." },
        topicName:       { type: "string",  description: "Target topic (default: bi.orders.in)." },
        kafkaComposePath: { type: "string", description: "Kafka Compose directory." },
        projectPath:     { type: "string",  description: "BI project path (for live capture)." },
        captureOutput:   { type: "boolean", description: "Run BI listener briefly and capture output. Default: false." },
      },
    },
  },
  {
    name: "trigger_business_rule_error",
    description:
      "Produce a structurally valid OrderEvent with a negative amount (-100.0) to test business validation. " +
      "Inspects functions.bal for amount validation. If none is found, reports that the message " +
      "was processed successfully and recommends adding a guard. Does NOT modify any project files.",
    inputSchema: {
      type: "object",
      properties: {
        topicName:       { type: "string",  description: "Target topic (default: bi.orders.in)." },
        kafkaComposePath: { type: "string", description: "Kafka Compose directory." },
        projectPath:     { type: "string",  description: "BI project path." },
        captureOutput:   { type: "boolean", description: "Run BI listener briefly and capture output. Default: false." },
      },
    },
  },
  {
    name: "test_missing_topic_error",
    description:
      "Test what happens when a message is produced to a non-existent topic. " +
      "Because KAFKA_AUTO_CREATE_TOPICS_ENABLE=true in the bundled compose file, the topic will be " +
      "auto-created rather than erroring. The tool reports this and explains how to reproduce a true " +
      "missing-topic error. Cleans up the test topic after the test.",
    inputSchema: {
      type: "object",
      properties: {
        kafkaComposePath: { type: "string", description: "Kafka Compose directory." },
      },
    },
  },
  {
    name: "test_consumer_not_running_flow",
    description:
      "Verify Kafka message retention when the BI consumer is not running. " +
      "Produces a valid OrderEvent to the input topic, confirms no output appears on the output topic " +
      "(timeout expected), then verifies the input message is retained in the topic. " +
      "Explains Kafka's durable log behaviour and offsetReset: EARLIEST.",
    inputSchema: {
      type: "object",
      properties: {
        topicName:        { type: "string", description: "Input topic (default: bi.orders.in)." },
        kafkaComposePath: { type: "string", description: "Kafka Compose directory." },
      },
    },
  },
  {
    name: "test_manual_commit_redelivery",
    description:
      "Analyse manual commit behaviour in the BI project and test whether failed messages are re-delivered. " +
      "Inspects main.bal to detect commit position (inside/after foreach loop). " +
      "Reports observed behaviour — never assumes re-delivery unless confirmed by logs. " +
      "Set captureOutput=true to run the BI listener and capture live output. " +
      "Set includeRestart=true to re-run the listener with the same group ID and confirm redelivery behaviour.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath:      { type: "string",  description: "BI project path." },
        kafkaComposePath: { type: "string",  description: "Kafka Compose directory." },
        captureOutput:    { type: "boolean", description: "Run BI listener briefly and capture output. Default: false." },
        includeRestart:   { type: "boolean", description: "Re-run listener with same group ID to test re-delivery. Default: false." },
      },
    },
  },
  {
    name: "check_dlq",
    description:
      "Inspect the BI project for Dead-Letter Queue (DLQ) configuration. " +
      "Scans .bal sources for dlqTopic, deadLetterTopic, or errorTopic configurable variables. " +
      "If DLQ exists: consumes up to 10 messages from the DLQ topic. " +
      "If DLQ does not exist: returns a clear message and implementation guidance.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath:      { type: "string", description: "BI project path." },
        kafkaComposePath: { type: "string", description: "Kafka Compose directory." },
      },
    },
  },
  {
    name: "show_error_diagnostics",
    description:
      "Return a focused diagnostics report highlighting known error patterns in recent Kafka and BI logs. " +
      "Highlights: JSON parse errors, type conversion errors, connection failures, unknown topics, " +
      "port conflicts, commit issues, and Ballerina runtime errors. Secrets are masked.",
    inputSchema: {
      type: "object",
      properties: {
        kafkaComposePath: { type: "string", description: "Kafka Compose directory." },
        projectPath:      { type: "string", description: "BI project path (to scan target/*.log files)." },
        lines:            { type: "number", description: "Number of Kafka log lines to analyse (default: 100)." },
      },
    },
  },
  {
    name: "generate_error_flow_report",
    description:
      "Generate a final structured report from one or more error-flow test results. " +
      "Shows: test name, topic, payload, expected behaviour, observed behaviour, log snippets, " +
      "pass/fail/warning status, and recommended next action. " +
      "Typically called by run_error_flow_suite — can also be called standalone.",
    inputSchema: {
      type: "object",
      properties: {
        title:   { type: "string", description: "Report title (default: 'Erroneous Flow Test Report')." },
        results: {
          type: "array",
          description: "Array of ErrorFlowResult objects from previous test runs.",
          items: { type: "object" },
        },
      },
    },
  },

  // ── Local process replicas ───────────────────────────────────────────────────
  {
    name: "start_bi_replica",
    description:
      "Start a Ballerina BI Kafka listener as a persistent background process. " +
      "Multiple replicas sharing the same kafkaGroupId form a Kafka consumer group — " +
      "Kafka distributes partitions across them automatically. " +
      "Each replica is tracked in-memory for the duration of the MCP server session. " +
      "The project must already be compiled (run validate_bi_project first). " +
      "Trigger phrases: 'start a replica', 'add another consumer', 'scale up locally', 'run N instances'.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath:     { type: "string",  description: "BI project directory. Auto-detected if omitted." },
        groupId:         { type: "string",  description: "Kafka consumer group ID shared by all replicas (default: order-processor)." },
        instanceId:      { type: "string",  description: "Unique name for this replica (auto-generated if omitted)." },
        configOverrides: {
          type: "object",
          description: "Extra -Ckey=value overrides passed to bal run (e.g. { kafkaTopic: 'payments' }).",
          additionalProperties: { type: "string" },
        },
      },
    },
  },
  {
    name: "stop_bi_replica",
    description:
      "Stop a specific local BI replica by its instanceId. " +
      "Sends SIGTERM for a graceful shutdown; falls back to SIGKILL after 5 seconds. " +
      "Use list_bi_replicas to see running instance IDs.",
    inputSchema: {
      type: "object",
      required: ["instanceId"],
      properties: {
        instanceId: { type: "string", description: "The instanceId of the replica to stop." },
      },
    },
  },
  {
    name: "stop_all_bi_replicas",
    description:
      "Stop all running local BI replicas. Optionally filter by project path. " +
      "Useful for a clean teardown after testing multi-replica scenarios.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath: { type: "string", description: "Only stop replicas for this project. Stops all if omitted." },
      },
    },
  },
  {
    name: "list_bi_replicas",
    description:
      "Show all currently running local BI replica processes: instanceId, PID, consumer group, " +
      "uptime, status, and a tail of recent log lines. " +
      "Trigger phrases: 'show replicas', 'list running instances', 'what replicas are running'.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "inspect_consumer_group",
    description:
      "Show the Kafka consumer group state: topic, partition, current offset, log-end offset, lag, " +
      "consumer ID, and host for every assigned partition. " +
      "Works for both local process replicas and containerized replicas. " +
      "Trigger phrases: 'show consumer group', 'check partition assignment', 'how is the load distributed', 'check lag'.",
    inputSchema: {
      type: "object",
      required: ["groupId"],
      properties: {
        groupId:          { type: "string", description: "Kafka consumer group ID to inspect." },
        kafkaComposePath: { type: "string", description: "Kafka Compose directory (uses default if omitted)." },
      },
    },
  },

  // ── Containerized replicas ───────────────────────────────────────────────────
  {
    name: "build_bi_docker_image",
    description:
      "Build a Docker image from the Ballerina BI project. " +
      "Steps: (1) bal build to produce the fat JAR, (2) generate a Dockerfile using " +
      "eclipse-temurin:17-jre-alpine as the base image, (3) docker build to create the image. " +
      "The image is tagged <imageName>:<tag> and stored in the local Docker daemon. " +
      "Must be done before start_bi_replicas_containerized. " +
      "Trigger phrases: 'build a docker image', 'containerise the BI project', 'package as docker'.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath: { type: "string", description: "BI project directory. Auto-detected if omitted." },
        imageName:   { type: "string", description: "Docker image name (default: bi-kafka-demo)." },
        tag:         { type: "string", description: "Image tag (default: latest)." },
      },
    },
  },
  {
    name: "generate_bi_docker_compose",
    description:
      "Generate a docker-compose.bi.yml file for the BI service. " +
      "The service joins the kafka-local Docker network and uses kafka:9093 as bootstrap server " +
      "so it can reach the bundled Kafka broker from inside a container. " +
      "Use 'docker compose -f docker-compose.bi.yml up -d --scale bi-service=N' to run N replicas. " +
      "Must be done before start_bi_replicas_containerized.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath:       { type: "string", description: "BI project directory. Auto-detected if omitted." },
        imageName:         { type: "string", description: "Docker image name (default: bi-kafka-demo)." },
        tag:               { type: "string", description: "Image tag (default: latest)." },
        groupId:           { type: "string", description: "Kafka consumer group ID (read from config.bal if omitted)." },
        inputTopic:        { type: "string", description: "Input Kafka topic (read from config.bal if omitted)." },
        outputTopic:       { type: "string", description: "Output Kafka topic (read from config.bal if omitted)." },
        outputComposeFile: { type: "string", description: "Compose file name to write (default: docker-compose.bi.yml)." },
      },
    },
  },
  {
    name: "start_bi_replicas_containerized",
    description:
      "Start N Docker container replicas of the BI Kafka listener. " +
      "Requires Kafka to be running and the Docker image to be built. " +
      "All containers join the kafka-local network and share the same consumer group, " +
      "so Kafka automatically distributes partitions across them. " +
      "Trigger phrases: 'run N docker replicas', 'start containerised replicas', 'scale containers'.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath:      { type: "string",  description: "BI project directory. Auto-detected if omitted." },
        composeFile:      { type: "string",  description: "Compose file name (default: docker-compose.bi.yml)." },
        replicas:         { type: "number",  description: "Number of container replicas to start (default: 2)." },
        kafkaComposePath: { type: "string",  description: "Kafka Compose directory (uses default if omitted)." },
      },
    },
  },
  {
    name: "stop_bi_replicas_containerized",
    description:
      "Stop and remove all containerized BI replica containers. " +
      "The Kafka broker and its topics are not affected. " +
      "Trigger phrases: 'stop docker replicas', 'tear down containers', 'stop containerised BI'.",
    inputSchema: {
      type: "object",
      properties: {
        projectPath:  { type: "string", description: "BI project directory. Auto-detected if omitted." },
        composeFile:  { type: "string", description: "Compose file name (default: docker-compose.bi.yml)." },
      },
    },
  },
  {
    name: "scale_bi_replicas",
    description:
      "Scale the number of running containerized BI replicas up or down. " +
      "Kafka automatically rebalances partitions across the new consumer count. " +
      "Set replicas=0 to stop all containers without removing the compose file. " +
      "Trigger phrases: 'scale to N replicas', 'add more containers', 'reduce replicas'.",
    inputSchema: {
      type: "object",
      required: ["replicas"],
      properties: {
        replicas:         { type: "number",  description: "Target number of replicas (0 to stop all)." },
        projectPath:      { type: "string",  description: "BI project directory. Auto-detected if omitted." },
        composeFile:      { type: "string",  description: "Compose file name (default: docker-compose.bi.yml)." },
        kafkaComposePath: { type: "string",  description: "Kafka Compose directory (uses default if omitted)." },
      },
    },
  },
];

// ── MCP Server ───────────────────────────────────────────────────────────────
const server = new Server(
  { name: "kafka-bi-mcp-server", version: "1.0.0" },
  {
    capabilities: { tools: {} },
    instructions: `
You are connected to the kafka-bi MCP server, which manages a local Apache Kafka
broker and WSO2 Ballerina Integrator (BI) Kafka project.

ALWAYS use the kafka-bi MCP tools for every Kafka and Ballerina operation.
Never use raw shell commands (docker compose, bal build, bal run, pkill, etc.)
as substitutes — the tools provide safety guardrails, structured output, and
guided workflows that raw commands cannot.

SETUP FLOW — follow this every time the user asks to set up Kafka and Ballerina:
1. Call setup_kafka_and_bi with NO arguments first.
   The tool returns a deployment mode selection table (standalone / local-replicas / containerized).
   Present the table to the user and wait for their choice.
2. Call setup_kafka_and_bi { deploymentMode: "<chosen mode>" } to run the full setup.
3. Follow the mode-specific next steps the tool returns.
Never skip the deployment mode picker. Never assume a mode without asking.

TOOL SELECTION GUIDE:
- "setup kafka" / "get started"         → setup_kafka_and_bi (no args first)
- "which mode" / "help me choose"       → choose_deployment_mode
- "run the demo"                         → run_bi_demo
- "check prerequisites"                  → check_prerequisites
- "start / stop / status kafka"          → start_kafka / stop_kafka / kafka_status
- "show kafka logs"                      → show_kafka_logs
- "list / create topic"                  → list_topics / create_topic
- "produce / consume message"            → produce_test_message / consume_test_message
- "build / run BI project"               → validate_bi_project / run_bi_project
- "inspect config"                       → inspect_bi_kafka_config
- "start/stop/list replicas"             → start_bi_replica / stop_bi_replica / list_bi_replicas
- "consumer group / partition assignment"→ inspect_consumer_group
- "build docker image"                   → build_bi_docker_image
- "start/stop/scale containers"          → start_bi_replicas_containerized / stop_bi_replicas_containerized / scale_bi_replicas
- "error flow tests"                     → run_error_flow_suite
- "diagnostics / logs"                   → show_error_diagnostics
`.trim(),
  },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    const handler = HANDLERS[name];
    if (!handler) {
      return {
        content: [{ type: "text", text: `Unknown tool: ${name}` }],
        isError: true,
      };
    }

    const result = await handler(args);
    return { content: [{ type: "text", text: result }] };
  } catch (err: any) {
    return {
      content: [
        {
          type: "text",
          text: `Tool '${name}' error:\n${err.message}\n${err.stack ?? ""}`,
        },
      ],
      isError: true,
    };
  }
});

// ── Start ─────────────────────────────────────────────────────────────────────
const transport = new StdioServerTransport();
await server.connect(transport);
console.error(
  "[kafka-bi-mcp] Server listening on stdio. Tools: " +
  TOOLS.map((t) => t.name).join(", "),
);
