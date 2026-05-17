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
 */

import { Server }               from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

// ── Tool handlers ────────────────────────────────────────────────────────────
import { setupKafkaAndBi, runBiDemo } from "./tools/setup.js";
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

// ── Handler registry ─────────────────────────────────────────────────────────
const HANDLERS: Record<string, (args: any) => Promise<string>> = {
  // Setup & demo
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
};

// ── Tool definitions (MCP schema) ────────────────────────────────────────────
const TOOLS = [

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
      "'set up the environment', 'get started'. " +
      "When setup is complete, the tool instructs you to call run_bi_demo — " +
      "present that option to the user and wait for their approval before calling it.",
    inputSchema: {
      type: "object",
      properties: {
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
];

// ── MCP Server ───────────────────────────────────────────────────────────────
const server = new Server(
  { name: "kafka-bi-mcp-server", version: "1.0.0" },
  { capabilities: { tools: {} } },
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
