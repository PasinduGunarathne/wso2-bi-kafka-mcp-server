# kafka-bi-mcp-server

MCP server that lets AI assistants set up, run, and test a local **Apache Kafka + WSO2 Ballerina Integrator** environment — one command to go from zero to a working producer/consumer flow.

Gives AI assistants **36 tools** to set up Kafka, manage topics, produce and consume messages, build and run Ballerina BI projects, generate sample code, run full **erroneous-flow test suites**, and deploy **multi-replica consumer groups** both locally and as Docker containers — without exposing a shell command interface.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| [Node.js](https://nodejs.org/) | 18+ | Run the MCP server |
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) or [Rancher Desktop](https://rancherdesktop.io/) | Latest | Run Kafka containers |
| [Ballerina](https://ballerina.io/downloads/) | 2201.x (Swan Lake) | Build and run BI projects |
| [Git](https://git-scm.com/) | Any | Clone the repo |

> **Not sure what's missing?** Call `check_prerequisites` from your AI client — it will check every dependency and print platform-specific install instructions for anything that's missing.

---

## Installation

### 1. Clone or locate the project

```bash
# If you already have the project directory:
cd /path/to/wso2-bi-kafka-mcp-server

# Or clone from source:
git clone <repo-url>
cd wso2-bi-kafka-mcp-server
```

### 2. Install dependencies

```bash
npm install
```

### 3. Build the server

```bash
npm run build
```

This compiles TypeScript to `dist/`. The entry point is `dist/index.js`.

### 4. Verify the build

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | node dist/index.js
```

You should see a JSON response listing all 35 tools.

---

## Default project paths (auto-detected)

The server automatically detects the best project location for your machine. **No configuration required.**

### Priority order

1. **WSO2 Integrator installed** (`~/WSO2Integrator/` exists)  
   → `~/WSO2Integrator/kafka-bi-demo/kafkaintegration/`

2. **macOS or Windows** (no WSO2 Integrator)  
   → `~/Documents/BallerinaProjects/kafka-bi-demo/kafkaintegration/`

3. **Linux** (no WSO2 Integrator)  
   → `~/BallerinaProjects/kafka-bi-demo/kafkaintegration/`

### Path cheat-sheet

| Platform | WSO2 Integrator installed | Default project path |
|----------|--------------------------|----------------------|
| macOS (Intel / ARM64) | Yes | `~/WSO2Integrator/kafka-bi-demo/kafkaintegration/` |
| macOS (Intel / ARM64) | No  | `~/Documents/BallerinaProjects/kafka-bi-demo/kafkaintegration/` |
| Windows | Yes | `%USERPROFILE%\WSO2Integrator\kafka-bi-demo\kafkaintegration\` |
| Windows | No  | `%USERPROFILE%\Documents\BallerinaProjects\kafka-bi-demo\kafkaintegration\` |
| Linux | Yes | `~/WSO2Integrator/kafka-bi-demo/kafkaintegration/` |
| Linux | No  | `~/BallerinaProjects/kafka-bi-demo/kafkaintegration/` |

### Using an existing Ballerina project

Pass the path explicitly to any tool that accepts `biProjectPath` or `projectPath`:

```
check_prerequisites { "biProjectPath": "/path/to/your/project" }
setup_kafka_and_bi  { "projectPath":   "/path/to/your/project" }
```

### Other fixed defaults

| Setting | Value |
|---------|-------|
| Kafka Docker Compose | Bundled in `resources/docker/docker-compose.yml` |
| Kafka bootstrap server | `localhost:9092` |
| Kafka UI | `http://localhost:8080` |

---

## Adding the MCP to AI clients

Replace `/absolute/path/to/wso2-bi-kafka-mcp-server` with the actual path on your machine.

### Claude Desktop

1. Open **Claude Desktop** → Settings → Developer → Edit Config, or open the config file directly:

   ```
   # macOS
   ~/Library/Application Support/Claude/claude_desktop_config.json

   # Windows
   %APPDATA%\Claude\claude_desktop_config.json
   ```

2. Add the server under `mcpServers`:

   ```json
   {
     "mcpServers": {
       "kafka-bi": {
         "command": "node",
         "args": [
           "/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js"
         ]
       }
     }
   }
   ```

3. Restart Claude Desktop. The 35 tools will appear automatically.

---

### Claude Code (CLI)

Claude Code uses **dedicated MCP config files**, not `settings.json`. MCP servers never go in `settings.json`.

#### Recommended — CLI commands (writes the correct file automatically)

```bash
# User scope — available in all your projects (recommended for personal use)
claude mcp add --scope user --transport stdio kafka-bi -- node /absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js

# Project scope — shared with your team via .mcp.json at the repo root
claude mcp add --scope project --transport stdio kafka-bi -- node /absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js
```

Verify:

```bash
claude mcp list
```

#### Manual — edit the config files directly

**User scope** (`~/.claude.json` — available in all your projects):

```json
{
  "mcpServers": {
    "kafka-bi": {
      "command": "node",
      "args": ["/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js"],
      "type": "stdio"
    }
  }
}
```

**Project scope** (`.mcp.json` at your **project root** — commit this to share with your team):

```json
{
  "mcpServers": {
    "kafka-bi": {
      "command": "node",
      "args": ["/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js"],
      "type": "stdio"
    }
  }
}
```

> **Note:** `~/.claude/.mcp.json` is not a valid path. User-scope MCP config lives in `~/.claude.json` (top-level key). Project-scope config lives in `.mcp.json` at the project root, not inside `.claude/`.

---

### Cursor

1. Open **Cursor** → Settings → Features → MCP (or `Cursor Settings > MCP`).
2. Click **Add new MCP server**.
3. Fill in:
   - **Name:** `kafka-bi`
   - **Type:** `stdio`
   - **Command:** `node`
   - **Args:** `/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js`

Or add directly to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "kafka-bi": {
      "command": "node",
      "args": [
        "/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js"
      ]
    }
  }
}
```

Restart Cursor after saving.

---

### Windsurf

Add to `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "kafka-bi": {
      "command": "node",
      "args": [
        "/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js"
      ]
    }
  }
}
```

Restart Windsurf after saving.

---

### VS Code (with Copilot or Continue.dev)

**Continue.dev** — add to `~/.continue/config.json`:

```json
{
  "mcpServers": [
    {
      "name": "kafka-bi",
      "command": "node",
      "args": [
        "/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js"
      ]
    }
  ]
}
```

**GitHub Copilot (VS Code extension with MCP support)** — add to VS Code `settings.json`:

```json
{
  "github.copilot.mcp.servers": {
    "kafka-bi": {
      "command": "node",
      "args": [
        "/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js"
      ],
      "type": "stdio"
    }
  }
}
```

---

### Zed

Add to `~/.config/zed/settings.json`:

```json
{
  "context_servers": {
    "kafka-bi": {
      "command": {
        "path": "node",
        "args": [
          "/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js"
        ]
      }
    }
  }
}
```

---

### Any MCP-compatible client (generic)

The server uses **stdio transport** — the standard for local MCP servers.

- **Command:** `node`
- **Args:** `["/absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js"]`
- **Transport:** `stdio`
- **Protocol:** JSON-RPC 2.0 over stdin/stdout

---

## Usage guide

Once configured, interact with the tools through your AI assistant using natural language. The assistant will call the correct tools automatically.

### Choosing a deployment mode

Before running setup, call `choose_deployment_mode` to get a guided comparison of all three ways to run the Ballerina Kafka integration:

```
"Help me choose a deployment mode"
"How should I deploy the Ballerina Kafka integration?"
"What's the difference between standalone and multi-replica?"
```

The tool presents a side-by-side comparison of the three modes with trade-off notes and decision questions. Once the user picks a mode, call it again with the chosen mode to get an exact step-by-step tool sequence:

```
choose_deployment_mode { mode: "standalone" }
choose_deployment_mode { mode: "local-replicas" }
choose_deployment_mode { mode: "containerized" }
```

| Mode | Best for | Key trade-off |
|------|----------|---------------|
| `standalone` | First-time setup, development, demos | Single consumer — all partitions on one instance |
| `local-replicas` | Testing consumer-group behaviour | In-memory — replicas lost on MCP server restart |
| `containerized` | Integration testing, pre-production | Requires a one-time Docker image build (~2 min) |

---

### Quick start (one command)

The fastest way to get everything running:

```
"Set up Kafka and Ballerina for me"
```

This single phrase triggers `setup_kafka_and_bi`, which:
1. Checks prerequisites (Docker, Ballerina, ports)
2. Starts the Kafka broker and Kafka UI
3. Creates the demo topics (`bi.orders.in`, `bi.orders.out`)
4. Generates a working Ballerina BI project with producer + consumer flows
5. Compiles it with `bal build`
6. Asks if you want to run the end-to-end demo

After confirming, say **"Yes, run the demo"** to trigger `run_bi_demo`, which produces a test order event, runs the BI listener, and verifies the processed result appears on the output topic.

---

### Step-by-step workflow

```
1. check_prerequisites      → verify Docker, Ballerina, paths, ports
2. setup_kafka_and_bi       → one-command: start Kafka + generate + build BI project
3. run_bi_demo              → end-to-end demo (produce → consume → verify)
```

Or manually, step by step:

```
1. check_prerequisites      → verify Docker, Ballerina, paths, ports
2. start_kafka              → start Kafka broker + UI
3. kafka_status             → confirm everything is running
4. create_topic             → create your topic
5. produce_test_message     → send a test message
6. consume_test_message     → verify the message arrived
7. inspect_bi_kafka_config  → review BI project configuration
8. validate_bi_project      → compile the BI project
9. run_bi_project           → start the Kafka listener
10. stop_kafka              → shut down when done
```

### Example prompts

```
"Check if everything is set up correctly for Kafka and BI development."

"Set up Kafka and Ballerina for me."

"Start Kafka and tell me the bootstrap server address."

"Create a topic called orders with 3 partitions."

"Send a test order event to the orders topic."

"Consume the last 5 messages from the orders topic."

"Show me the Kafka configuration used by the BI project."

"Build the BI project and show me any errors."

"Run the BI Kafka listener."

"Generate a new BI Kafka sample project at ~/workspace/my-kafka-app with topic name payments."

"Show the last 100 lines of Kafka logs."

"Stop Kafka when I'm done."
```

---

## Tool reference

### `choose_deployment_mode`
Interactive wizard that helps the user pick a deployment mode. Call with **no arguments** to see a side-by-side comparison of all three modes. Call with `mode` to get the exact step-by-step tool sequence.

| Argument | Type | Values | Description |
|----------|------|--------|-------------|
| `mode` | string | `standalone` \| `local-replicas` \| `containerized` | Omit to see the comparison; provide to get the mode-specific guide |

The AI assistant is instructed to call this tool automatically at the start of any Kafka + BI setup conversation unless the user has already stated their preferred mode.

---

### `setup_kafka_and_bi`
One-command setup: starts Kafka, creates demo topics, generates and compiles a Ballerina BI project with consumer + producer flows. Detects the best project location automatically (see [Default project paths](#default-project-paths-auto-detected) above).

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `projectPath` | string | auto-detected | Where to generate the BI project. Accepts an existing project path. |
| `kafkaComposePath` | string | bundled | Directory containing `docker-compose.yml` |

---

### `run_bi_demo`
End-to-end demo: produces a test `OrderEvent` → starts the BI listener → verifies the processed result on the output topic. Call this after `setup_kafka_and_bi`.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `projectPath` | string | auto-detected | BI project directory (same smart-path logic as setup) |
| `kafkaComposePath` | string | bundled | Kafka Compose directory |
| `includeNegativeTests` | boolean | `false` | After the success demo, also run invalid-JSON and schema-mismatch error-flow tests |

---

### `check_prerequisites`
Checks Docker (installed + daemon running), Docker Compose v2, Node.js, Ballerina CLI, Kafka Compose file, BI project directory, and port availability (9092, 8080). Shows platform-specific install instructions for anything missing, plus a full **environment layout** block showing the auto-detected project path and how to use an existing project.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `biProjectPath` | string | auto-detected | Path to an existing Ballerina BI project |
| `kafkaComposePath` | string | bundled | Directory containing `docker-compose.yml` |

---

### `start_kafka`
Starts the Kafka stack (broker on port 9092 + Kafka UI on port 8080) using the bundled Docker Compose file. Waits up to 60s for broker readiness.

| Argument | Type | Default |
|----------|------|---------|
| `kafkaComposePath` | string | bundled |

Returns: bootstrap server address, Kafka UI URL.

---

### `stop_kafka`
Stops Kafka containers. Preserves data volumes by default.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `kafkaComposePath` | string | bundled | |
| `deleteVolumes` | boolean | `false` | Also delete the `kafka-storage` volume (data is lost) |

---

### `kafka_status`
Shows container state, broker liveness, and Kafka UI reachability.

| Argument | Type | Default |
|----------|------|---------|
| `kafkaComposePath` | string | bundled |

---

### `show_kafka_logs`
Shows recent container logs. Sensitive values (passwords, tokens) are automatically redacted.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `kafkaComposePath` | string | bundled | |
| `service` | `kafka` \| `kafka-ui` | all services | Service to filter logs |
| `lines` | number | `50` | Lines to return (max 500) |

---

### `list_topics`
Lists all Kafka topics, separating user topics from internal ones (`__consumer_offsets`, etc.).

| Argument | Type | Default |
|----------|------|---------|
| `kafkaComposePath` | string | bundled |

---

### `create_topic`
Creates a Kafka topic. Idempotent — safe to call if the topic already exists.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `topicName` | string | **required** | Letters, digits, `.`, `_`, `-` only |
| `partitions` | number | `1` | Number of partitions |
| `replicationFactor` | number | `1` | Replication factor |
| `kafkaComposePath` | string | bundled | |

---

### `produce_test_message`
Produces a message to a topic. Sends a default JSON payload if no message is provided. Message is piped via stdin — no shell injection possible.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `topicName` | string | **required** | Target topic |
| `message` | string | auto JSON | Custom message payload |
| `kafkaComposePath` | string | bundled | |

---

### `consume_test_message`
Consumes a limited number of messages. Always times out — never hangs.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `topicName` | string | **required** | Topic to read from |
| `maxMessages` | number | `5` | Maximum messages to return |
| `fromBeginning` | boolean | `true` | Start from earliest offset |
| `kafkaComposePath` | string | bundled | |

---

### `validate_bi_project`
Runs `bal build` in the BI project. Read-only — does not modify any files.

| Argument | Type | Default |
|----------|------|---------|
| `biProjectPath` | string | auto-detected |

---

### `run_bi_project`
Starts the BI project with `bal run`. Captures 15 seconds of startup output. Never overwrites `Config.toml`.

| Argument | Type | Default |
|----------|------|---------|
| `biProjectPath` | string | auto-detected |

---

### `inspect_bi_kafka_config`
Reads and displays all `configurable` declarations from `.bal` files and active `Config.toml` overrides. Secrets are masked. Read-only. Also shows a **Commit Behaviour** section (auto-commit flag, manual commit position relative to the foreach loop) and a **Dead-Letter Queue (DLQ)** section.

| Argument | Type | Default |
|----------|------|---------|
| `biProjectPath` | string | auto-detected |

---

### `generate_bi_kafka_sample`
Generates a new Ballerina BI Kafka project skeleton at a specified directory. Will not write inside the existing BI project.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `targetPath` | string | **required** | Absolute output directory |
| `packageName` | string | `kafkasample` | Ballerina package name |
| `orgName` | string | `myorg` | Ballerina org name |
| `topicName` | string | `events` | Kafka topic to subscribe to |
| `groupId` | string | `<packageName>-consumer` | Consumer group ID |
| `bootstrapServers` | string | `localhost:9092` | Kafka bootstrap servers |

Generated files: `Ballerina.toml`, `config.bal`, `types.bal`, `connections.bal`, `main.bal`, `functions.bal`.

---

## Erroneous flow testing

The server includes 10 additional tools for simulating failures, inspecting error-handling behaviour, and verifying recovery — all non-destructive and safe to run against your real project.

### `run_error_flow_suite`
Orchestrates all error-flow sub-tests in sequence and returns a structured report. The quickest way to run a full error-flow pass.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `projectPath` | string | auto-detected | BI project directory |
| `inputTopic` | string | `bi.orders.in` | Input topic |
| `outputTopic` | string | `bi.orders.out` | Output topic |
| `kafkaComposePath` | string | bundled | |
| `includeManualCommitRedeliveryTest` | boolean | `false` | Include the commit/re-delivery analysis test |
| `includeDlqCheck` | boolean | `false` | Include the DLQ inspection test |
| `includeKafkaUnavailableTest` | boolean | `false` | Temporarily stop Kafka and observe behaviour (**runs last**) |
| `timeoutSeconds` | number | `30` | Per-test capture window |

---

### `trigger_invalid_json_error`
Produces a malformed JSON payload (`{ "orderId": "ORD-ERR-001", "amount": }`) to the input topic. Explains the expected Ballerina parse error and the log-and-continue commit behaviour.

| Argument | Type | Default |
|----------|------|---------|
| `topicName` | string | `bi.orders.in` |
| `kafkaComposePath` | string | bundled |
| `projectPath` | string | auto-detected |

---

### `trigger_schema_mismatch_error`
Produces a structurally valid but schema-incompatible payload. Use `variant` to choose between a missing-field payload and a wrong-type payload (e.g. `amount: "not-a-number"`).

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `topicName` | string | `bi.orders.in` | |
| `variant` | `missing-field` \| `wrong-type` | `missing-field` | Which schema violation to inject |
| `kafkaComposePath` | string | bundled | |
| `projectPath` | string | auto-detected | |

---

### `trigger_business_rule_error`
Produces a valid-JSON, valid-schema payload with a negative `amount` (-100.0). Inspects `functions.bal` for amount-validation logic. If none is found, the message is processed successfully and the tool reports a `warning` with a recommendation to add a guard.

| Argument | Type | Default |
|----------|------|---------|
| `topicName` | string | `bi.orders.in` |
| `kafkaComposePath` | string | bundled |
| `projectPath` | string | auto-detected |

---

### `test_missing_topic_error`
Generates a unique throwaway topic name and attempts to describe it, produce a message, then describe it again. Reports how `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` (the bundled Compose default) means a missing topic is created rather than causing an error, and explains how to reproduce a true missing-topic failure.

| Argument | Type | Default |
|----------|------|---------|
| `kafkaComposePath` | string | bundled |

---

### `test_consumer_not_running_flow`
Produces a valid order to the input topic without starting the BI listener. Verifies the output topic stays empty (timeout as expected), then confirms the input message is retained by Kafka's log-retention policy — ready to be consumed when the listener starts next.

| Argument | Type | Default |
|----------|------|---------|
| `topicName` | string | `bi.orders.in` |
| `kafkaComposePath` | string | bundled |

---

### `test_manual_commit_redelivery`
Inspects the BI project source with static analysis (`detectCommitBehavior`) to determine whether the manual `caller->commit()` call is inside or outside the foreach loop. Runs the BI listener with a short capture window, observes whether failed messages cause the commit to be skipped, and reports the actual re-delivery behaviour. Optionally re-runs with the same group ID to confirm.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `projectPath` | string | auto-detected | |
| `kafkaComposePath` | string | bundled | |
| `includeRestart` | boolean | `false` | Re-run with the same group ID to verify no re-delivery |

---

### `check_dlq`
Scans `.bal` sources for dead-letter queue patterns (`dlqTopic`, `deadLetterTopic`, `errorTopic` configurables + matching producer send calls). If a DLQ is found, consumes up to 10 messages from it. If not found, returns actionable guidance for adding DLQ support.

| Argument | Type | Default |
|----------|------|---------|
| `projectPath` | string | auto-detected |
| `kafkaComposePath` | string | bundled |

---

### `show_error_diagnostics`
Pulls recent Kafka broker logs, optionally reads BI runtime log files from the project's `target/` directory, applies known-pattern highlighting (❌ for errors, ⚠️ for warnings), and returns a pattern-occurrence summary. Sensitive values are masked.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `kafkaComposePath` | string | bundled | |
| `projectPath` | string | auto-detected | |
| `lines` | number | `100` | Lines of Kafka logs to retrieve |

---

### `generate_error_flow_report`
Formats a structured report from an array of `ErrorFlowResult` objects. Includes a summary table (test name / status / topic / recommendation) and a per-test detail section. Returns a final verdict and confirms no project files were modified.

| Argument | Type | Description |
|----------|------|-------------|
| `results` | `ErrorFlowResult[]` | **required** — results array from `run_error_flow_suite` or individual test tools |
| `title` | string | Optional report title |

---

### Example prompts for error-flow testing

```
"Run the full error-flow test suite on my BI project."

"Inject a malformed JSON payload into the orders topic and show me what Ballerina does."

"Produce a message with a missing required field and check the schema mismatch error."

"Send an order with a negative amount and see if the BI project validates it."

"What happens when I produce to a topic that doesn't exist?"

"Produce a message to the input topic but don't start the BI listener — what happens to the message?"

"Check whether the BI project re-delivers failed messages after a restart."

"Does the BI project have a dead-letter queue?"

"Highlight any errors in the Kafka broker logs."

"Generate a detailed report of the last error-flow test run."
```

---

## Multi-replica deployment

The server supports two deployment modes for running multiple Ballerina Kafka consumer instances against a single Kafka broker.

| | Local process replicas | Containerized replicas |
|---|---|---|
| **How it works** | Multiple `bal run` processes on the host, tracked in-memory | N Docker containers, each running the compiled JAR |
| **Kafka bootstrap** | `localhost:9092` | `kafka:9093` (internal Docker network) |
| **Lifecycle** | Tied to the MCP server session | Independent — survive MCP server restarts |
| **Prerequisites** | Compiled project (`validate_bi_project`) | Built Docker image (`build_bi_docker_image`) |
| **Partition balance** | Kafka auto-rebalances on each start/stop | Kafka auto-rebalances on each scale operation |
| **Best for** | Quick local testing of consumer-group behaviour | Production-like multi-replica validation |

### Quick start — local process replicas

```
1. validate_bi_project                  → ensure the project is compiled
2. start_bi_replica { groupId: "order-processor" }   → start replica 1
3. start_bi_replica { groupId: "order-processor" }   → start replica 2
4. list_bi_replicas                     → see both running
5. inspect_consumer_group { groupId: "order-processor" }  → see partition assignment
6. stop_all_bi_replicas                 → clean up
```

### Quick start — containerized replicas

```
1. build_bi_docker_image                → compile + build Docker image
2. generate_bi_docker_compose           → create docker-compose.bi.yml
3. start_bi_replicas_containerized { replicas: 3 }   → start 3 containers
4. inspect_consumer_group { groupId: "order-processor" }  → see partition assignment
5. scale_bi_replicas { replicas: 2 }    → scale down to 2
6. stop_bi_replicas_containerized       → clean up
```

### `start_bi_replica`
Start a Ballerina BI Kafka listener as a persistent background process. Multiple replicas with the same `groupId` form a Kafka consumer group — Kafka distributes partitions across them automatically.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `projectPath` | string | auto-detected | BI project directory |
| `groupId` | string | `order-processor` | Consumer group ID (shared by all replicas) |
| `instanceId` | string | auto-generated | Unique name for this replica |
| `configOverrides` | object | `{}` | Extra `-Ckey=value` overrides (e.g. `{ kafkaTopic: "payments" }`) |

---

### `stop_bi_replica`
Stop a specific local replica by `instanceId`. Sends SIGTERM then SIGKILL if needed.

| Argument | Type | Required |
|----------|------|---------|
| `instanceId` | string | **required** |

---

### `stop_all_bi_replicas`
Stop all running local replicas. Optionally filter by project path.

| Argument | Type | Default |
|----------|------|---------|
| `projectPath` | string | all projects |

---

### `list_bi_replicas`
Show all running local replicas — instance ID, PID, consumer group, uptime, status, and a tail of recent log output. No arguments required.

---

### `inspect_consumer_group`
Show the Kafka consumer group state: partition assignment, current offset, log-end offset, and lag per partition. Works for both local and containerized replicas.

| Argument | Type | Required |
|----------|------|---------|
| `groupId` | string | **required** |
| `kafkaComposePath` | string | bundled |

---

### `build_bi_docker_image`
Build a Docker image from the Ballerina project. Steps: `bal build` → auto-generate `Dockerfile` (eclipse-temurin:21-jre base) → `docker build`.

| Argument | Type | Default |
|----------|------|---------|
| `projectPath` | string | auto-detected |
| `imageName` | string | `bi-kafka-demo` |
| `tag` | string | `latest` |

---

### `generate_bi_docker_compose`
Generate a `docker-compose.bi.yml` for the BI service. The service joins the `kafka-local` Docker network and uses `kafka:9093` as the bootstrap server. Consumer group ID, topics, and image name are read from `config.bal` by default.

| Argument | Type | Default |
|----------|------|---------|
| `projectPath` | string | auto-detected |
| `imageName` | string | `bi-kafka-demo` |
| `tag` | string | `latest` |
| `groupId` | string | from `config.bal` |
| `inputTopic` | string | from `config.bal` |
| `outputTopic` | string | from `config.bal` |
| `outputComposeFile` | string | `docker-compose.bi.yml` |

---

### `start_bi_replicas_containerized`
Start N Docker container replicas of the BI listener. Requires Kafka to be running and the Docker image to be built.

| Argument | Type | Default |
|----------|------|---------|
| `projectPath` | string | auto-detected |
| `composeFile` | string | `docker-compose.bi.yml` |
| `replicas` | number | `2` |
| `kafkaComposePath` | string | bundled |

---

### `stop_bi_replicas_containerized`
Stop and remove all containerized BI replicas. Kafka broker and topics are not affected.

| Argument | Type | Default |
|----------|------|---------|
| `projectPath` | string | auto-detected |
| `composeFile` | string | `docker-compose.bi.yml` |

---

### `scale_bi_replicas`
Scale the number of running containers up or down. Kafka rebalances partitions automatically. Set `replicas: 0` to stop all without removing the compose file.

| Argument | Type | Required | Default |
|----------|------|---------|---------|
| `replicas` | number | **required** | — |
| `projectPath` | string | | auto-detected |
| `composeFile` | string | | `docker-compose.bi.yml` |
| `kafkaComposePath` | string | | bundled |

---

### Example prompts for multi-replica deployment

```
"Start 3 local BI replicas sharing the same consumer group."

"Show me the running replicas and their status."

"Show the Kafka partition assignment across my replicas."

"Stop replica bi-replica-1234567890."

"Stop all local BI replicas."

"Build a Docker image from my BI project."

"Generate the Docker Compose file for the BI service."

"Start 4 containerized BI replicas."

"Scale down to 2 replicas."

"Stop all containerized BI replicas."
```

---

## Project structure

```
wso2-bi-kafka-mcp-server/
├── fixtures/
│   └── error-flows/
│       ├── invalid-json.txt               # Malformed JSON fixture
│       ├── schema-missing-field.json      # Valid JSON, missing required fields
│       ├── schema-wrong-type.json         # Valid JSON, wrong field types
│       ├── business-invalid-amount.json   # Valid JSON, negative amount
│       └── valid-order.json               # Baseline valid OrderEvent
├── resources/
│   └── docker/
│       └── docker-compose.yml     # Bundled Kafka stack (KRaft, no ZooKeeper)
├── src/
│   ├── index.ts                   # MCP server entry point — tool registry + stdio transport
│   ├── config.ts                  # Path defaults, smart WSO2I detection, constants
│   ├── types.ts                   # Shared TypeScript types (incl. ErrorFlowResult, ReplicaInfo)
│   ├── tools/
│   │   ├── prerequisites.ts       # check_prerequisites (with env layout block)
│   │   ├── setup.ts               # setup_kafka_and_bi, run_bi_demo
│   │   ├── kafka.ts               # start_kafka, stop_kafka, kafka_status, show_kafka_logs
│   │   ├── kafka-admin.ts         # list_topics, create_topic
│   │   ├── kafka-messages.ts      # produce_test_message, consume_test_message
│   │   ├── bi.ts                  # validate_bi_project, run_bi_project,
│   │   │                          #   inspect_bi_kafka_config, generate_bi_kafka_sample
│   │   ├── error-flows.ts         # 10 erroneous-flow tools
│   │   ├── replica-manager.ts     # 5 local process replica tools
│   │   └── containerized.ts       # 5 Docker-based replica tools
│   └── utils/
│       ├── docker.ts              # Docker / Compose CLI wrappers + spawnBackground()
│       ├── logger.ts              # Emoji-formatted output helpers
│       ├── validation.ts          # Input validation and secret masking
│       ├── log-patterns.ts        # Pure-function error pattern detection utilities
│       └── log-patterns.test.ts   # Vitest unit tests for log-patterns
├── dist/                          # Compiled JavaScript (git-ignored)
├── package.json
├── tsconfig.json
└── README.md
```

---

## Development

```bash
# Run without building (uses tsx)
npm run dev

# Rebuild after changes
npm run build

# Run unit tests (pure-function utilities, no Docker or Ballerina required)
npm test

# Clean compiled output and rebuild
npm run clean && npm run build
```

To add a new tool:
1. Add the handler function to the appropriate file in `src/tools/`
2. Register it in `HANDLERS` and `TOOLS` in `src/index.ts`
3. Run `npm run build`
4. If you add pure utility functions, add tests to `src/utils/*.test.ts` and run `npm test`

---

## Safety guarantees

- No generic shell execution — only allowlisted commands via `execa`
- All paths validated before use
- Message payloads sent via stdin — never interpolated into shell commands
- Sensitive values masked in all output (`password`, `secret`, `token`, `key`)
- Write operations restricted to explicitly provided output directories
- `stop_kafka` preserves volumes by default; deletion requires explicit `deleteVolumes: true`
- `generate_bi_kafka_sample` refuses to write inside the existing BI project
- `run_bi_project` never overwrites `Config.toml`
- No destructive Docker commands (`docker system prune`, etc.)
- Runtime container name resolution — never assumes a hardcoded container name
