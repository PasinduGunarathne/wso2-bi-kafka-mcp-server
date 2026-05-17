# kafka-bi-mcp-server

MCP server that lets AI assistants set up, run, and test a local **Apache Kafka + WSO2 Ballerina Integrator** environment — one command to go from zero to a working producer/consumer flow.

Gives AI assistants **15 tools** to set up Kafka, manage topics, produce and consume messages, build and run Ballerina BI projects, and generate sample code — without exposing a shell command interface.

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

You should see a JSON response listing all 15 tools.

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

3. Restart Claude Desktop. The 15 tools will appear automatically.

---

### Claude Code (CLI)

**Project-level** (`.claude/settings.json` in your working directory):

```json
{
  "mcpServers": {
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

**User-level** (`~/.claude/settings.json` — available in every project):

```json
{
  "mcpServers": {
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

Or add it directly from the terminal using the Claude Code CLI:

```bash
# Personal (available in all your projects — recommended for this MCP)
claude mcp add --transport stdio kafka-bi -- node /absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js

# Project-level (shared with team via .mcp.json at the repo root)
claude mcp add --transport stdio --scope project kafka-bi -- node /absolute/path/to/wso2-bi-kafka-mcp-server/dist/index.js
```

Verify it was added:

```bash
claude mcp list
```

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
Reads and displays all `configurable` declarations from `.bal` files and active `Config.toml` overrides. Secrets are masked. Read-only.

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

## Project structure

```
wso2-bi-kafka-mcp-server/
├── resources/
│   └── docker/
│       └── docker-compose.yml     # Bundled Kafka stack (KRaft, no ZooKeeper)
├── src/
│   ├── index.ts                   # MCP server entry point — tool registry + stdio transport
│   ├── config.ts                  # Path defaults, smart WSO2I detection, constants
│   ├── types.ts                   # Shared TypeScript types
│   ├── tools/
│   │   ├── prerequisites.ts       # check_prerequisites (with env layout block)
│   │   ├── setup.ts               # setup_kafka_and_bi, run_bi_demo
│   │   ├── kafka.ts               # start_kafka, stop_kafka, kafka_status, show_kafka_logs
│   │   ├── kafka-admin.ts         # list_topics, create_topic
│   │   ├── kafka-messages.ts      # produce_test_message, consume_test_message
│   │   └── bi.ts                  # validate_bi_project, run_bi_project,
│   │                              #   inspect_bi_kafka_config, generate_bi_kafka_sample
│   └── utils/
│       ├── docker.ts              # Docker / Compose CLI wrappers (no shell injection)
│       ├── logger.ts              # Emoji-formatted output helpers
│       └── validation.ts          # Input validation and secret masking
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

# Clean compiled output and rebuild
npm run clean && npm run build
```

To add a new tool:
1. Add the handler function to the appropriate file in `src/tools/`
2. Register it in `HANDLERS` and `TOOLS` in `src/index.ts`
3. Run `npm run build`

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
