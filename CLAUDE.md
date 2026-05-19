# Claude Code Instructions — kafka-bi-mcp-server

## Always use the kafka-bi MCP tools

This project ships an MCP server (`kafka-bi`) that provides all Kafka and Ballerina
operations as structured tools. **Always call the kafka-bi MCP tools instead of running
raw Bash commands** for any Kafka or Ballerina task.

Do NOT use `docker compose`, `bal build`, `bal run`, `kafka-topics.sh`, or similar
commands directly. Use the corresponding MCP tool instead.

## Tool mapping

| User intent                              | Call this tool                          |
|------------------------------------------|-----------------------------------------|
| "setup kafka and ballerina" / "get started" | `setup_kafka_and_bi` (no args first — shows deployment mode picker) |
| "which mode should I use"                | `choose_deployment_mode`                |
| "run the demo"                           | `run_bi_demo`                           |
| "check prerequisites"                    | `check_prerequisites`                   |
| "start kafka"                            | `start_kafka`                           |
| "stop kafka"                             | `stop_kafka`                            |
| "kafka status" / "is kafka running"      | `kafka_status`                          |
| "show kafka logs"                        | `show_kafka_logs`                       |
| "list topics"                            | `list_topics`                           |
| "create topic"                           | `create_topic`                          |
| "produce a message" / "send a message"   | `produce_test_message`                  |
| "consume messages" / "read messages"     | `consume_test_message`                  |
| "build / validate BI project"            | `validate_bi_project`                   |
| "run BI project" / "start the listener"  | `run_bi_project`                        |
| "inspect BI config"                      | `inspect_bi_kafka_config`               |
| "generate BI sample"                     | `generate_bi_kafka_sample`              |
| "run error flow tests"                   | `run_error_flow_suite`                  |
| "start replica" / "scale locally"        | `start_bi_replica`                      |
| "stop replica"                           | `stop_bi_replica`                       |
| "list replicas"                          | `list_bi_replicas`                      |
| "consumer group" / "partition assignment"| `inspect_consumer_group`                |
| "build docker image"                     | `build_bi_docker_image`                 |
| "generate docker compose"                | `generate_bi_docker_compose`            |
| "start containers" / "containerized"     | `start_bi_replicas_containerized`       |
| "stop containers"                        | `stop_bi_replicas_containerized`        |
| "scale replicas"                         | `scale_bi_replicas`                     |

## Setup flow — ALWAYS follow this sequence

When the user asks to set up Kafka and Ballerina (any phrasing):

1. Call `setup_kafka_and_bi` with **no arguments**.
   - The tool returns a deployment mode selection table.
   - Present the table to the user and ask them to choose a mode.
2. Call `setup_kafka_and_bi { deploymentMode: "<chosen mode>" }`.
   - The tool runs the full setup and returns mode-specific next steps.
3. Follow the next steps the tool returns.

Never skip the deployment mode picker. Never infer the mode without asking.

## Rules

- Never run `docker compose`, `bal build`, `bal run`, or `kafka-topics.sh` directly.
- Never call `pkill` or `kill` to stop Ballerina processes — use `stop_bi_replica` or `stop_all_bi_replicas`.
- Never read or write files inside the BI project directory directly — use the MCP tools.
- If a kafka-bi tool returns an error, show the error output to the user and ask how to proceed. Do not try to fix it with raw Bash commands.
