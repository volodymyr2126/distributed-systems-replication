# Distributed Systems Replicated Log

A replicated in-memory message log with one HTTP master and one or more HTTP
secondaries. The master registers secondaries, sends each appended message to all
registered replicas concurrently, and waits for their acknowledgements before
finishing the POST request.

## Requirements

- Python 3.14 or newer
- [uv](https://docs.astral.sh/uv/) for installing Python dependencies
- Docker with the Compose plugin to run the services

## Set up

From the repository root, install the locked dependencies:

```sh
uv sync --locked
```

Optional settings can be changed in `.env`:

| Variable | Default | Description |
| --- | --- | --- |
| `APP_PORT` | `8080` | Host port for the master HTTP API |
| `SECONDARY_REPLICAS_COUNT` | `2` | Number of secondary containers |
| `SLEEP_AFTER_REQUESTS` | `0` | Delay in seconds before a secondary acknowledges a message |
| `LOG_LEVEL` | `INFO` | Application log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` |

## Start the Docker services

Build and start one master and the configured number of secondaries:

```sh
docker compose up --build -d
```

Check service status and the secondary host ports:

```sh
docker compose ps
```

The master is available at `http://127.0.0.1:${APP_PORT:-8080}`. The secondary
ports are dynamically assigned and shown in the `docker compose ps` output.
Follow logs with:

```sh
docker compose logs -f main secondary
```

Stop the services with:

```sh
docker compose down
```

The master API provides `GET /health`, `POST /message?message=...&w=...`, and
`GET /message`. The required `w` parameter is the number of acknowledgements
including the master: `w=1` waits only for the master, while `w=2` waits for
the master and one secondary. It must be between `1` and the configured
secondary count plus one. The master continues replicating to remaining
secondaries after satisfying the requested concern.

Secondaries provide `GET /health`, `POST /message?message=...&message_id=...`
for replication, and `GET /message`. The master assigns message IDs, and each
secondary accepts messages in ID order.

## Run tests

### In-memory simulation (no Docker required)

The simulation and Docker integration tests live together in `tests/`. The
simulation creates `MasterNode`, `SecondaryNode`, and `MockedTransport` objects
in-process. It tests parallel delivery, ACK waiting, delay controls, and
simulated link failures:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_replication_harness.py' -v
```

The iteration 2 harness tests (`test_iteration2_harness.py`) cover write concern
(`append_msg(msg, w=...)`), the delayed-secondary acceptance scenario
(temporary inconsistency, then eventual consistency), and
`transport.reorder_messages(master, secondary, messages_count=2)`, which delivers
batches in reverse; secondaries buffer by message ID so their log stays in order.

### HTTP integration tests (Docker services must be running)

The integration test discovers the master and secondary published ports from
the current Compose project and reads the configured secondary delay directly
from Compose. Start the services, then run:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_http_integration.py' -v
```

If the Compose services are not running, the integration test is skipped. Start
the stack with `docker compose up --build -d` first.
