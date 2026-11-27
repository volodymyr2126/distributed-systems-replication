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
| `REPLICATION_TIMEOUT` | `60` | Seconds the master waits for a secondary ACK before the attempt counts as failed and is retried; keep above `SLEEP_AFTER_REQUESTS` |
| `RETRY_INITIAL_DELAY` | `0.5` | First retry delay in seconds (doubles per failed attempt, with jitter) |
| `RETRY_MAX_DELAY` | `10` | Upper bound of the retry delay in seconds |
| `HEARTBEAT_INTERVAL` | `2` | Seconds between master heartbeats to each secondary |
| `HEARTBEAT_TIMEOUT` | `2` | Seconds before a heartbeat counts as missed |
| `HEARTBEAT_UNHEALTHY_AFTER` | `3` | Consecutive missed heartbeats after which a secondary is `unhealthy` (1+ missed = `suspected`) |
| `LOG_LEVEL` | `INFO` | Application log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` |

Each secondary registers with a node ID, so the master still recognises it after
its address changes and never counts it twice. Docker Compose sets
`NODE_ID_FILE`: the ID is generated once and kept in a per-replica anonymous
volume, which survives restarts and recreation by `docker compose up`. A
secondary removed with `docker compose rm` or `down` gets a new ID; use
`docker compose down -v` to also delete the volumes. Outside Compose, set a unique `NODE_ID` (or `NODE_ID_FILE`) per secondary; with neither,
the master identifies it by URL.

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

### Iteration 3: retries, total order, health, quorum

- **Retries**: the master keeps one sender per secondary that delivers the log in
  id order and retries failed attempts (connection error, timeout, 5xx) forever
  with exponential backoff and jitter. A secondary that was down, joins late or
  restarts is caught up automatically with every message it is missing.
- **Write concern**: `w > 1` blocks the client until `w-1` secondaries have
  acknowledged, however long that takes. Only that request waits; other clients
  keep being served. Writes are accepted before all secondaries have registered.
- **Secondaries** deduplicate by message id and buffer out-of-order messages: a
  message is shown by `GET /message` only after all earlier ids arrived.
- **Heartbeats**: the master pings `GET /health` of every secondary; status moves
  `healthy` -> `suspected` -> `unhealthy` and back. `GET /health` on the master
  returns `status`, `read_only`, `quorum`, `alive_nodes` and per-secondary
  `status`/`acked_up_to`.
- **Quorum**: the cluster is the master plus `SECONDARY_REPLICAS_COUNT`
  secondaries; quorum is a majority. Without a quorum of non-`unhealthy` nodes the
  master is read-only: `POST /message` returns 503 ("No quorum ... read-only")
  while `GET /message` keeps working.

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

The iteration 3 harness tests (`test_iteration3_harness.py`) cover retries via
`transport.drop_message`/`remove_drop`, deduplication of delayed retries, total
order with a hidden gap, late-joining secondaries (self-check acceptance scenario)
and parallel clients not being blocked. `test_write_concern.py` covers the same
semantics over HTTP, plus read-only mode and `/health`.

### HTTP integration tests (Docker services must be running)

The integration test discovers the master and secondary published ports from
the current Compose project and reads the configured secondary delay directly
from Compose. Start the services, then run:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_http_integration.py' -v
```

If the Compose services are not running, the integration test is skipped. Start
the stack with `docker compose up --build -d` first.
