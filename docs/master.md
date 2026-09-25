# OpenCtrl Master

This document describes the OpenCtrl master process: startup configuration,
HTTP resources, event streaming, state behavior, and runtime process contract.

## Contents

- [Model](#model)
- [Command Line](#command-line)
- [Master URL](#master-url)
- [API Conventions](#api-conventions)
- [Authentication](#authentication)
- [Data Types](#data-types)
- [Master Info](#master-info)
- [Instances](#instances)
- [Events](#events)
- [TCP Ping](#tcp-ping)
- [State](#state)
- [Logging](#logging)
- [Security Notes](#security-notes)
- [Runtime Process Contract](#runtime-process-contract)
- [Client Patterns](#client-patterns)
- [Implementation Limits](#implementation-limits)
- [Release Build](#release-build)

## Model

OpenCtrl runs one master process. The master stores instance definitions and
starts one child process for each running instance:

```text
<runtime-binary> <instance-url>
```

The master owns:

- the HTTP API;
- API-key authentication;
- instance lifecycle state;
- child process start, stop, restart, and delete operations;
- Server-Sent Events;
- gob state persistence;
- TLS termination for the API;
- process-level system information.

Managed child processes own their own URL validation and runtime behavior. A
child should remain in the foreground while the instance is active and should
exit when the master asks it to stop.

## Command Line

```sh
openctrl <configuration-url>
openctrl help
openctrl version
```

Configuration URLs use the `master` scheme:

```sh
openctrl 'master://127.0.0.1:8080'
openctrl 'master://0.0.0.0:8443/admin?tls=1'
openctrl 'master://0.0.0.0:8443?tls=2&crt=/etc/openctrl/fullchain.pem&key=/etc/openctrl/privkey.pem'
```

Unknown URL schemes are rejected before the process starts.

## Master URL

```text
master://<host>:<port>[/prefix][?<query>]
```

The URL host and port become the HTTP listen address. The path configures the
API prefix. If the path is empty or `/`, OpenCtrl uses `/api`. The API version
is appended automatically.

| Master URL | API base |
| --- | --- |
| `master://127.0.0.1:8080` | `http://127.0.0.1:8080/api/v2` |
| `master://127.0.0.1:8080/control` | `http://127.0.0.1:8080/control/v2` |
| `master://127.0.0.1:8443?tls=1` | `https://127.0.0.1:8443/api/v2` |

### Query Parameters

| Parameter | Values | Default | Description |
| --- | --- | --- | --- |
| `tls` | `0`, `1`, `2` | `0` | `0` serves HTTP, `1` serves HTTPS with an in-memory self-signed certificate, `2` serves HTTPS with `crt` and `key`. |
| `crt` | file path | empty | Certificate file used when `tls=2`. |
| `key` | file path | empty | Private key file used when `tls=2`. |
| `bin` | file path | current executable | Runtime binary used to launch managed instances. |

TLS mode `2` reloads the configured certificate and key at most once per hour
when a new TLS handshake asks for a certificate. All TLS-enabled modes require
TLS 1.3 or newer.

## API Conventions

All endpoints are relative to the API base. With the default prefix, the base
is:

```text
/api/v2
```

Requests that carry JSON bodies should send:

```text
Content-Type: application/json
```

Responses are plain JSON resources. There is no success envelope.

| Operation | Success shape |
| --- | --- |
| `GET /info` | master info object |
| `POST /info` | master info object |
| `GET /instances` | instance array |
| `POST /instances` | instance object |
| `GET /instances/{id}` | instance object |
| `PATCH /instances/{id}` | instance object |
| `PUT /instances/{id}` | instance object |
| `DELETE /instances/{id}` | empty body |
| `GET /tcping` | TCP ping result object |

Errors use:

```json
{
  "error": "message"
}
```

### Status Codes

| Status | Meaning |
| --- | --- |
| `200 OK` | Request succeeded. |
| `201 Created` | Instance was accepted and stored. |
| `204 No Content` | Instance was deleted. |
| `400 Bad Request` | Body, URL, action, or query parameter is invalid. |
| `401 Unauthorized` | API key is missing or invalid. |
| `403 Forbidden` | Operation is not allowed on the internal API-key instance. |
| `404 Not Found` | Instance ID does not exist. |
| `405 Method Not Allowed` | Route exists, but the HTTP method is unsupported. |
| `409 Conflict` | Instance ID collision or unchanged replacement URL. |

Connection failures in `GET /tcping` are returned as `200 OK` with
`connected: false` and a string `error` value.

### CORS

Protected routes also handle `OPTIONS` preflight requests. Responses include:

```text
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, PATCH, POST, PUT, DELETE, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization, X-API-Key, Cache-Control
```

Plan deployments as if any origin can attempt to call the API. The API key and
network boundary remain the access-control mechanism.

### Endpoint Table

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/info` | Return master identity, runtime configuration, uptime, and system metrics. |
| `POST` | `/info` | Set the master alias. |
| `GET` | `/instances` | Return all instances, including the internal API-key instance. |
| `POST` | `/instances` | Create an instance and start it asynchronously. |
| `GET` | `/instances/{id}` | Return one instance. |
| `PATCH` | `/instances/{id}` | Update alias, restart policy, metadata, or lifecycle action. |
| `PUT` | `/instances/{id}` | Replace the instance URL and restart the instance. |
| `DELETE` | `/instances/{id}` | Stop and delete the instance. |
| `GET` | `/events` | Open the Server-Sent Events stream. |
| `GET` | `/tcping?target=host:port` | Test TCP reachability from the master process. |

OpenCtrl does not expose a generated API schema route.

### Ordering and Consistency

`GET /instances` is backed by a concurrent map. Treat list ordering as
undefined and sort by `id`, `alias`, or another client-side key when a stable
display order is required.

Lifecycle operations may continue after the HTTP response:

- `POST /instances` stores the instance, starts it in a goroutine, and returns a
  snapshot.
- `PATCH /instances/{id}` returns after recording metadata and policy changes;
  `start`, `stop`, and `restart` run in goroutines.
- `PUT /instances/{id}` stops the old process, stores the new URL, starts the
  replacement, and then returns a snapshot.

Use the event stream as the source of live transitions and periodically refresh
from `GET /instances` if missing an event would be unacceptable.

## Authentication

Protected endpoints require:

```text
X-API-Key: <api-key>
```

On the first successful start, OpenCtrl generates an API key and prints:

```text
Master.run: API key created: <api-key>
```

On later starts, it prints:

```text
Master.run: API key loaded: <api-key>
```

The key is persisted in the internal instance with ID `********`. Its `url`
field stores the API key, and its `config` field stores the master ID.

Regenerate the key:

```sh
curl -X PATCH "${BASE}/instances/********" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"action":"restart"}'
```

The regenerated key is printed to stdout. Existing SSE clients are sent a
`shutdown` event or are closed, and subsequent requests must use the new key.

The internal API-key instance is visible in `GET /instances`. Avoid rendering
or storing its `url` value outside trusted configuration.

## Data Types

The following TypeScript declarations mirror the JSON resources exposed by the
API.

```ts
export type InstanceStatus = "stopped" | "running" | "error";
export type InstanceAction = "start" | "stop" | "restart" | "reset";
export type TlsMode = "0" | "1" | "2";

export interface PeerMeta {
  sid: string;
  type: string;
  alias: string;
}

export interface InstanceMeta {
  peer: PeerMeta;
  tags: Record<string, string>;
}

export interface Instance {
  id: string;
  alias: string;
  type: string;
  status: InstanceStatus;
  url: string;
  config: string;
  restart: boolean;
  meta: InstanceMeta;
  mode: number;
  ping: number;
  pool: number;
  tcps: number;
  udps: number;
  tcprx: number;
  tcptx: number;
  udprx: number;
  udptx: number;
}

export interface MasterInfo {
  mid: string;
  alias: string;
  os: string;
  arch: string;
  noc: number;
  cpu: number;
  mem_total: number;
  mem_used: number;
  swap_total: number;
  swap_used: number;
  netrx: number;
  nettx: number;
  diskr: number;
  diskw: number;
  sysup: number;
  ver: string;
  name: string;
  uptime: number;
  log: string;
  tls: TlsMode;
  crt: string;
  key: string;
}

export interface InstanceEvent {
  type: "initial" | "create" | "update" | "delete" | "log" | "shutdown";
  time: string;
  instance: Instance | null;
  logs: string;
}

export interface TcpPingResult {
  target: string;
  connected: boolean;
  latency: number;
  error: string | null;
}

export interface ApiError {
  error: string;
}
```

Numeric byte counters are JSON numbers. Preserve them as numbers unless your
client environment cannot safely represent large counters; in that case, keep
the latest raw JSON and display rounded values.

## Master Info

Read master info:

```sh
curl -H "X-API-Key: ${API_KEY}" "${BASE}/info"
```

Set master alias:

```sh
curl -X POST "${BASE}/info" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"alias":"prod-master-1"}'
```

The alias is limited to 256 characters and is persisted through the internal
API-key instance.

Example response:

```json
{
  "mid": "1a2b3c4d5e6f7890",
  "alias": "prod-master-1",
  "os": "linux",
  "arch": "amd64",
  "noc": 8,
  "cpu": 12,
  "mem_total": 16777216000,
  "mem_used": 7340032000,
  "swap_total": 0,
  "swap_used": 0,
  "netrx": 123456,
  "nettx": 654321,
  "diskr": 1048576,
  "diskw": 2097152,
  "sysup": 86400,
  "ver": "dev",
  "name": "127.0.0.1",
  "uptime": 42,
  "log": "default",
  "tls": "0",
  "crt": "",
  "key": ""
}
```

### Field Notes

| Field | Unit or meaning |
| --- | --- |
| `mid` | 16-character hexadecimal master ID. |
| `alias` | Operator-defined master name. |
| `os`, `arch` | Go runtime operating system and architecture. |
| `noc` | CPU count from the Go runtime. |
| `cpu` | Linux CPU utilization percentage over a short sample; `-1` elsewhere. |
| `mem_total`, `mem_used` | Bytes from `/proc/meminfo` on Linux. |
| `swap_total`, `swap_used` | Bytes from `/proc/meminfo` on Linux. |
| `netrx`, `nettx` | Bytes across non-loopback, non-container interfaces on Linux. |
| `diskr`, `diskw` | Disk bytes from `/proc/diskstats` on Linux. |
| `sysup` | System uptime in seconds on Linux. |
| `ver` | Build version from `main.version`. |
| `name` | Hostname from the listen address or certificate server name. |
| `uptime` | Master process uptime in seconds. |
| `log` | Compatibility field retained for older clients; currently empty. |
| `tls`, `crt`, `key` | Effective startup TLS configuration values. |

Linux hosts return platform metrics from `/proc`. Other operating systems
return `cpu: -1` and zero-valued platform metrics.

## Instances

### Instance Object

```json
{
  "id": "d34db33f",
  "alias": "edge-a",
  "type": "portal",
  "status": "running",
  "url": "portal://secret@127.0.0.1:2000",
  "config": "",
  "restart": true,
  "meta": {
    "peer": {
      "sid": "site-1",
      "type": "edge",
      "alias": "Site 1 Edge"
    },
    "tags": {
      "region": "us-east"
    }
  },
  "mode": 0,
  "ping": 12,
  "pool": 0,
  "tcps": 10,
  "udps": 2,
  "tcprx": 123456,
  "tcptx": 654321,
  "udprx": 2048,
  "udptx": 4096
}
```

| Field | Description |
| --- | --- |
| `id` | Random 8-character hexadecimal instance ID. |
| `alias` | Operator-defined display name. |
| `type` | URL scheme from the instance URL. |
| `status` | `stopped`, `running`, or `error`. |
| `url` | Normalized instance URL. |
| `config` | Reserved runtime configuration field. The internal API-key instance stores the master ID here. |
| `restart` | Whether to auto-start the instance and restart it after a process failure or fatal lifecycle transition. |
| `meta.peer` | Optional peer metadata with `sid`, `type`, and `alias`. |
| `meta.tags` | Optional string map for operator metadata. |
| `mode`, `pool` | Reserved fields fixed at zero. |
| `ping` | Latest active upstream transport latency from telemetry, in milliseconds. |
| `tcps`, `udps` | Active TCP and UDP counts from telemetry snapshots. |
| `tcprx`, `tcptx`, `udprx`, `udptx` | Logical payload counters from telemetry snapshots. |

`ping` reflects Nowhere's transport sample, not an ICMP probe or the duration of
an HTTP request. QUIC uses its connection RTT; TCP uses the kernel RTT on Linux
and a 1 ms placeholder on other platforms. Zero means no active sample is
available, or OpenCtrl has cleared stale live metrics. It is separate from the
master's `/tcping` endpoint.

Status meanings:

| Status | Meaning |
| --- | --- |
| `stopped` | No child process is active. |
| `running` | The child process has started and is being monitored. |
| `error` | Start failed, the child exited with an error, telemetry reported a fatal lifecycle transition, or telemetry became stale. |

### List Instances

```sh
curl -H "X-API-Key: ${API_KEY}" "${BASE}/instances"
```

The response is an array of instance objects. It includes the internal
`********` instance.

Client-side handling recommendations:

- key local state by `id`;
- sort the array before displaying it;
- redact or hide the internal `********` instance unless explicitly managing
  API credentials;
- treat `url` as sensitive when it may contain credentials;
- keep a bounded log buffer per instance rather than storing unlimited log
  events.

### Create Instance

```sh
curl -X POST "${BASE}/instances" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"alias":"edge-a","url":"portal://secret@127.0.0.1:2000"}'
```

Request body:

| Field | Required | Description |
| --- | --- | --- |
| `url` | yes | Nowhere instance URL with a `portal://` or `vector://` scheme. |
| `alias` | no | Initial display name. |

Creation rules:

- the instance ID is generated from four random bytes;
- the URL scheme becomes `type`;
- `restart` defaults to `true`;
- `meta.tags` starts as an empty map;
- the child process starts asynchronously after storage.

The response status is `201 Created` with the created instance snapshot. The
snapshot may still be `stopped` while the child process is starting; consume
`update` events or refresh the instance before presenting it as fully running.

### Get Instance

```sh
curl -H "X-API-Key: ${API_KEY}" "${BASE}/instances/${ID}"
```

Use this endpoint to reconcile one row after a missed event, a failed local
transition, or a reconnect.

### Patch Instance

```sh
curl -X PATCH "${BASE}/instances/${ID}" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "alias": "edge-b",
    "action": "restart",
    "restart": true,
    "meta": {
      "peer": {
        "sid": "site-1",
        "type": "edge",
        "alias": "Site 1 Edge"
      },
      "tags": {
        "region": "us-east",
        "env": "prod"
      }
    }
  }'
```

Patch fields:

| Field | Description |
| --- | --- |
| `alias` | Updates the alias when non-empty. Maximum length is 256 characters. |
| `action` | One of `start`, `stop`, `restart`, or `reset`. |
| `restart` | Sets the auto-restart policy. |
| `meta.peer` | Replaces peer metadata fields. Each field is limited to 256 characters. |
| `meta.tags` | Replaces the full tags map. Keys and values are limited to 256 characters. |

Actions:

| Action | Effect |
| --- | --- |
| `start` | Starts the child if the instance is `stopped`. |
| `stop` | Stops the child if it is active. |
| `restart` | Stops the child, then starts it. |
| `reset` | Resets byte counters while preserving counter bases for future telemetry snapshots. |

Lifecycle actions run asynchronously except `reset`, which is applied before
the response is returned. If a control button or automation triggers `start`,
`stop`, or `restart`, treat the response as an accepted command and wait for an
`update` event or a follow-up `GET`.

For the internal `********` instance, only `{"action":"restart"}` has a special
effect: it regenerates the API key. Other PATCH bodies return the current
snapshot without changing it.

### Metadata Updates

`meta.tags` is a full replacement, not a merge. Preserve existing tags when
updating one key:

```ts
async function setTag(api: OpenCtrlClient, id: string, key: string, value: string) {
  const instance = await api.getInstance(id);
  await api.patchInstance(id, {
    meta: {
      peer: instance.meta.peer,
      tags: { ...instance.meta.tags, [key]: value },
    },
  });
}
```

Use tags for filtering, grouping, ownership, environment, region, or any other
operator-defined labels. Keep keys stable and values short; the server enforces
the 256-character limit only on PATCH.

### Replace Instance URL

```sh
curl -X PUT "${BASE}/instances/${ID}" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"url":"portal://secret@127.0.0.1:2001"}'
```

The URL must use the `portal` or `vector` scheme. If the normalized URL is unchanged,
OpenCtrl returns `409 Conflict`. Otherwise, the master stops the current child,
stores the new URL and type, and starts the instance again.

The internal `********` instance cannot be updated with `PUT`.

### Delete Instance

```sh
curl -X DELETE \
  -H "X-API-Key: ${API_KEY}" \
  "${BASE}/instances/${ID}"
```

The master marks the instance deleted, stops the child, removes it from memory,
persists state, and emits a `delete` event. The internal `********` instance
cannot be deleted.

After a successful delete, remove local state immediately. A later `delete`
event for the same ID should be idempotent.

### State Reducer

The event stream is easiest to consume with an ID-keyed reducer:

```ts
type InstanceMap = Map<string, Instance>;

export function applyInstanceEvent(state: InstanceMap, event: InstanceEvent) {
  if (event.type === "shutdown") {
    return;
  }

  const instance = event.instance;
  if (!instance) {
    return;
  }

  if (event.type === "delete") {
    state.delete(instance.id);
    return;
  }

  state.set(instance.id, instance);
}
```

Keep logs outside the instance map:

```ts
export function appendBoundedLog(
  logs: Map<string, string[]>,
  event: InstanceEvent,
  limit = 500,
) {
  if (event.type !== "log" || !event.instance || event.logs === "") {
    return;
  }

  const lines = logs.get(event.instance.id) ?? [];
  lines.push(event.logs);
  if (lines.length > limit) {
    lines.splice(0, lines.length - limit);
  }
  logs.set(event.instance.id, lines);
}
```

## Events

Open the stream:

```sh
curl -N -H "X-API-Key: ${API_KEY}" "${BASE}/events"
```

SSE messages use the event name `instance`:

```text
event: instance
data: {"type":"update","time":"2026-06-08T12:00:00Z","instance":{...},"logs":""}
```

The stream starts with:

```text
retry: 3000
```

Then it sends one `initial` event for each stored instance.

Event payload:

```json
{
  "type": "update",
  "time": "2026-06-08T12:00:00Z",
  "instance": {
    "id": "d34db33f"
  },
  "logs": ""
}
```

Event types:

| Type | Description |
| --- | --- |
| `initial` | Sent once per current instance when an SSE client connects. |
| `create` | Sent after an instance is created. |
| `update` | Sent when lifecycle state, metadata, counters, or API-key state changes. |
| `delete` | Sent after an instance is removed. |
| `log` | Sent for safe telemetry runtime events, completed accesses, and delivery-gap notices. |
| `shutdown` | Sent when the master is shutting down or when API key regeneration closes SSE clients. |

Each subscriber has a bounded log queue and a separate map of the latest state
per instance. Metrics are sent every five seconds; status changes and user
operations are sent immediately. Repeated pending state updates coalesce.
Log overflow produces a loss notice as the client drains its queue, including
when no further logs arrive. A client that exceeds the state capacity or cannot
complete a write within five seconds is disconnected and must reconcile after
reconnecting. Slow subscribers do not block telemetry reads.

### Header-Based SSE

Some SSE clients cannot attach custom request headers. Use a client that can
send `X-API-Key`, or read the stream with a normal HTTP request:

```ts
export async function readEvents(
  base: string,
  apiKey: string,
  onEvent: (event: InstanceEvent) => void,
  signal?: AbortSignal,
) {
  const response = await fetch(`${base}/events`, {
    headers: {
      "X-API-Key": apiKey,
      "Cache-Control": "no-cache",
    },
    signal,
  });

  if (!response.ok || !response.body) {
    throw new Error(`events request failed: ${response.status}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  for (;;) {
    const { value, done } = await reader.read();
    if (done) {
      return;
    }

    buffer += decoder.decode(value, { stream: true });
    const frames = buffer.split("\n\n");
    buffer = frames.pop() ?? "";

    for (const frame of frames) {
      const eventLine = frame.split("\n").find((line) => line.startsWith("event: "));
      const dataLine = frame.split("\n").find((line) => line.startsWith("data: "));

      if (eventLine === "event: instance" && dataLine) {
        onEvent(JSON.parse(dataLine.slice("data: ".length)) as InstanceEvent);
      }
    }
  }
}
```

### Reconnect Strategy

Use the `retry: 3000` hint as the minimum reconnect delay. Add a small backoff
when the connection fails repeatedly.

Recommended sequence:

1. Open `/events`.
2. Apply all `initial` events.
3. Apply live `create`, `update`, `delete`, and `log` events.
4. On disconnect, reconnect after a delay.
5. After reconnect, reconcile with `GET /instances` if local state may have
   missed changes.

If a `shutdown` event arrives during API-key regeneration, the next request with
the old key will fail. Load the new key from the operator-controlled channel
where startup logs are collected.

## TCP Ping

```sh
curl -H "X-API-Key: ${API_KEY}" "${BASE}/tcping?target=example.com:443"
```

Response:

```json
{
  "target": "example.com:443",
  "connected": true,
  "latency": 38,
  "error": null
}
```

The master allows up to 10 concurrent TCP ping requests. If the semaphore is
not acquired within one second, the response is successful JSON with
`connected: false` and `error: "too many requests"`.

Connection attempts use a 5-second timeout. A failed connection is not an HTTP
error; check `connected` and `error`.

## State

State is stored beside the executable, not beside the current working
directory:

```text
<executable-directory>/gob/openctrl.gob
<executable-directory>/gob/openctrl.gob.backup
```

The state file is a Go gob-encoded map of instance IDs to instance objects.
Writes use a temporary file in the same directory and then rename it into
place. Temporary `gob-*.tmp` files in the state directory are removed at load
time.

On startup, stored non-internal instances are reset to `stopped` with zero live
metrics. Portal and Vector instances with `restart: true` are auto-started.
Records for unsupported schemes are retained without starting, with an
operational log explaining why. The internal `********` instance is loaded as
the source of API key, master ID, and master alias.

The periodic maintenance task runs every hour. On each tick it:

- writes `openctrl.gob.backup`;
- restarts instances with `restart: true` only when a process failure or fatal
  Nowhere lifecycle transition made them eligible. Telemetry loss alone is not
  restart eligible.

During shutdown, OpenCtrl stops active child processes, saves state, emits SSE
shutdown events, and shuts down the HTTP server with a 5-second context
deadline.

## Logging

The master writes operational logs with Go's standard `log` package:

```text
2026/06/09 12:00:00 Master.run: started: http://127.0.0.1:8080/api/v2
```

Nowhere child stdout and stderr are discarded. Safe runtime events, completed
accesses, and gap notices received from local telemetry are sent to SSE
subscribers as `log` events. OpenCtrl does not store a log history.

## Security Notes

OpenCtrl relies on the API key and the deployment environment for access
control:

- protect the API key printed by the master;
- protect the state directory because `openctrl.gob` stores the API key;
- use `tls=2` with a trusted certificate outside local-only deployments;
- use a trusted `bin` path because managed instances execute that binary;
- treat instance URLs as secrets when they contain credentials, keys, or
  tokens;
- account for permissive CORS headers when exposing the API across origins.

## Nowhere Process and Telemetry Contract

The master supervises Nowhere Portal and Vector processes. Set
`?bin=/path/to/nowhere` unless OpenCtrl and Nowhere are provided by the same
executable.

### Launch

```text
<nowhere-binary> <portal-or-vector-url>
```

The child receives the full URL as its first positional argument and runs as
the same operating-system user. OpenCtrl discovers the protected
`nowhere.telemetry` registry record belonging to the child PID, validates the
registry, namespace, endpoint, process identity, and hello identity, then
subscribes in `detail` mode. There is no TCP fallback or stdout parser.

OpenCtrl uses the fixed telemetry contract defined by Nowhere. The contract has
no OpenCtrl-specific fields, messages, version negotiation, or replay. See the
Nowhere telemetry contract for framing, discovery, privacy, and delivery semantics. The supported
deployment places both processes on the same host or in the same container,
with the same user, temporary directory, and process namespace.

### Stop

On Unix-like systems, the master sends `SIGTERM`. On Windows, Go does not
support sending the requested interrupt signal to the child. The master waits
up to 5 seconds and then kills the process if it has not exited.

After a stop, OpenCtrl clears the process handle and live metrics, then marks
the instance `stopped`.

### Exit Status

If the child exits cleanly while it was not being stopped, OpenCtrl resets the
instance to `stopped`. If the child exits with an error, OpenCtrl marks the
instance `error`.

The hourly task restarts an instance with `restart: true` only while its
process-failure or fatal-lifecycle reason still applies. Telemetry loss alone
does not qualify it for restart.

### Metrics, Logs, and Health

Snapshots map logical TCP and UDP counters, active counts, and ping into the
instance fields. `mode` and `pool` are always zero. Byte totals are cumulative
across restarts and support the reset operation.

Safe `runtime_event` messages and completed `access_finish` records become SSE
`log` events. `access_start` is not logged separately. Telemetry `gap` messages
and local SSE queue overflow produce visible loss notices. Child stdout and
stderr are discarded, are not mirrored to OpenCtrl logs, and never change
instance status.

Failures before telemetry connects are reported as launch, connection, or exit
failures; raw child error text is unavailable. Diagnostic logs and certificate
fingerprints follow Nowhere's own logging policy and are not forwarded.

The child is `running` after a successful process start. It becomes `error` if
no nonzero snapshot arrives within 15 seconds, or if snapshots later stop for
the greater of 15 seconds and three advertised telemetry intervals. IPC
reconnect uses exponential backoff capped at 30 seconds. A fresh snapshot while
the lifecycle is `READY` restores `running`; observation failure alone never
qualifies the child for automatic restart.

## Client Patterns

### Minimal API Wrapper

```ts
export class OpenCtrlClient {
  constructor(
    private readonly base: string,
    private readonly apiKey: string,
  ) {}

  private async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    headers.set("X-API-Key", this.apiKey);

    if (init.body && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }

    const response = await fetch(`${this.base}${path}`, {
      ...init,
      headers,
    });

    if (response.status === 204) {
      return undefined as T;
    }

    const text = await response.text();
    const data = text ? JSON.parse(text) : undefined;

    if (!response.ok) {
      const message =
        data && typeof data.error === "string"
          ? data.error
          : `request failed: ${response.status}`;
      throw new Error(message);
    }

    return data as T;
  }

  info() {
    return this.request<MasterInfo>("/info");
  }

  setAlias(alias: string) {
    return this.request<MasterInfo>("/info", {
      method: "POST",
      body: JSON.stringify({ alias }),
    });
  }

  listInstances() {
    return this.request<Instance[]>("/instances");
  }

  getInstance(id: string) {
    return this.request<Instance>(`/instances/${encodeURIComponent(id)}`);
  }

  createInstance(input: { alias?: string; url: string }) {
    return this.request<Instance>("/instances", {
      method: "POST",
      body: JSON.stringify(input),
    });
  }

  patchInstance(
    id: string,
    input: {
      alias?: string;
      action?: InstanceAction;
      restart?: boolean;
      meta?: Partial<InstanceMeta>;
    },
  ) {
    return this.request<Instance>(`/instances/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: JSON.stringify(input),
    });
  }

  replaceInstanceURL(id: string, url: string) {
    return this.request<Instance>(`/instances/${encodeURIComponent(id)}`, {
      method: "PUT",
      body: JSON.stringify({ url }),
    });
  }

  async deleteInstance(id: string) {
    await this.request<void>(`/instances/${encodeURIComponent(id)}`, {
      method: "DELETE",
    });
  }

  tcping(target: string) {
    return this.request<TcpPingResult>(
      `/tcping?target=${encodeURIComponent(target)}`,
    );
  }
}
```

### Initial Load

Use one path for initial state and one path for live changes:

```ts
const api = new OpenCtrlClient("http://127.0.0.1:8080/api/v2", apiKey);
const instances = new Map<string, Instance>();

for (const instance of await api.listInstances()) {
  instances.set(instance.id, instance);
}

const controller = new AbortController();
readEvents(
  "http://127.0.0.1:8080/api/v2",
  apiKey,
  (event) => {
    applyInstanceEvent(instances, event);
    appendBoundedLog(instanceLogs, event);
  },
  controller.signal,
);
```

If the event stream starts before the list call, apply `initial` events first
and then reconcile with the list. If the list call starts first, accept that an
event can arrive between list completion and stream connection, then reconcile
after connection. The simplest robust pattern is to refresh `GET /instances`
after any reconnect.

### Local Transition State

Commands such as `start`, `stop`, and `restart` are accepted quickly and then
complete through process state changes. Keep a separate local pending state if
the caller needs to show an in-progress operation:

```ts
const pending = new Set<string>();

async function restart(api: OpenCtrlClient, id: string) {
  pending.add(id);
  try {
    await api.patchInstance(id, { action: "restart" });
  } finally {
    setTimeout(() => pending.delete(id), 10_000);
  }
}

function onInstanceEvent(event: InstanceEvent) {
  if (event.instance) {
    pending.delete(event.instance.id);
  }
}
```

Use server state as authoritative. Local pending flags should expire even if no
event arrives.

### Retry Rules

Use different retry behavior for different failure classes:

| Failure | Suggested behavior |
| --- | --- |
| Network error | Retry idempotent reads after a delay. |
| `401` | Stop retrying until a valid API key is available. |
| `400` | Surface the validation message and let the caller change input. |
| `404` on detail | Remove or refresh local state for that ID. |
| `409` on URL replacement | Treat as unchanged input. |
| TCP ping `connected: false` | Show the `error` value; do not treat it as request failure. |
| SSE disconnect | Reconnect and refresh `GET /instances`. |

For commands that change process state, avoid automatic unbounded retries. A
network timeout can leave the command accepted but the response lost. Prefer a
follow-up `GET /instances/{id}` or event-stream reconciliation before retrying.

### Display Rules

Useful presentation rules:

- show `alias` when non-empty, otherwise show `id`;
- hide or redact `url` by default when it can contain credentials;
- render `status` directly from the server instead of deriving it from metrics;
- use `tcps` and `udps` as live counts, not cumulative counters;
- use `tcprx`, `tcptx`, `udprx`, and `udptx` as cumulative counters since the
  last reset and persisted base;
- keep empty `meta.tags` as `{}` rather than `null`;
- treat missing logs as an empty string;
- keep log storage bounded.

### Redaction

URLs, certificate paths, and API keys can be sensitive. A simple redactor should
cover the common cases before values are copied into logs:

```ts
export function redactOpenCtrlValue(value: string) {
  return value
    .replace(/([?&](?:key|token|secret|password)=)[^&]+/gi, "$1<redacted>")
    .replace(/(X-API-Key:\s*)[0-9a-f]+/gi, "$1<redacted>")
    .replace(/\/\/([^/@]+)@/g, "//<redacted>@");
}
```

## Implementation Limits

| Limit | Value |
| --- | --- |
| API key ID | `********` |
| Instance ID length | 8 lowercase hexadecimal characters |
| Master ID length | 16 lowercase hexadecimal characters |
| API key length | 32 lowercase hexadecimal characters |
| Alias, peer field, tag key, tag value | 256 characters |
| TCP ping concurrency | 10 |
| TCP ping dial timeout | 5 seconds |
| Stop grace period | 5 seconds |
| Instance health check interval | 5 seconds |
| State backup and restart check interval | 1 hour |
| Telemetry startup timeout | 15 seconds without a nonzero snapshot |
| Telemetry freshness | Greater of 15 seconds or three advertised intervals |
| SSE retry hint | 3000 milliseconds |
| Per-subscriber pending instance states | 1024 |
| Per-subscriber queued logs | 10 |
| SSE write deadline | 5 seconds |
| Certificate reload interval | 1 hour |

## Release Build

Tags matching `v*.*.*` trigger release builds for:

- Linux `amd64` and `arm64`;
- macOS `amd64` and `arm64`;
- FreeBSD `amd64` and `arm64`;
- Windows `amd64` and `arm64`.

The release workflow sets `main.version` to the tag name.
