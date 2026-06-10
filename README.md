# OpenCtrl

OpenCtrl is an advanced control-plane master for URL-defined runtime instances.

It stores instance definitions, supervises child processes, exposes a versioned
HTTP API, and streams lifecycle, log, and metric updates over Server-Sent
Events. The master stays generic: orchestration belongs here; protocol behavior
belongs to the runtime binary.

## Design

- URL-first instance configuration
- Runtime-neutral process supervision
- Versioned REST API under `/api/v2`
- Server-Sent Events for live state
- Local durable state with backup
- HTTP, self-signed TLS, or certificate-backed TLS

## Build

```sh
go build -trimpath -o ./bin/openctrl ./cmd/openctrl
```

## Run

```sh
./bin/openctrl 'master://127.0.0.1:8080'
```

On first start, OpenCtrl prints an API key:

```text
Master.run: API key created: <api-key>
```

Use it with the API:

```sh
BASE='http://127.0.0.1:8080/api/v2'
API_KEY='<api-key>'

curl -H "X-API-Key: ${API_KEY}" "${BASE}/info"
curl -H "X-API-Key: ${API_KEY}" "${BASE}/instances"
```

To supervise non-`master` instance schemes, point the master at a compatible
runtime binary:

```sh
./bin/openctrl 'master://127.0.0.1:8080?bin=/opt/openctrl/runtime'
```

Create an instance:

```sh
curl -X POST "${BASE}/instances" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"alias":"edge-a","url":"managed://edge-a"}'
```

## Master URL

```text
master://<host>:<port>[/prefix][?<query>]
```

The host and port become the listen address. The path becomes the API prefix,
with the API version appended automatically.

| URL | API base |
| --- | --- |
| `master://127.0.0.1:8080` | `http://127.0.0.1:8080/api/v2` |
| `master://127.0.0.1:8080/control` | `http://127.0.0.1:8080/control/v2` |
| `master://127.0.0.1:8443?tls=1` | `https://127.0.0.1:8443/api/v2` |

| Query | Description |
| --- | --- |
| `tls=0` | Plain HTTP. |
| `tls=1` | Self-signed TLS. |
| `tls=2&crt=<path>&key=<path>` | Certificate-backed TLS. |
| `bin=<path>` | Runtime binary used for managed instances. |

## API

All protected requests require:

```text
X-API-Key: <api-key>
```

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/info` | Read master identity and runtime information. |
| `POST` | `/info` | Set the master alias. |
| `GET` | `/instances` | List instances. |
| `POST` | `/instances` | Create and start an instance. |
| `GET` | `/instances/{id}` | Read one instance. |
| `PATCH` | `/instances/{id}` | Update metadata, policy, or lifecycle action. |
| `PUT` | `/instances/{id}` | Replace the instance URL. |
| `DELETE` | `/instances/{id}` | Stop and delete an instance. |
| `GET` | `/events` | Subscribe to instance events. |
| `GET` | `/tcping?target=host:port` | Test TCP reachability. |

`PATCH /instances/{id}` accepts `start`, `stop`, `restart`, and `reset`.

SSE messages use the event name `instance`:

```text
event: instance
data: {"type":"update","time":"2026-06-08T12:00:00Z","instance":{...},"logs":""}
```

Event types are `initial`, `create`, `update`, `delete`, `log`, and
`shutdown`.

## Runtime Contract

OpenCtrl launches managed instances as:

```text
<runtime-binary> <instance-url>
```

Runtime binaries must accept the full instance URL as the first argument, stay
in the foreground while active, reject invalid configuration before long-running
work starts, handle termination promptly, and return a non-zero exit code for
failures that should move the instance to `error`.

Runtime binaries should write newline-delimited logs to stdout or stderr. They
may emit checkpoints for metrics:

```text
CHECK_POINT|MODE=<n>|PING=<n>ms|POOL=<n>|TCPS=<n>|UDPS=<n>|TCPRX=<bytes>|TCPTX=<bytes>|UDPRX=<bytes>|UDPTX=<bytes>
```

Non-checkpoint output is forwarded to stdout and published as `log` events. A
line containing `ERROR` marks the instance as `error`.

## State

State is stored beside the executable:

```text
<executable-directory>/gob/openctrl.gob
<executable-directory>/gob/openctrl.gob.backup
```

Protect the API key, state directory, runtime binary path, and instance URLs.
Use `tls=2` for networked deployments.

## Release

```sh
go build -trimpath \
  -ldflags="-s -w -X main.version=$VERSION" \
  -o ./bin/openctrl ./cmd/openctrl
```

Tagged releases are built with GoReleaser for Darwin, FreeBSD, Linux, and
Windows on `amd64` and `arm64`.

## Documentation

See [docs/master.md](docs/master.md) for the full API and runtime contract.

## License

OpenCtrl is released under the BSD 3-Clause License.
