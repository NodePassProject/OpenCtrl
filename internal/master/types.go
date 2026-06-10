package master

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"os/exec"
	"regexp"
	"sync"
	"time"
)

// Master owns one OpenCtrl control-plane process.
//
// It keeps the API server, child-process lifecycle state, persistent state, and
// SSE fan-out in one package-local boundary so protocol-specific child binaries
// remain outside the controller's parsing contract.
//
// The struct deliberately keeps API-facing state and runtime coordination state
// together because lifecycle transitions must update both under a predictable
// lock order. Production code should use the methods in this package instead
// of mutating fields directly; tests may inject instanceCmd to replace process
// creation.
type Master struct {
	// Immutable runtime configuration derived from the master URL.
	endpointTCPAddr *net.TCPAddr // Bound HTTP listener address.
	tlsMode         tlsMode      // Compact TLS mode reported through /info.
	prefix          string       // Versioned API prefix, for example /api/v2.
	version         string       // Build version reported through /info.
	hostname        string       // Preferred external name reported to clients.
	crtPath         string       // Certificate path reported for TLS visibility.
	keyPath         string       // Key path reported for TLS visibility.
	binPath         string       // Optional executable used for managed children.

	// Mutable master identity exposed through the info endpoint.
	masterID string // Persistent ID stored in the reserved API-key record.
	alias    string // Operator-controlled display metadata; protected by mu.

	// instances stores *instance values by ID. sync.Map keeps REST handlers,
	// lifecycle goroutines, persistence, and SSE reporting loosely coupled.
	//
	// Values are mutable pointers. Code must hold instance.mu before reading or
	// writing fields unless it is using snapshot.
	instances sync.Map

	server     *http.Server // Assigned in Run after routes are built.
	mTLSConfig *tls.Config  // Nil means plaintext HTTP for the master API.
	statePath  string       // Canonical gob file path for persisted state.

	// stateMu serializes gob writes so async saves cannot interleave.
	stateMu sync.Mutex
	mu      sync.RWMutex // Protects master-level mutable metadata.

	// subscribers contains live SSE clients; notifyChannel decouples event
	// producers from slow or disconnected clients.
	//
	// The subscriber map stores *sseSubscriber values by generated subscriber
	// ID.
	subscribers sync.Map
	// notifyChannel is the master-wide queue consumed by startEventDispatcher.
	notifyChannel chan *instanceEvent

	tcpPingSem   chan struct{} // Counting semaphore for diagnostic TCP probes.
	startTime    time.Time     // Process uptime anchor for /info.
	periodicDone chan struct{} // Stops maintenance during shutdown.

	// Test hook; nil uses exec.CommandContext.
	instanceCmd func(context.Context, string, string) *exec.Cmd
}

// instance is both the public JSON model and the private lifecycle record for
// a managed child process.
//
// Persisted fields must remain gob-compatible across ordinary upgrades. Runtime
// fields below the blank line are reconstructed after load and should never be
// assumed to survive a process restart.
type instance struct {
	// Persisted and API-visible fields.
	ID      string `json:"id"`      // Stable registry key and URL path segment.
	Alias   string `json:"alias"`   // Optional operator-facing display text.
	Type    string `json:"type"`    // Child URL scheme, used for display/filtering.
	Status  string `json:"status"`  // Public state: stopped, running, or error.
	URL     string `json:"url"`     // Opaque child configuration URL.
	Config  string `json:"config"`  // Child config; master ID on apiKeyID.
	Restart bool   `json:"restart"` // Auto-start/recover this instance.
	Meta    meta   `json:"meta"`    // User metadata and child peer metadata.
	Mode    int32  `json:"mode"`    // Decoded from CHECK_POINT.
	Ping    int32  `json:"ping"`    // Decoded from CHECK_POINT, in ms.
	Pool    int32  `json:"pool"`    // Decoded from CHECK_POINT.
	TCPS    int32  `json:"tcps"`    // Decoded TCP session count.
	UDPS    int32  `json:"udps"`    // Decoded UDP session count.
	TCPRX   uint64 `json:"tcprx"`   // Accumulated TCP receive bytes.
	TCPTX   uint64 `json:"tcptx"`   // Accumulated TCP transmit bytes.
	UDPRX   uint64 `json:"udprx"`   // Accumulated UDP receive bytes.
	UDPTX   uint64 `json:"udptx"`   // Accumulated UDP transmit bytes.

	// Runtime-only coordination state.
	mu          sync.Mutex // Protects every mutable field on this instance.
	lifecycleMu sync.Mutex // Serializes start/stop/restart/delete/replace.

	// *Base fields preserve cumulative counters across restarts.
	tcpRXBase uint64
	tcpTXBase uint64
	udpRXBase uint64
	udpTXBase uint64

	// *Reset fields track operator resets without requiring child counters to
	// reset. They are subtracted from subsequent checkpoint values.
	tcpRXReset uint64
	tcpTXReset uint64
	udpRXReset uint64
	udpTXReset uint64

	cmd            *exec.Cmd          // Active child process while running.
	stopped        chan struct{}      // Closed by the master for intentional stops.
	exited         chan struct{}      // Closed after cmd.Wait returns.
	deleted        bool               // Suppresses late output from removed instances.
	cancelFunc     context.CancelFunc // Cancels the child CommandContext.
	lastCheckpoint time.Time
}

// meta carries operator-managed annotations for an instance.
type meta struct {
	Peer peer              `json:"peer"` // Optional protocol-level remote identity.
	Tags map[string]string `json:"tags"` // Full replacement on PATCH; not merged.
}

// peer describes the remote side associated with an instance when the child
// protocol exposes that information.
type peer struct {
	SID   string `json:"sid"`   // Protocol-specific session or subject ID.
	Type  string `json:"type"`  // Protocol-specific peer class.
	Alias string `json:"alias"` // Display metadata for the peer.
}

// instanceLogWriter bridges child stdout/stderr into local logs, checkpoint
// state, and SSE events.
//
// A single writer is assigned to both stdout and stderr so ordering is as close
// as the operating system and os/exec pipes allow. The writer treats checkpoint
// lines as machine-readable telemetry and all other lines as human-readable log
// output.
type instanceLogWriter struct {
	instanceID string         // Stable label for forwarded child log lines.
	target     io.Writer      // Destination for human-readable child output.
	master     *Master        // Registry updates and SSE fan-out.
	checkpoint *regexp.Regexp // Child CHECK_POINT compatibility parser.
	instance   *instance      // Mutable child record updated by parsed output.
}

// instanceEvent is the single SSE payload shape emitted by the master API.
//
// Type values currently include initial, create, update, delete, log, and
// shutdown. The Logs field is intentionally retained for log events even though
// the master-side startup log parameter was removed.
type instanceEvent struct {
	Type     string    `json:"type"`     // Event semantic.
	Time     time.Time `json:"time"`     // Generated at enqueue time.
	Instance *instance `json:"instance"` // Omitted only for global events.
	Logs     string    `json:"logs"`     // One child log line when Type is log.
}

// sseSubscriber owns one client's bounded event queue and close signal.
type sseSubscriber struct {
	events chan *instanceEvent // Bounded event queue.
	done   chan struct{}       // Disconnect or forced reconnect signal.
	once   sync.Once           // Idempotent close guard.
}

// systemInfo is the platform snapshot returned by the info endpoint.
type systemInfo struct {
	CPU       int    `json:"cpu"`        // Integer percentage; -1 means unavailable.
	MemTotal  uint64 `json:"mem_total"`  // Bytes.
	MemUsed   uint64 `json:"mem_used"`   // Bytes.
	SwapTotal uint64 `json:"swap_total"` // Bytes.
	SwapUsed  uint64 `json:"swap_used"`  // Bytes.
	NetRX     uint64 `json:"netrx"`      // Cumulative non-container RX bytes.
	NetTX     uint64 `json:"nettx"`      // Cumulative non-container TX bytes.
	DiskR     uint64 `json:"diskr"`      // Cumulative whole-disk read bytes.
	DiskW     uint64 `json:"diskw"`      // Cumulative whole-disk written bytes.
	SysUp     uint64 `json:"sysup"`      // Host uptime in seconds.
}

// tcpPingResult is the JSON response shape for an on-demand TCP reachability
// probe.
type tcpPingResult struct {
	Target    string  `json:"target"`    // Requested host:port.
	Connected bool    `json:"connected"` // TCP handshake completed.
	Latency   int64   `json:"latency"`   // Dial duration in milliseconds.
	Error     *string `json:"error"`     // Nil on success; displayable on failure.
}
