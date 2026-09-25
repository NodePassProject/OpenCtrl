package master

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os/exec"
	"sync"
	"time"
)

type Master struct {
	endpointTCPAddr *net.TCPAddr
	tlsMode         tlsMode
	prefix          string
	version         string
	hostname        string
	crtPath         string
	keyPath         string
	binPath         string

	masterID string
	alias    string

	instances sync.Map

	server     *http.Server
	mTLSConfig *tls.Config
	statePath  string

	stateMu sync.Mutex
	mu      sync.RWMutex

	subscribers sync.Map

	tcpPingSem   chan struct{}
	startTime    time.Time
	periodicDone chan struct{}

	instanceCmd func(context.Context, string, string) *exec.Cmd
}

type instance struct {
	ID      string `json:"id"`
	Alias   string `json:"alias"`
	Type    string `json:"type"`
	Status  string `json:"status"`
	URL     string `json:"url"`
	Config  string `json:"config"`
	Restart bool   `json:"restart"`
	Meta    meta   `json:"meta"`
	Mode    int32  `json:"mode"`
	Ping    int32  `json:"ping"`
	Pool    int32  `json:"pool"`
	TCPS    int32  `json:"tcps"`
	UDPS    int32  `json:"udps"`
	TCPRX   uint64 `json:"tcprx"`
	TCPTX   uint64 `json:"tcptx"`
	UDPRX   uint64 `json:"udprx"`
	UDPTX   uint64 `json:"udptx"`

	mu          sync.Mutex
	lifecycleMu sync.Mutex

	tcpRXBase uint64
	tcpTXBase uint64
	udpRXBase uint64
	udpTXBase uint64

	tcpRXReset uint64
	tcpTXReset uint64
	udpRXReset uint64
	udpTXReset uint64

	cmd               *exec.Cmd
	stopped           chan struct{}
	exited            chan struct{}
	deleted           bool
	cancelFunc        context.CancelFunc
	lastTelemetry     time.Time
	lastSequence      uint64
	telemetryIdentity string
	telemetryInterval time.Duration
	telemetryReady    bool
	runtimeFailure    bool
}

type meta struct {
	Peer peer              `json:"peer"`
	Tags map[string]string `json:"tags"`
}

type peer struct {
	SID   string `json:"sid"`
	Type  string `json:"type"`
	Alias string `json:"alias"`
}

type instanceEvent struct {
	Type     string    `json:"type"`
	Time     time.Time `json:"time"`
	Instance *instance `json:"instance"`
	Logs     string    `json:"logs"`
}

type sseSubscriber struct {
	mu          sync.Mutex
	wake        chan struct{}
	states      map[string]*instanceEvent
	logs        []*instanceEvent
	done        chan struct{}
	once        sync.Once
	droppedLogs uint64
	gapInstance *instance
	gapSent     bool
}

type systemInfo struct {
	CPU       int    `json:"cpu"`
	MemTotal  uint64 `json:"mem_total"`
	MemUsed   uint64 `json:"mem_used"`
	SwapTotal uint64 `json:"swap_total"`
	SwapUsed  uint64 `json:"swap_used"`
	NetRX     uint64 `json:"netrx"`
	NetTX     uint64 `json:"nettx"`
	DiskR     uint64 `json:"diskr"`
	DiskW     uint64 `json:"diskw"`
	SysUp     uint64 `json:"sysup"`
}

type tcpPingResult struct {
	Target    string  `json:"target"`
	Connected bool    `json:"connected"`
	Latency   int64   `json:"latency"`
	Error     *string `json:"error"`
}
