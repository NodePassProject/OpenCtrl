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

type Master struct {
	endpointTCPAddr *net.TCPAddr
	tlsMode         tlsMode
	masterID        string
	alias           string
	prefix          string
	version         string
	hostname        string
	crtPath         string
	keyPath         string
	binPath         string
	instances       sync.Map
	server          *http.Server
	mTLSConfig      *tls.Config
	statePath       string
	stateMu         sync.Mutex
	mu              sync.RWMutex
	subscribers     sync.Map
	notifyChannel   chan *instanceEvent
	tcpPingSem      chan struct{}
	startTime       time.Time
	periodicDone    chan struct{}
	instanceCmd     func(context.Context, string, string) *exec.Cmd
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

	mu             sync.Mutex
	lifecycleMu    sync.Mutex
	tcpRXBase      uint64
	tcpTXBase      uint64
	udpRXBase      uint64
	udpTXBase      uint64
	tcpRXReset     uint64
	tcpTXReset     uint64
	udpRXReset     uint64
	udpTXReset     uint64
	cmd            *exec.Cmd
	stopped        chan struct{}
	exited         chan struct{}
	deleted        bool
	cancelFunc     context.CancelFunc
	lastCheckpoint time.Time
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

type instanceLogWriter struct {
	instanceID string
	target     io.Writer
	master     *Master
	checkpoint *regexp.Regexp
	instance   *instance
}

type instanceEvent struct {
	Type     string    `json:"type"`
	Time     time.Time `json:"time"`
	Instance *instance `json:"instance"`
	Logs     string    `json:"logs"`
}

type sseSubscriber struct {
	events chan *instanceEvent
	done   chan struct{}
	once   sync.Once
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
