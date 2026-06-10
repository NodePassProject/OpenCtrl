// Package master implements the OpenCtrl control plane.
//
// A master process is intentionally small: it exposes the HTTP API, owns the
// persistent registry of managed child instances, starts and stops child
// binaries, and broadcasts instance state over Server-Sent Events. It does not
// understand protocol-specific child URLs beyond the URL scheme. That boundary
// is important because third-party binaries are expected to parse their own
// configuration URLs and to report progress through the shared log/checkpoint
// contract.
//
// Most state in this package is process-local and protected by explicit locks
// on the relevant instance or master. HTTP handlers should snapshot mutable
// records before returning them, and lifecycle operations should use
// instance.lifecycleMu to avoid overlapping starts, stops, restarts, deletes,
// and URL replacements for the same instance.
package master

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// defaultAPIPath is used when the master URL does not specify an API
	// prefix. The version suffix is appended separately.
	defaultAPIPath = "/api"

	// openAPIVersion is the HTTP API major version exposed in every route.
	// Keep this aligned with public documentation and the Go module major
	// version when making a breaking API change.
	openAPIVersion = "v2"

	// stateFilePath and stateFileName place the gob state beside the running
	// executable. This keeps self-contained release bundles portable.
	stateFilePath = "gob"
	stateFileName = "openctrl.gob"

	// sseRetryTime is sent as the SSE retry directive in milliseconds.
	sseRetryTime = 3000

	// apiKeyID is a reserved pseudo-instance ID. Its URL field stores the
	// current API key and its Config field stores the persistent master ID.
	apiKeyID = "********"

	// pingSemLimit caps concurrent TCP probes so the diagnostic endpoint cannot
	// exhaust file descriptors or create unbounded outbound dials.
	pingSemLimit = 10

	// channelBufSize bounds the master-wide event queue. Events are best effort
	// under backpressure; lifecycle goroutines must never block on UI clients.
	channelBufSize = 1024

	// baseDuration is the short sampling/spread interval used for CPU samples
	// and paced auto-start after state restore.
	baseDuration = 100 * time.Millisecond

	// gracefulTimeout is the maximum time allowed for a child process or the
	// HTTP server to exit before the master escalates.
	gracefulTimeout = 5 * time.Second

	// reportInterval is the cadence expected for child checkpoint reporting and
	// the timeout used by single TCP diagnostic dials.
	reportInterval = 5 * time.Second

	// reloadInterval controls infrequent maintenance such as certificate reload
	// checks and periodic state backups.
	reloadInterval = 1 * time.Hour

	// maxValueLen bounds small operator-controlled strings to keep persisted
	// state and API responses predictable.
	maxValueLen = 256
)

// NewMaster builds a control-plane master from a parsed master:// URL.
//
// The master URL owns only controller concerns: listener address, API prefix,
// TLS mode, certificate paths, and the optional managed binary path. Child URLs
// are treated as opaque commands and are parsed by their own binaries.
//
// URL contract:
//   - Host selects the TCP listener address.
//   - Path selects the API prefix; an empty path becomes /api.
//   - tls/crt/key configure the server-side TLS mode.
//   - bin optionally selects the executable used for managed child instances.
//
// Construction also restores persisted state before starting the event
// dispatcher so newly connected SSE clients can receive an initial snapshot of
// the same registry the API will serve.
func NewMaster(parsedURL *url.URL, version string) (*Master, error) {
	host, err := net.ResolveTCPAddr("tcp", parsedURL.Host)
	if err != nil {
		return nil, fmt.Errorf("Master.new: resolve host failed: %w", err)
	}

	tlsMode, tlsConfig, err := newServerTLSConfig(parsedURL, serverTLSOptions{allowNone: true})
	if err != nil {
		return nil, fmt.Errorf("Master.new: %w", err)
	}

	var hostname string
	if tlsConfig != nil && tlsConfig.ServerName != "" {
		hostname = tlsConfig.ServerName
	} else {
		hostname = parsedURL.Hostname()
	}

	// The API version is appended after normalizing the user-supplied prefix so
	// both master://host and master://host/custom expose a v2 route surface.
	prefix := parsedURL.Path
	if prefix == "" || prefix == "/" {
		prefix = defaultAPIPath
	} else {
		prefix = strings.TrimRight(prefix, "/")
	}

	execPath, _ := os.Executable()
	baseDir := filepath.Dir(execPath)

	// The state file is anchored next to the executable, not the process working
	// directory. That keeps service-manager launches and shell launches
	// consistent.
	master := &Master{
		endpointTCPAddr: host,
		tlsMode:         tlsMode,
		prefix:          fmt.Sprintf("%s/%s", prefix, openAPIVersion),
		version:         version,
		crtPath:         parsedURL.Query().Get("crt"),
		keyPath:         parsedURL.Query().Get("key"),
		binPath:         parsedURL.Query().Get("bin"),
		hostname:        hostname,
		mTLSConfig:      tlsConfig,
		statePath:       filepath.Join(baseDir, stateFilePath, stateFileName),
		notifyChannel:   make(chan *instanceEvent, channelBufSize),
		tcpPingSem:      make(chan struct{}, pingSemLimit),
		startTime:       time.Now(),
		periodicDone:    make(chan struct{}),
	}
	master.loadState()

	go master.startEventDispatcher()

	return master, nil
}

// Run serves the authenticated HTTP API until the process receives a shutdown
// signal, then drains child instances, SSE clients, and persistent state.
//
// Run is intentionally blocking. The caller starts one core and then lets the
// core own process lifetime, signal handling, and graceful shutdown.
func (m *Master) Run() {
	scheme := "http"
	if m.mTLSConfig != nil {
		scheme = "https"
	}
	log.Printf("Master.run: started: %v://%v%v", scheme, m.endpointTCPAddr, m.prefix)

	apiKey, ok := m.findInstance(apiKeyID)
	if !ok {
		// API credentials are represented as a reserved instance so the existing
		// state persistence path can store the key, master ID, and alias without
		// a second file format.
		apiKey = &instance{
			ID:     apiKeyID,
			URL:    generateAPIKey(),
			Config: generateMasterID(),
			Meta:   meta{Tags: make(map[string]string)},
		}
		m.instances.Store(apiKeyID, apiKey)
		m.saveState()
		m.masterID = apiKey.Config
		log.Printf("Master.run: API key created: %v", apiKey.URL)
	} else {
		m.alias = apiKey.Alias
		m.masterID = apiKey.Config

		log.Printf("Master.run: API key loaded: %v", apiKey.URL)
	}

	mux := http.NewServeMux()

	// All current endpoints are authenticated. CORS preflight is handled inside
	// the middleware so browser clients can discover the API without a key on
	// OPTIONS requests.
	protectedEndpoints := map[string]http.HandlerFunc{
		fmt.Sprintf("%s/instances", m.prefix):  m.handleInstances,
		fmt.Sprintf("%s/instances/", m.prefix): m.handleInstanceDetail,
		fmt.Sprintf("%s/events", m.prefix):     m.handleSSE,
		fmt.Sprintf("%s/info", m.prefix):       m.handleInfo,
		fmt.Sprintf("%s/tcping", m.prefix):     m.handleTCPPing,
	}

	apiKeyMiddleware := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			setCORSHeaders(w)
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			if apiKeyInstance, keyExists := m.findInstance(apiKeyID); keyExists {
				apiKey := apiKeyInstance.snapshot()
				if apiKey.URL != "" {
					// The API key may be regenerated while the server is
					// running, so read it from state for each request instead of
					// capturing the value created at startup.
					reqAPIKey := r.Header.Get("X-API-Key")
					if reqAPIKey == "" {
						httpError(w, "Unauthorized: API key required", http.StatusUnauthorized)
						return
					}

					if reqAPIKey != apiKey.URL {
						httpError(w, "Unauthorized: Invalid API key", http.StatusUnauthorized)
						return
					}
				}
			}

			next(w, r)
		}
	}

	for path, handler := range protectedEndpoints {
		mux.HandleFunc(path, apiKeyMiddleware(handler))
	}

	m.server = &http.Server{
		Addr:      m.endpointTCPAddr.String(),
		ErrorLog:  log.Default(),
		Handler:   mux,
		TLSConfig: m.mTLSConfig,
	}

	go func() {
		var err error
		if m.mTLSConfig != nil {
			err = m.server.ListenAndServeTLS("", "")
		} else {
			err = m.server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Printf("Master.run: listen failed: %v", err)
		}
	}()

	go m.startPeriodicTasks()

	// Only process-level termination signals stop the master. API-level
	// lifecycle operations are limited to managed child instances.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	<-ctx.Done()
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), gracefulTimeout)
	defer cancel()
	if err := m.shutdown(shutdownCtx); err != nil {
		log.Printf("Master.run: shutdown failed: %v", err)
	} else {
		log.Printf("Master.run: shutdown complete")
	}
}

// shutdown coordinates all master-owned resources under the caller's deadline.
//
// The work runs in a goroutine so the caller's context can cap the total
// shutdown time even if a child process or the HTTP server takes too long. Best
// effort logging is used during shutdown because returning a partial error
// would not change process exit behavior.
func (m *Master) shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)

		close(m.periodicDone)
		m.shutdownSSEConnections()

		// Stop managed children concurrently, but only for instances that still
		// have a live process. The per-instance lifecycle lock inside
		// stopInstance preserves ordering for each individual child.
		var wg sync.WaitGroup
		m.instances.Range(func(key, value any) bool {
			inst := value.(*instance)
			inst.mu.Lock()
			shouldStop := inst.Status != "stopped" && inst.cmd != nil && inst.cmd.Process != nil
			inst.mu.Unlock()
			if shouldStop {
				wg.Add(1)
				go func(i *instance) {
					defer wg.Done()
					m.stopInstance(i)
				}(inst)
			}
			return true
		})
		wg.Wait()

		if err := m.saveState(); err != nil {
			log.Printf("Master.shutdown: save state failed: %v", err)
		} else {
			log.Printf("Master.shutdown: state saved: %v", m.statePath)
		}

		if err := m.server.Shutdown(ctx); err != nil {
			log.Printf("Master.shutdown: api shutdown failed: %v", err)
		}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("Master.shutdown: context error: %w", ctx.Err())
	case <-done:
		return nil
	}
}
