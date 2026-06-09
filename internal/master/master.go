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
	defaultAPIPath  = "/api"
	openAPIVersion  = "v2"
	stateFilePath   = "gob"
	stateFileName   = "openctrl.gob"
	sseRetryTime    = 3000
	apiKeyID        = "********"
	pingSemLimit    = 10
	channelBufSize  = 1024
	baseDuration    = 100 * time.Millisecond
	gracefulTimeout = 5 * time.Second
	reportInterval  = 5 * time.Second
	reloadInterval  = 1 * time.Hour
	maxValueLen     = 256
)

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

	prefix := parsedURL.Path
	if prefix == "" || prefix == "/" {
		prefix = defaultAPIPath
	} else {
		prefix = strings.TrimRight(prefix, "/")
	}

	execPath, _ := os.Executable()
	baseDir := filepath.Dir(execPath)

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

func (m *Master) Run() {
	scheme := "http"
	if m.mTLSConfig != nil {
		scheme = "https"
	}
	log.Printf("Master.run: started: %v://%v%v", scheme, m.endpointTCPAddr, m.prefix)

	apiKey, ok := m.findInstance(apiKeyID)
	if !ok {
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

func (m *Master) shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)

		close(m.periodicDone)
		m.shutdownSSEConnections()

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
