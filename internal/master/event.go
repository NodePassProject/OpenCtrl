package master

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// checkpointRegexp is the child-binary progress contract consumed by the
// master. Keep this format stable for compatible third-party binaries.
//
// The format is intentionally plain text so child binaries in any language can
// emit it on stdout/stderr without linking to this Go package. Field order is
// part of the contract because the parser maps capture groups directly onto
// instance counters.
var checkpointRegexp = regexp.MustCompile(`CHECK_POINT\|MODE=(\d+)\|PING=(\d+)ms\|POOL=(\d+)\|TCPS=(\d+)\|UDPS=(\d+)\|TCPRX=(\d+)\|TCPTX=(\d+)\|UDPRX=(\d+)\|UDPTX=(\d+)`)

// Write consumes child stdout/stderr, updates checkpoint-derived metrics, and
// mirrors user-visible lines to both stdout and SSE log events.
//
// os/exec may call Write with arbitrary byte chunks, so the scanner treats each
// complete line independently. Returning len(p), nil keeps the child process
// from failing because the master chose to ignore a malformed telemetry line.
func (w *instanceLogWriter) Write(p []byte) (n int, err error) {
	scanner := bufio.NewScanner(bytes.NewReader(p))

	for scanner.Scan() {
		line := scanner.Text()
		if matches := w.checkpoint.FindStringSubmatch(line); len(matches) == 10 {
			w.instance.mu.Lock()
			// The first five captures are small signed metrics. Invalid numeric
			// fields are ignored individually so one bad value does not discard
			// the entire checkpoint.
			for i, field := range []*int32{&w.instance.Mode, &w.instance.Ping, &w.instance.Pool, &w.instance.TCPS, &w.instance.UDPS} {
				if v, err := strconv.ParseInt(matches[i+1], 10, 32); err == nil {
					*field = int32(v)
				}
			}

			// Traffic counters are absolute values from the child process plus a
			// local base for previous runs. Reset offsets let the operator zero
			// the public counters without needing child-process support.
			stats := []*uint64{&w.instance.TCPRX, &w.instance.TCPTX, &w.instance.UDPRX, &w.instance.UDPTX}
			bases := []uint64{w.instance.tcpRXBase, w.instance.tcpTXBase, w.instance.udpRXBase, w.instance.udpTXBase}
			resets := []*uint64{&w.instance.tcpRXReset, &w.instance.tcpTXReset, &w.instance.udpRXReset, &w.instance.udpTXReset}
			for i, stat := range stats {
				if v, err := strconv.ParseUint(matches[i+6], 10, 64); err == nil {
					if v >= *resets[i] {
						*stat = bases[i] + v - *resets[i]
					} else {
						*stat = bases[i] + v
						*resets[i] = 0
					}
				}
			}

			w.instance.lastCheckpoint = time.Now()

			// A valid checkpoint is stronger evidence than a previous textual
			// ERROR line, so it can promote the instance back to running.
			if w.instance.Status == "error" {
				w.instance.Status = "running"
			}

			// Deleted instances may still produce late output while the process
			// is draining. Do not recreate map state or emit updates for them.
			if !w.instance.deleted {
				w.master.instances.Store(w.instanceID, w.instance)
			}
			deleted := w.instance.deleted
			w.instance.mu.Unlock()
			if !deleted {
				w.master.sendSSEEvent("update", w.instance)
			}
			continue
		}

		w.instance.mu.Lock()
		// Ordinary ERROR lines are a low-cost signal for protocols that cannot
		// emit a structured failure event. Metrics are cleared so dashboards do
		// not show stale health after the error transition.
		if w.instance.Status != "error" && !w.instance.deleted && strings.Contains(line, "ERROR") {
			w.instance.Status = "error"
			w.instance.Ping = 0
			w.instance.Pool = 0
			w.instance.TCPS = 0
			w.instance.UDPS = 0
			w.master.instances.Store(w.instanceID, w.instance)
		}
		deleted := w.instance.deleted
		w.instance.mu.Unlock()

		// Master stdout keeps child output visible in service logs. The instance
		// suffix is added after parsing so it does not affect checkpoint format.
		fmt.Fprintf(w.target, "%s [%s]\n", line, w.instanceID)

		if !deleted {
			w.master.sendSSEEvent("log", w.instance, line)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(w.target, "%s [%s]", p, w.instanceID)
	}
	return len(p), nil
}

// handleSSE streams an initial snapshot followed by live instance events.
//
// The stream always uses event: instance, with the semantic event type inside
// the JSON payload. Clients should first process all initial events, then apply
// live events, and periodically reconcile with GET /instances because live
// events are best effort under backpressure.
func (m *Master) handleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	subscriberID := generateID()

	subscriber := &sseSubscriber{
		events: make(chan *instanceEvent, 10),
		done:   make(chan struct{}),
	}

	m.subscribers.Store(subscriberID, subscriber)
	defer func() {
		if value, exists := m.subscribers.LoadAndDelete(subscriberID); exists {
			value.(*sseSubscriber).close()
		}
	}()

	fmt.Fprintf(w, "retry: %d\n\n", sseRetryTime)

	// Send a point-in-time registry view before live events. sync.Map iteration
	// is unordered, so clients must key by instance ID rather than relying on
	// sequence.
	m.instances.Range(func(_, value any) bool {
		instance := value.(*instance)
		event := &instanceEvent{
			Type:     "initial",
			Time:     time.Now(),
			Instance: instance.snapshot(),
		}

		data, err := json.Marshal(event)
		if err == nil {
			fmt.Fprintf(w, "event: instance\ndata: %s\n\n", data)
			w.(http.Flusher).Flush()
		}
		return true
	})

	for {
		select {
		case <-r.Context().Done():
			return
		case <-subscriber.done:
			return
		case event := <-subscriber.events:
			data, err := json.Marshal(event)
			if err != nil {
				log.Printf("Master.handleSSE: event marshal failed: %v", err)
				continue
			}

			fmt.Fprintf(w, "event: instance\ndata: %s\n\n", data)
			w.(http.Flusher).Flush()
			if event.Type == "shutdown" {
				// shutdown is a terminal event for this connection. Clients are
				// expected to reconnect and re-authenticate with the current key.
				return
			}
		}
	}
}

// sendSSEEvent enqueues a best-effort event for the dispatcher. Dropping under
// backpressure keeps child process handling from blocking on slow clients.
//
// The event carries a snapshot, not the live instance pointer. That prevents a
// subscriber from observing later mutations through an older queued event.
func (m *Master) sendSSEEvent(eventType string, instance *instance, logs ...string) {
	event := &instanceEvent{
		Type: eventType,
		Time: time.Now(),
	}
	if instance != nil {
		event.Instance = instance.snapshot()
	}

	if len(logs) > 0 {
		// Preserve the logs field for client compatibility. The startup log
		// parameter was removed, but SSE log events remain part of the API.
		event.Logs = logs[0]
	}

	select {
	case m.notifyChannel <- event:
	default:
	}
}

// shutdownSSEConnections tells every live SSE subscriber to reconnect.
//
// This is used both during master shutdown and API-key rotation. A graceful
// shutdown event is preferred, but a full queue is closed immediately because
// the subscriber is already unable to keep up.
func (m *Master) shutdownSSEConnections() {
	m.subscribers.Range(func(key, value any) bool {
		subscriber := value.(*sseSubscriber)
		m.subscribers.Delete(key)
		select {
		case subscriber.events <- &instanceEvent{Type: "shutdown", Time: time.Now()}:
		default:
			subscriber.close()
		}
		return true
	})
}

// startEventDispatcher fans queued events out to all active subscribers.
//
// The dispatcher does not remove slow subscribers on every dropped event.
// Dropping keeps producers cheap; handler cleanup and shutdown paths own
// subscriber lifecycle.
func (m *Master) startEventDispatcher() {
	for event := range m.notifyChannel {
		m.subscribers.Range(func(_, value any) bool {
			subscriber := value.(*sseSubscriber)
			select {
			case <-subscriber.done:
				return true
			default:
			}
			select {
			case subscriber.events <- event:
			case <-subscriber.done:
			default:
			}
			return true
		})
	}
}

// close marks a subscriber as closed exactly once.
//
// Multiple goroutines can race to close the same subscriber: request cleanup,
// API-key rotation, and master shutdown. sync.Once makes those paths idempotent.
func (s *sseSubscriber) close() {
	s.once.Do(func() {
		close(s.done)
	})
}
