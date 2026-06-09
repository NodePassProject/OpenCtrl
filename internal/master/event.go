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

var checkpointRegexp = regexp.MustCompile(`CHECK_POINT\|MODE=(\d+)\|PING=(\d+)ms\|POOL=(\d+)\|TCPS=(\d+)\|UDPS=(\d+)\|TCPRX=(\d+)\|TCPTX=(\d+)\|UDPRX=(\d+)\|UDPTX=(\d+)`)

func (w *instanceLogWriter) Write(p []byte) (n int, err error) {
	scanner := bufio.NewScanner(bytes.NewReader(p))

	for scanner.Scan() {
		line := scanner.Text()
		if matches := w.checkpoint.FindStringSubmatch(line); len(matches) == 10 {
			w.instance.mu.Lock()
			for i, field := range []*int32{&w.instance.Mode, &w.instance.Ping, &w.instance.Pool, &w.instance.TCPS, &w.instance.UDPS} {
				if v, err := strconv.ParseInt(matches[i+1], 10, 32); err == nil {
					*field = int32(v)
				}
			}

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

			if w.instance.Status == "error" {
				w.instance.Status = "running"
			}

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
				return
			}
		}
	}
}

func (m *Master) sendSSEEvent(eventType string, instance *instance, logs ...string) {
	event := &instanceEvent{
		Type: eventType,
		Time: time.Now(),
	}
	if instance != nil {
		event.Instance = instance.snapshot()
	}

	if len(logs) > 0 {
		event.Logs = logs[0]
	}

	select {
	case m.notifyChannel <- event:
	default:
	}
}

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

func (s *sseSubscriber) close() {
	s.once.Do(func() {
		close(s.done)
	})
}
