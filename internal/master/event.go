package master

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (m *Master) handleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	controller := http.NewResponseController(w)
	write := func(event *instanceEvent) error {
		if err := controller.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		data, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: instance\ndata: %s\n\n", data); err != nil {
			return err
		}
		return controller.Flush()
	}
	subscriber := &sseSubscriber{
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
		states: make(map[string]*instanceEvent),
	}
	m.subscribers.Store(subscriber, subscriber)
	defer func() {
		m.subscribers.Delete(subscriber)
		subscriber.close()
	}()
	if err := controller.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return
	}
	if _, err := fmt.Fprintf(w, "retry: %d\n\n", sseRetryTime); err != nil {
		return
	}
	failed := false
	m.instances.Range(func(_, value any) bool {
		event := &instanceEvent{Type: "initial", Time: time.Now(), Instance: value.(*instance).snapshot()}
		failed = write(event) != nil
		return !failed
	})
	if failed || controller.Flush() != nil {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-subscriber.done:
			return
		case <-subscriber.wake:
			for {
				event := subscriber.next()
				if event == nil {
					break
				}
				if write(event) != nil || event.Type == "shutdown" {
					return
				}
			}
		}
	}
}

func (m *Master) sendSSEEvent(eventType string, inst *instance, logs ...string) {
	event := &instanceEvent{Type: eventType, Time: time.Now()}
	if inst != nil {
		event.Instance = inst.snapshot()
	}
	if len(logs) > 0 {
		event.Logs = logs[0]
	}
	m.publishEvent(event)
}

func (m *Master) publishEvent(event *instanceEvent) {
	m.subscribers.Range(func(_, value any) bool {
		value.(*sseSubscriber).enqueue(event)
		return true
	})
}

func (m *Master) shutdownSSEConnections() {
	m.subscribers.Range(func(key, value any) bool {
		m.subscribers.Delete(key)
		value.(*sseSubscriber).enqueue(&instanceEvent{Type: "shutdown", Time: time.Now()})
		return true
	})
}

func (s *sseSubscriber) enqueue(event *instanceEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		return
	default:
	}
	if event.Type == "log" {
		if len(s.logs) == 10 {
			s.close()
			return
		}
		s.logs = append(s.logs, event)
	} else {
		key := ""
		if event.Instance != nil {
			key = event.Instance.ID
		}
		if _, exists := s.states[key]; !exists && len(s.states) == channelBufSize {
			s.close()
			return
		}
		s.states[key] = event
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *sseSubscriber) next() *instanceEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	default:
	}
	var selected *instanceEvent
	var key string
	for id, event := range s.states {
		if selected == nil || event.Time.Before(selected.Time) {
			selected, key = event, id
		}
	}
	if len(s.logs) > 0 && (selected == nil || s.logs[0].Time.Before(selected.Time)) {
		event := s.logs[0]
		s.logs[0] = nil
		s.logs = s.logs[1:]
		return event
	}
	if selected != nil {
		delete(s.states, key)
		return selected
	}
	return nil
}

func (s *sseSubscriber) close() {
	s.once.Do(func() { close(s.done) })
}
