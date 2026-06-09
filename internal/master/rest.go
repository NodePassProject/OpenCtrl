package master

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

var validInstanceActions = map[string]bool{
	"start":   true,
	"stop":    true,
	"restart": true,
	"reset":   true,
}

type instancePatchRequest struct {
	Alias   string `json:"alias,omitempty"`
	Action  string `json:"action,omitempty"`
	Restart *bool  `json:"restart,omitempty"`
	Meta    *struct {
		Peer *peer             `json:"peer,omitempty"`
		Tags map[string]string `json:"tags,omitempty"`
	} `json:"meta,omitempty"`
}

func (m *Master) handleInstances(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		instances := []*instance{}
		m.instances.Range(func(_, value any) bool {
			instances = append(instances, value.(*instance).snapshot())
			return true
		})
		writeJSON(w, http.StatusOK, instances)

	case http.MethodPost:
		var reqData struct {
			Alias string `json:"alias"`
			URL   string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil || reqData.URL == "" {
			httpError(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		instanceType, enhancedURL, err := m.normalizeInstanceURL(reqData.URL)
		if err != nil {
			httpError(w, err.Error(), http.StatusBadRequest)
			return
		}

		id := generateID()
		if _, exists := m.instances.Load(id); exists {
			httpError(w, "instance ID already exists", http.StatusConflict)
			return
		}

		instance := &instance{
			ID:      id,
			Alias:   reqData.Alias,
			Type:    instanceType,
			URL:     enhancedURL,
			Status:  "stopped",
			Restart: true,
			Meta:    meta{Tags: make(map[string]string)},
			stopped: make(chan struct{}),
		}

		m.instances.Store(id, instance)

		go m.startInstance(instance)

		m.saveStateAsync()
		writeJSON(w, http.StatusCreated, instance.snapshot())

		m.sendSSEEvent("create", instance)

	default:
		httpError(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *Master) handleInstanceDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, fmt.Sprintf("%s/instances/", m.prefix))
	if id == "" || id == "/" {
		httpError(w, "instance ID is required", http.StatusBadRequest)
		return
	}

	instance, ok := m.findInstance(id)
	if !ok {
		httpError(w, "instance not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, instance.snapshot())
	case http.MethodPatch:
		m.handlePatchInstance(w, r, id, instance)
	case http.MethodPut:
		m.handlePutInstance(w, r, id, instance)
	case http.MethodDelete:
		m.handleDeleteInstance(w, id, instance)
	default:
		httpError(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *Master) handlePatchInstance(w http.ResponseWriter, r *http.Request, id string, inst *instance) {
	var reqData instancePatchRequest
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		httpError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if id == apiKeyID {
		if reqData.Action == "restart" {
			inst.mu.Lock()
			inst.URL = generateAPIKey()
			m.instances.Store(apiKeyID, inst)
			apiKey := inst.URL
			inst.mu.Unlock()

			log.Printf("Master.handlePatchInstance: API key regenerated: %v", apiKey)
			m.saveStateAsync()
			go m.shutdownSSEConnections()
			m.sendSSEEvent("update", inst)
		}
		writeJSON(w, http.StatusOK, inst.snapshot())
		return
	}

	if reqData.Alias != "" && len(reqData.Alias) > maxValueLen {
		httpError(w, fmt.Sprintf("instance alias exceeds maximum length %d", maxValueLen), http.StatusBadRequest)
		return
	}
	if reqData.Action != "" && !validInstanceActions[reqData.Action] {
		httpError(w, fmt.Sprintf("Invalid action: %s", reqData.Action), http.StatusBadRequest)
		return
	}
	if reqData.Meta != nil {
		if reqData.Meta.Peer != nil {
			if len(reqData.Meta.Peer.SID) > maxValueLen {
				httpError(w, fmt.Sprintf("meta peer.sid exceeds maximum length %d", maxValueLen), http.StatusBadRequest)
				return
			}
			if len(reqData.Meta.Peer.Type) > maxValueLen {
				httpError(w, fmt.Sprintf("meta peer.type exceeds maximum length %d", maxValueLen), http.StatusBadRequest)
				return
			}
			if len(reqData.Meta.Peer.Alias) > maxValueLen {
				httpError(w, fmt.Sprintf("meta peer.alias exceeds maximum length %d", maxValueLen), http.StatusBadRequest)
				return
			}
		}
		for key, value := range reqData.Meta.Tags {
			if len(key) > maxValueLen {
				httpError(w, fmt.Sprintf("meta tag key exceeds maximum length %d", maxValueLen), http.StatusBadRequest)
				return
			}
			if len(value) > maxValueLen {
				httpError(w, fmt.Sprintf("meta tag value exceeds maximum length %d", maxValueLen), http.StatusBadRequest)
				return
			}
		}
	}

	inst = m.currentInstance(inst)
	inst.lifecycleMu.Lock()
	if value, exists := m.instances.Load(id); !exists || value.(*instance) != inst {
		inst.lifecycleMu.Unlock()
		httpError(w, "instance not found", http.StatusNotFound)
		return
	}

	inst.mu.Lock()
	if inst.deleted {
		inst.mu.Unlock()
		inst.lifecycleMu.Unlock()
		httpError(w, "instance not found", http.StatusNotFound)
		return
	}
	changed := false
	if reqData.Alias != "" && inst.Alias != reqData.Alias {
		inst.Alias = reqData.Alias
		changed = true
	}
	if reqData.Action == "reset" {
		inst.tcpRXReset = inst.TCPRX - inst.tcpRXBase
		inst.tcpTXReset = inst.TCPTX - inst.tcpTXBase
		inst.udpRXReset = inst.UDPRX - inst.udpRXBase
		inst.udpTXReset = inst.UDPTX - inst.udpTXBase
		inst.TCPRX, inst.TCPTX, inst.UDPRX, inst.UDPTX = 0, 0, 0, 0
		inst.tcpRXBase, inst.tcpTXBase, inst.udpRXBase, inst.udpTXBase = 0, 0, 0, 0
		changed = true
	}
	if reqData.Restart != nil && inst.Restart != *reqData.Restart {
		inst.Restart = *reqData.Restart
		changed = true
	}
	if reqData.Meta != nil {
		if reqData.Meta.Peer != nil {
			inst.Meta.Peer = *reqData.Meta.Peer
		}
		if reqData.Meta.Tags != nil {
			inst.Meta.Tags = cloneTags(reqData.Meta.Tags)
		}
		changed = true
	}
	if changed {
		m.instances.Store(id, inst)
	}
	inst.mu.Unlock()
	inst.lifecycleMu.Unlock()

	if changed {
		m.saveStateAsync()
		log.Printf("Master.handlePatchInstance: instance updated [%v]", inst.ID)
		m.sendSSEEvent("update", inst)
	}

	if reqData.Action != "" && reqData.Action != "reset" {
		switch reqData.Action {
		case "start":
			go m.startInstance(inst)
		case "stop":
			go m.stopInstance(inst)
		case "restart":
			go m.restartInstance(inst)
		}
	}

	writeJSON(w, http.StatusOK, inst.snapshot())
}

func (m *Master) handlePutInstance(w http.ResponseWriter, r *http.Request, id string, inst *instance) {
	if id == apiKeyID {
		httpError(w, "Forbidden: API Key", http.StatusForbidden)
		return
	}

	var reqData struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil || reqData.URL == "" {
		httpError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	instanceType, enhancedURL, err := m.normalizeInstanceURL(reqData.URL)
	if err != nil {
		httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	inst = m.currentInstance(inst)
	inst.lifecycleMu.Lock()
	if value, exists := m.instances.Load(id); !exists || value.(*instance) != inst {
		inst.lifecycleMu.Unlock()
		httpError(w, "instance not found", http.StatusNotFound)
		return
	}

	inst.mu.Lock()
	if inst.deleted {
		inst.mu.Unlock()
		inst.lifecycleMu.Unlock()
		httpError(w, "instance not found", http.StatusNotFound)
		return
	}
	if inst.URL == enhancedURL {
		inst.mu.Unlock()
		inst.lifecycleMu.Unlock()
		httpError(w, "instance URL conflict", http.StatusConflict)
		return
	}
	inst.mu.Unlock()

	m.stopInstanceLocked(inst)

	inst.mu.Lock()
	inst.URL = enhancedURL
	inst.Type = instanceType
	inst.Status = "stopped"
	m.instances.Store(inst.ID, inst)
	inst.mu.Unlock()

	m.startInstanceLocked(inst)
	inst.lifecycleMu.Unlock()

	m.saveStateAsync()
	writeJSON(w, http.StatusOK, inst.snapshot())

	log.Printf("Master.handlePutInstance: instance URL updated: %v [%v]", enhancedURL, inst.ID)
}

func (m *Master) handleDeleteInstance(w http.ResponseWriter, id string, inst *instance) {
	if id == apiKeyID {
		httpError(w, "Forbidden: API Key", http.StatusForbidden)
		return
	}

	inst = m.currentInstance(inst)
	inst.lifecycleMu.Lock()
	if value, exists := m.instances.Load(id); !exists || value.(*instance) != inst {
		inst.lifecycleMu.Unlock()
		httpError(w, "instance not found", http.StatusNotFound)
		return
	}

	inst.mu.Lock()
	if inst.deleted {
		inst.mu.Unlock()
		inst.lifecycleMu.Unlock()
		httpError(w, "instance not found", http.StatusNotFound)
		return
	}
	inst.deleted = true
	m.instances.Store(id, inst)
	inst.mu.Unlock()

	m.stopInstanceLocked(inst)
	m.instances.Delete(id)
	inst.lifecycleMu.Unlock()

	m.saveStateAsync()
	w.WriteHeader(http.StatusNoContent)
	m.sendSSEEvent("delete", inst)
}

func (m *Master) handleInfo(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, m.masterInfo())

	case http.MethodPost:
		var reqData struct {
			Alias string `json:"alias"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
			httpError(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if len(reqData.Alias) > maxValueLen {
			httpError(w, fmt.Sprintf("Master alias exceeds maximum length %d", maxValueLen), http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		m.alias = reqData.Alias
		alias := m.alias
		m.mu.Unlock()

		if apiKey, ok := m.findInstance(apiKeyID); ok {
			apiKey.mu.Lock()
			apiKey.Alias = alias
			m.instances.Store(apiKeyID, apiKey)
			apiKey.mu.Unlock()
			m.saveStateAsync()
		}

		writeJSON(w, http.StatusOK, m.masterInfo())

	default:
		httpError(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *Master) handleTCPPing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	target := r.URL.Query().Get("target")
	if target == "" {
		httpError(w, "Target address required", http.StatusBadRequest)
		return
	}

	result := &tcpPingResult{
		Target:    target,
		Connected: false,
		Latency:   0,
		Error:     nil,
	}

	select {
	case m.tcpPingSem <- struct{}{}:
		defer func() { <-m.tcpPingSem }()
	case <-time.After(time.Second):
		errMsg := "too many requests"
		result.Error = &errMsg
		writeJSON(w, http.StatusOK, result)
		return
	}

	start := time.Now()
	conn, err := net.DialTimeout("tcp", target, reportInterval)
	if err != nil {
		errMsg := err.Error()
		result.Error = &errMsg
		writeJSON(w, http.StatusOK, result)
		return
	}

	result.Connected = true
	result.Latency = time.Since(start).Milliseconds()
	conn.Close()
	writeJSON(w, http.StatusOK, result)
}
