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

// validInstanceActions is the PATCH action allowlist accepted by the instance
// endpoint.
//
// The action names are part of the public JSON API. Additions should be treated
// as API changes and documented alongside client-facing behavior.
var validInstanceActions = map[string]bool{
	"start":   true,
	"stop":    true,
	"restart": true,
	"reset":   true,
}

// instancePatchRequest models the partial updates accepted by PATCH
// /instances/{id}.
//
// Pointer fields distinguish "not provided" from zero values. For Meta, the
// nested pointer lets clients update peer, tags, both, or neither.
type instancePatchRequest struct {
	Alias   string `json:"alias,omitempty"`
	Action  string `json:"action,omitempty"`
	Restart *bool  `json:"restart,omitempty"`
	Meta    *struct {
		Peer *peer             `json:"peer,omitempty"`
		Tags map[string]string `json:"tags,omitempty"`
	} `json:"meta,omitempty"`
}

// handleInstances lists instances and creates new managed child instances.
//
// GET returns a snapshot array in sync.Map iteration order, which is not
// stable. POST accepts an opaque child URL, stores the instance immediately, and
// starts it asynchronously so the HTTP response is not tied to process startup
// latency.
func (m *Master) handleInstances(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// Snapshot each record so the response cannot observe concurrent
		// lifecycle mutations while encoding.
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

		// normalizeInstanceURL validates only controller-level URL rules. The
		// scheme becomes the display type; the full URL remains owned by the
		// child binary.
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

		// Startup continues after the create response. Clients should watch SSE
		// update events or poll the instance to learn the final running/error
		// state.
		go m.startInstance(instance)

		m.saveStateAsync()
		writeJSON(w, http.StatusCreated, instance.snapshot())

		m.sendSSEEvent("create", instance)

	default:
		httpError(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleInstanceDetail routes item-level instance operations by HTTP method.
//
// The path suffix is the raw instance ID. The caller-facing not-found behavior
// is the same for missing, deleted, or concurrently replaced records.
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

// handlePatchInstance applies metadata, lifecycle, restart, reset, and API-key
// update operations without replacing the instance URL.
//
// PATCH is intentionally multi-purpose because clients often update metadata
// and request a lifecycle action from the same UI interaction. Durable field
// changes are applied first; lifecycle actions then run asynchronously unless
// the action is reset, which is purely in-memory counter state plus persistence.
func (m *Master) handlePatchInstance(w http.ResponseWriter, r *http.Request, id string, inst *instance) {
	var reqData instancePatchRequest
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		httpError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if id == apiKeyID {
		if reqData.Action == "restart" {
			// The reserved API-key record uses "restart" to mean key rotation.
			// Existing SSE clients are asked to reconnect so they re-authenticate
			// with the newly persisted key.
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
		// Bound all small operator-controlled strings before storing them in the
		// gob file or echoing them to every SSE client.
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
	// After acquiring lifecycleMu, confirm that the registry still points to the
	// same record. This prevents a stale handler from editing an instance that a
	// concurrent DELETE or PUT has already replaced.
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
		// Reset zeroes the public counters while preserving future accumulation.
		// The current totals become reset offsets, and the next checkpoint is
		// interpreted relative to those offsets.
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
			// Tags are full replacement, not merge, so clients can remove keys by
			// sending the desired final map.
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
		// Lifecycle actions are intentionally launched after the response state
		// update path. Clients should use SSE/polling for completion.
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

// handlePutInstance replaces the child URL and restarts the instance under the
// new command.
//
// PUT is a full URL replacement, not a metadata update. It stops the current
// child under the same lifecycle lock, swaps the opaque URL/type, then starts a
// new child with the replacement command.
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
	// Revalidate after the lifecycle lock so stale handlers cannot replace an
	// instance that has been deleted or superseded.
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
		// Treat an identical replacement as a conflict to signal that no new
		// command was accepted.
		inst.mu.Unlock()
		inst.lifecycleMu.Unlock()
		httpError(w, "instance URL conflict", http.StatusConflict)
		return
	}
	inst.mu.Unlock()

	// Stop and start while holding lifecycleMu so no other lifecycle request can
	// interleave with the URL swap.
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

// handleDeleteInstance removes an instance after its child process has stopped.
//
// The deleted flag is set before stopping so late child output cannot write the
// instance back into the registry or emit fresh updates while deletion is in
// progress.
func (m *Master) handleDeleteInstance(w http.ResponseWriter, id string, inst *instance) {
	if id == apiKeyID {
		httpError(w, "Forbidden: API Key", http.StatusForbidden)
		return
	}

	inst = m.currentInstance(inst)
	inst.lifecycleMu.Lock()
	// Revalidate the registry pointer under lifecycleMu for the same stale
	// handler protection used by PATCH and PUT.
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

// handleInfo reads or updates master-level identity metadata.
//
// The alias is stored both on Master and in the reserved API-key instance so it
// survives restart through the existing state file.
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

// handleTCPPing performs a bounded TCP dial probe for UI diagnostics.
//
// The endpoint returns HTTP 200 with an embedded Error for probe failures. That
// keeps transport-level API reachability separate from target reachability.
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
		// Shed load quickly when too many probes are already in flight.
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
