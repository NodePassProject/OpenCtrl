package master

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"maps"
	"net/http"
	"time"
)

// setCORSHeaders applies the permissive browser policy used by the local
// control-plane API.
//
// Authentication still happens through X-API-Key. The permissive origin policy
// is intentional because OpenCtrl clients are often local dashboards, static
// files, or reverse-proxied tools that do not share a fixed origin.
func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, Cache-Control")
}

// httpError writes a JSON error response using the API's shared envelope.
//
// The helper also sets CORS headers because errors can be emitted before a
// handler reaches the normal response path.
func httpError(w http.ResponseWriter, message string, statusCode int) {
	setCORSHeaders(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// writeJSON writes a JSON response with common API headers.
//
// Encoding errors are intentionally ignored because all current callers pass
// simple structs, maps, or slices. A future caller with custom marshaling should
// handle its own error path before calling this helper.
func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	setCORSHeaders(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// generateID returns a compact random identifier for instance records.
//
// The 32-bit space is small but adequate for locally managed instance IDs; the
// create handler still checks for collisions before accepting the ID.
func generateID() string {
	var b [4]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// generateMasterID returns the persistent random identity for this master.
//
// The value is stored in the reserved API-key record rather than a separate
// config file so state has one persistence mechanism.
func generateMasterID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// generateAPIKey returns the bearer secret expected in X-API-Key.
//
// The 128-bit random value is hex encoded for copy/paste friendliness and can
// be rotated through the reserved API-key instance.
func generateAPIKey() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// cloneTags makes tag maps safe to share across snapshots and request updates.
//
// A nil input becomes an empty map so JSON responses consistently expose an
// object for tags and callers can mutate the result without nil checks.
func cloneTags(tags map[string]string) map[string]string {
	if tags == nil {
		return make(map[string]string)
	}

	cloned := make(map[string]string, len(tags))
	maps.Copy(cloned, tags)
	return cloned
}

// resetStoppedInstanceLocked normalizes runtime-only fields after an instance
// has fully stopped. The caller must hold instance.mu.
//
// Traffic counters are deliberately left untouched: stopping a process should
// not erase historical byte totals. Health-style metrics are cleared because
// they only describe a live child.
func resetStoppedInstanceLocked(instance *instance) {
	instance.Status = "stopped"
	instance.cmd = nil
	instance.stopped = make(chan struct{})
	instance.exited = nil
	instance.cancelFunc = nil
	instance.lastCheckpoint = time.Time{}
	instance.Ping = 0
	instance.Pool = 0
	instance.TCPS = 0
	instance.UDPS = 0
}
