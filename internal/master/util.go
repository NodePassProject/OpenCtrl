package master

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"maps"
	"net/http"
	"time"
)

func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, Cache-Control")
}

func httpError(w http.ResponseWriter, message string, statusCode int) {
	setCORSHeaders(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	setCORSHeaders(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func generateID() string {
	var b [4]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func generateMasterID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func generateAPIKey() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func cloneTags(tags map[string]string) map[string]string {
	if tags == nil {
		return make(map[string]string)
	}

	cloned := make(map[string]string, len(tags))
	maps.Copy(cloned, tags)
	return cloned
}

func resetStoppedInstanceLocked(instance *instance) {
	instance.Status = "stopped"
	instance.cmd = nil
	instance.stopped = make(chan struct{})
	instance.exited = nil
	instance.cancelFunc = nil
	instance.lastTelemetry = time.Time{}
	instance.telemetryInterval = 0
	instance.telemetryReady = false
	instance.runtimeFailure = false
	instance.Ping = 0
	instance.Pool = 0
	instance.TCPS = 0
	instance.UDPS = 0
}
