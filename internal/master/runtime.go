package master

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os/exec"
	"strings"
	"time"
)

func (m *Master) runTelemetry(ctx context.Context, inst *instance, cmd *exec.Cmd) {
	backoff := time.Second
	hadConnection := false
	inst.mu.Lock()
	role := inst.Type
	inst.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return
		}
		entries, err := discoverTelemetry(uint32(cmd.Process.Pid))
		if err == nil {
			for _, entry := range entries {
				inst.mu.Lock()
				identityMatches := inst.telemetryIdentity == "" || inst.telemetryIdentity == entry.Instance.RegistryName
				inst.mu.Unlock()
				if !identityMatches {
					continue
				}
				conn, hello, connectErr := connectTelemetry(ctx, entry)
				if connectErr != nil || hello.Instance.Role != role {
					if conn != nil {
						conn.Close()
					}
					continue
				}
				if hadConnection {
					m.sendTelemetryLog(inst, cmd, "Telemetry reconnected; events emitted while disconnected may be missing")
				}
				hadConnection = true
				m.applyTelemetryHello(inst, cmd, hello)
				connectedAt := time.Now()
				err = m.consumeTelemetry(ctx, inst, cmd, conn)
				conn.Close()
				inst.mu.Lock()
				if inst.lastTelemetry.After(connectedAt) {
					backoff = time.Second
				}
				inst.mu.Unlock()
				if ctx.Err() != nil {
					return
				}
				m.sendTelemetryLog(inst, cmd, "Telemetry disconnected; reconnecting and live events may be missing")
				break
			}
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (m *Master) consumeTelemetry(ctx context.Context, inst *instance, cmd *exec.Cmd, conn net.Conn) error {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()
	subscribed := false
	for {
		inst.mu.Lock()
		timeout := max(15*time.Second, 3*inst.telemetryInterval)
		inst.mu.Unlock()
		if !subscribed {
			timeout = 5 * time.Second
		}
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		message, err := readTelemetryFrame(conn)
		if err != nil {
			return err
		}
		if !m.telemetryCurrent(inst, cmd) {
			return context.Canceled
		}
		if !subscribed && message.Type != "subscribed" && message.Type != "error" {
			return fmt.Errorf("telemetry subscription was not acknowledged")
		}
		switch message.Type {
		case "subscribed":
			var value struct {
				RequestID    uint64 `json:"request_id"`
				Subscription string `json:"subscription"`
			}
			if subscribed || json.Unmarshal(message.Data, &value) != nil || value.RequestID != 1 || value.Subscription != "detail" {
				return fmt.Errorf("invalid telemetry subscription acknowledgement")
			}
			subscribed = true
		case "access_start":
		case "snapshot":
			var value telemetrySnapshot
			if json.Unmarshal(message.Data, &value) != nil {
				return fmt.Errorf("invalid telemetry snapshot")
			}
			m.applyTelemetrySnapshot(inst, cmd, value)
		case "lifecycle":
			var value telemetryLifecycle
			if json.Unmarshal(message.Data, &value) != nil {
				return fmt.Errorf("invalid telemetry lifecycle")
			}
			m.applyTelemetryLifecycle(inst, cmd, value)
		case "runtime_event":
			var value telemetryRuntime
			if json.Unmarshal(message.Data, &value) != nil {
				return fmt.Errorf("invalid telemetry runtime event")
			}
			m.sendTelemetryLog(inst, cmd, formatRuntimeEvent(value))
		case "access_finish":
			var value telemetryAccessFinish
			if json.Unmarshal(message.Data, &value) != nil {
				return fmt.Errorf("invalid telemetry access event")
			}
			m.sendTelemetryLog(inst, cmd, formatAccessFinish(value))
		case "gap":
			var value telemetryGap
			if json.Unmarshal(message.Data, &value) != nil {
				return fmt.Errorf("invalid telemetry gap")
			}
			m.sendTelemetryLog(inst, cmd, fmt.Sprintf("Telemetry dropped %d live event(s)", value.Missed))
		case "error":
			return fmt.Errorf("telemetry server rejected subscription")
		default:
			return fmt.Errorf("unknown telemetry message %q", message.Type)
		}
	}
}

func (m *Master) telemetryCurrent(inst *instance, cmd *exec.Cmd) bool {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	return telemetryCurrentLocked(inst, cmd)
}

func (m *Master) sendTelemetryLog(inst *instance, cmd *exec.Cmd, message string) {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if telemetryCurrentLocked(inst, cmd) {
		m.publishEvent(&instanceEvent{Type: "log", Time: time.Now(), Instance: inst.snapshotLocked(), Logs: message})
	}
}

func telemetryCurrentLocked(inst *instance, cmd *exec.Cmd) bool {
	if inst.deleted || inst.cmd != cmd {
		return false
	}
	select {
	case <-inst.stopped:
		return false
	default:
		return true
	}
}

func (m *Master) applyTelemetryHello(inst *instance, cmd *exec.Cmd, hello telemetryHello) {
	inst.mu.Lock()
	if telemetryCurrentLocked(inst, cmd) {
		inst.telemetryIdentity = "nowhere." + hello.Instance.ID
		inst.telemetryInterval = time.Duration(hello.Instance.TelemetryIntervalMS) * time.Millisecond
		inst.telemetryReady = hello.Lifecycle == "READY"
	}
	inst.mu.Unlock()
	m.applyTelemetryLifecycle(inst, cmd, telemetryLifecycle{State: hello.Lifecycle, Reason: hello.Reason})
}

func (m *Master) applyTelemetrySnapshot(inst *instance, cmd *exec.Cmd, value telemetrySnapshot) {
	if value.Sequence == 0 {
		return
	}
	statusChanged := false
	inst.mu.Lock()
	if telemetryCurrentLocked(inst, cmd) && value.Sequence > inst.lastSequence {
		inst.lastSequence = value.Sequence
		inst.Ping = clampInt32(value.PingMS)
		inst.Pool = 0
		inst.TCPS = clampSignedInt32(value.TCPActive)
		inst.UDPS = clampSignedInt32(value.UDPActive)
		applyCounter(&inst.TCPRX, inst.tcpRXBase, &inst.tcpRXReset, value.TCPLogicalUp)
		applyCounter(&inst.TCPTX, inst.tcpTXBase, &inst.tcpTXReset, value.TCPLogicalDown)
		applyCounter(&inst.UDPRX, inst.udpRXBase, &inst.udpRXReset, value.UDPLogicalUp)
		applyCounter(&inst.UDPTX, inst.udpTXBase, &inst.udpTXReset, value.UDPLogicalDown)
		inst.lastTelemetry = time.Now()
		if inst.telemetryReady && inst.Status == "error" && !inst.runtimeFailure {
			inst.Status = "running"
			statusChanged = true
		}
	}
	inst.mu.Unlock()
	if statusChanged {
		m.sendSSEEvent("update", inst)
	}
}

func (m *Master) applyTelemetryLifecycle(inst *instance, cmd *exec.Cmd, value telemetryLifecycle) {
	changed := false
	inst.mu.Lock()
	if telemetryCurrentLocked(inst, cmd) {
		inst.telemetryReady = value.State == "READY"
		if value.Reason == "START_FAILED" || value.Reason == "TCP_LISTENER_EXIT" || value.Reason == "QUIC_LISTENER_EXIT" || value.Reason == "SOCKS_LISTENER_EXIT" {
			inst.runtimeFailure = true
			if inst.Status != "error" {
				inst.Status = "error"
				changed = true
			}
		}
	}
	inst.mu.Unlock()
	if changed {
		m.sendSSEEvent("update", inst)
	}
}

func applyCounter(destination *uint64, base uint64, reset *uint64, value uint64) {
	var delta uint64
	if value >= *reset {
		delta = value - *reset
	} else {
		delta = value
		*reset = 0
	}
	if delta > math.MaxUint64-base {
		*destination = math.MaxUint64
	} else {
		*destination = base + delta
	}
}

func clampInt32(value uint64) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(value)
}

func clampSignedInt32(value int64) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < 0 {
		return 0
	}
	return int32(value)
}

func telemetryTime(milliseconds uint64) string {
	return time.UnixMilli(int64(milliseconds)).Local().Format("2006-01-02 15:04:05.000")
}

func formatRuntimeEvent(value telemetryRuntime) string {
	client := ""
	if value.Client != nil {
		client = " [" + *value.Client + "]"
	}
	return fmt.Sprintf("%s  %s  %s: %s%s", telemetryTime(value.TimestampMS), strings.ToUpper(value.Level), value.Kind, value.Message, client)
}

func formatAccessFinish(value telemetryAccessFinish) string {
	client := "-"
	if value.Client != nil {
		client = *value.Client
	}
	detail := ""
	if value.Error != nil {
		detail = ": " + *value.Error
	}
	return fmt.Sprintf("%s  INFO  %s %s %s -> %s %dms up=%d down=%d%s", telemetryTime(value.TimestampMS), strings.ToUpper(value.Protocol), strings.ToUpper(value.Outcome), client, value.Target, value.DurationMS, value.UploadBytes, value.DownloadBytes, detail)
}
