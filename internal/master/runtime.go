package master

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os/exec"
	"time"
)

func (m *Master) runTelemetry(ctx context.Context, inst *instance, cmd *exec.Cmd) {
	backoff := time.Second
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
				m.applyTelemetryHello(inst, cmd, hello)
				err = m.consumeTelemetry(ctx, inst, cmd, conn)
				backoff = time.Second
				conn.Close()
				if ctx.Err() != nil {
					return
				}
				m.clearActiveTelemetry(inst, cmd)
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
	for {
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		message, err := readTelemetryFrame(conn)
		if err != nil {
			return err
		}
		if !m.telemetryCurrent(inst, cmd) {
			return context.Canceled
		}
		if message.Type != "snapshot" {
			return fmt.Errorf("expected telemetry snapshot")
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(message.Data, &fields) != nil || len(fields) != 6 {
			return fmt.Errorf("incomplete telemetry snapshot")
		}
		for _, name := range [...]string{
			"tcp_upload_bytes", "tcp_download_bytes", "udp_upload_bytes", "udp_download_bytes",
			"tcp_active", "udp_active",
		} {
			field, present := fields[name]
			if !present {
				return fmt.Errorf("incomplete telemetry snapshot")
			}
			if bytes.Equal(bytes.TrimSpace(field), []byte("null")) {
				return fmt.Errorf("null telemetry statistic")
			}
		}
		decoder := json.NewDecoder(bytes.NewReader(message.Data))
		decoder.DisallowUnknownFields()
		var value telemetrySnapshot
		if decoder.Decode(&value) != nil || value.TCPActive < 0 || value.UDPActive < 0 {
			return fmt.Errorf("invalid telemetry snapshot")
		}
		m.applyTelemetrySnapshot(inst, cmd, value)
	}
}

func (m *Master) clearActiveTelemetry(inst *instance, cmd *exec.Cmd) {
	inst.mu.Lock()
	changed := telemetryCurrentLocked(inst, cmd) && (inst.TCPS != 0 || inst.UDPS != 0)
	if changed {
		inst.TCPS, inst.UDPS = 0, 0
	}
	inst.mu.Unlock()
	if changed {
		m.sendSSEEvent("update", inst)
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
	if !inst.deleted && inst.cmd == cmd {
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
	defer inst.mu.Unlock()
	if telemetryCurrentLocked(inst, cmd) {
		inst.telemetryIdentity = "nowhere." + hello.Instance.ID
	}
}

func (m *Master) applyTelemetrySnapshot(inst *instance, cmd *exec.Cmd, value telemetrySnapshot) {
	inst.mu.Lock()
	current := telemetryCurrentLocked(inst, cmd)
	if current {
		inst.TCPS = clampSignedInt32(value.TCPActive)
		inst.UDPS = clampSignedInt32(value.UDPActive)
		applyCounter(&inst.TCPRX, inst.tcpRXBase, &inst.tcpRXReset, value.TCPUpload)
		applyCounter(&inst.TCPTX, inst.tcpTXBase, &inst.tcpTXReset, value.TCPDownload)
		applyCounter(&inst.UDPRX, inst.udpRXBase, &inst.udpRXReset, value.UDPUpload)
		applyCounter(&inst.UDPTX, inst.udpTXBase, &inst.udpTXReset, value.UDPDownload)
	}
	inst.mu.Unlock()
	if current {
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

func clampSignedInt32(value int64) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < 0 {
		return 0
	}
	return int32(value)
}
