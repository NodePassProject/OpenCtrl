package master

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	telemetryProtocol = "nowhere.telemetry"
	maxTelemetryFrame = 64 * 1024
	maxRegistrySize   = 4096
)

type telemetryRegistry struct {
	Instance  discoveredInstance `json:"instance"`
	Endpoint  string             `json:"endpoint"`
	Transport string             `json:"transport"`
	Namespace string             `json:"namespace"`
	Protocol  string             `json:"protocol"`
}

type discoveredInstance struct {
	RegistryName string `json:"registry_name"`
	UID          uint32 `json:"uid"`
	PID          uint32 `json:"pid"`
	Incarnation  uint64 `json:"incarnation"`
}

type telemetryEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type telemetryHello struct {
	Instance  telemetryDescriptor `json:"instance"`
	Lifecycle string              `json:"lifecycle"`
	Reason    string              `json:"lifecycle_reason"`
}

type telemetryDescriptor struct {
	Protocol            string `json:"telemetry_protocol"`
	ID                  string `json:"id"`
	Role                string `json:"role"`
	PID                 uint32 `json:"pid"`
	Version             string `json:"version"`
	Endpoint            string `json:"endpoint"`
	ConfigSummary       string `json:"config_summary"`
	TelemetryIntervalMS uint64 `json:"telemetry_interval_ms"`
}

type telemetrySnapshot struct {
	Sequence       uint64 `json:"sequence"`
	TimestampMS    uint64 `json:"timestamp_ms"`
	TCPLogicalUp   uint64 `json:"tcp_logical_up"`
	TCPLogicalDown uint64 `json:"tcp_logical_down"`
	UDPLogicalUp   uint64 `json:"udp_logical_up"`
	UDPLogicalDown uint64 `json:"udp_logical_down"`
	TCPActive      int64  `json:"tcp_active"`
	UDPActive      int64  `json:"udp_active"`
	PingMS         uint64 `json:"ping_ms"`
}

type telemetryLifecycle struct {
	State       string `json:"state"`
	Reason      string `json:"reason"`
	TimestampMS uint64 `json:"timestamp_ms"`
}

type telemetryRuntime struct {
	TimestampMS uint64  `json:"timestamp_ms"`
	Level       string  `json:"level"`
	Kind        string  `json:"kind"`
	Message     string  `json:"message"`
	Client      *string `json:"client"`
}

type telemetryAccessFinish struct {
	TimestampMS   uint64  `json:"timestamp_ms"`
	DurationMS    uint64  `json:"duration_ms"`
	Protocol      string  `json:"protocol"`
	Client        *string `json:"client"`
	Target        string  `json:"target"`
	Outcome       string  `json:"outcome"`
	UploadBytes   uint64  `json:"upload_bytes"`
	DownloadBytes uint64  `json:"download_bytes"`
	Error         *string `json:"error"`
}

type telemetryGap struct {
	Missed uint64 `json:"missed"`
}

func discoverTelemetry(pid uint32) ([]telemetryRegistry, error) {
	directory, err := telemetryDirectory()
	if err != nil {
		return nil, err
	}
	if err := validateTelemetryDirectory(directory); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var found []telemetryRegistry
	for _, item := range entries {
		if item.IsDir() || !strings.HasPrefix(item.Name(), "nowhere.") || !strings.HasSuffix(item.Name(), ".json") {
			continue
		}
		path := filepath.Join(directory, item.Name())
		payload, err := readTelemetryRegistry(path)
		if err != nil {
			continue
		}
		var entry telemetryRegistry
		if json.Unmarshal(payload, &entry) != nil || entry.Instance.PID != pid || !validTelemetryRegistry(entry, path) {
			continue
		}
		found = append(found, entry)
	}
	sort.Slice(found, func(i, j int) bool { return found[i].Instance.RegistryName < found[j].Instance.RegistryName })
	return found, nil
}

func validTelemetryRegistry(entry telemetryRegistry, path string) bool {
	id, ok := strings.CutPrefix(entry.Instance.RegistryName, "nowhere.")
	if !ok || len(id) != 32 || !lowerHex(id) || entry.Protocol != telemetryProtocol || entry.Transport != telemetryTransport() {
		return false
	}
	if filepath.Base(path) != entry.Instance.RegistryName+".json" || entry.Endpoint != telemetryEndpoint(id) {
		return false
	}
	namespace, err := telemetryNamespace()
	return err == nil && entry.Namespace == namespace && validateTelemetryProcess(entry.Instance)
}

func lowerHex(value string) bool {
	for _, c := range []byte(value) {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func connectTelemetry(ctx context.Context, entry telemetryRegistry) (net.Conn, telemetryHello, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := dialTelemetry(dialCtx, entry.Endpoint, entry.Instance.PID)
	if err != nil {
		return nil, telemetryHello{}, err
	}
	closeWithError := func(err error) (net.Conn, telemetryHello, error) {
		conn.Close()
		return nil, telemetryHello{}, err
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return closeWithError(err)
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	message, err := readTelemetryFrame(conn)
	if err != nil {
		return closeWithError(err)
	}
	if message.Type != "hello" {
		return closeWithError(fmt.Errorf("telemetry did not begin with hello"))
	}
	var hello telemetryHello
	if err := json.Unmarshal(message.Data, &hello); err != nil {
		return closeWithError(err)
	}
	id := strings.TrimPrefix(entry.Instance.RegistryName, "nowhere.")
	if hello.Instance.Protocol != telemetryProtocol || hello.Instance.ID != id || hello.Instance.PID != entry.Instance.PID {
		return closeWithError(fmt.Errorf("telemetry hello identity mismatch"))
	}
	if hello.Instance.TelemetryIntervalMS < 250 || hello.Instance.TelemetryIntervalMS > 60_000 {
		return closeWithError(fmt.Errorf("telemetry interval is outside the contract"))
	}
	command := map[string]any{"type": "subscribe", "data": map[string]any{"request_id": uint64(1), "subscription": "detail"}}
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return closeWithError(err)
	}
	if err := writeTelemetryFrame(conn, command); err != nil {
		return closeWithError(err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return closeWithError(err)
	}
	return conn, hello, nil
}

func readTelemetryFrame(reader io.Reader) (telemetryEnvelope, error) {
	var length [4]byte
	if _, err := io.ReadFull(reader, length[:1]); err != nil {
		return telemetryEnvelope{}, err
	}
	if conn, ok := reader.(net.Conn); ok {
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return telemetryEnvelope{}, err
		}
	}
	if _, err := io.ReadFull(reader, length[1:]); err != nil {
		return telemetryEnvelope{}, err
	}
	size := binary.BigEndian.Uint32(length[:])
	if size == 0 || size > maxTelemetryFrame {
		return telemetryEnvelope{}, fmt.Errorf("invalid telemetry frame length %d", size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return telemetryEnvelope{}, err
	}
	var message telemetryEnvelope
	if !utf8.Valid(payload) {
		return message, fmt.Errorf("invalid telemetry UTF-8")
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		return telemetryEnvelope{}, err
	}
	if message.Type == "" || len(message.Data) == 0 || message.Data[0] != '{' {
		return message, fmt.Errorf("invalid telemetry envelope")
	}
	return message, nil
}

func writeTelemetryFrame(writer io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > 1024 {
		return fmt.Errorf("invalid telemetry command length %d", len(payload))
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	frame := append(length[:], payload...)
	for len(frame) > 0 {
		n, err := writer.Write(frame)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(frame) {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}
