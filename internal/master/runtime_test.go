// Process-owned status, raw child output, cumulative statistics and SSE bounds.

package master

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testSubscriber() *sseSubscriber {
	return &sseSubscriber{wake: make(chan struct{}, 1), done: make(chan struct{}), states: make(map[string]*instanceEvent)}
}

func TestChildOutputHelper(t *testing.T) {
	if os.Getenv("OPENCTRL_TEST_CHILD") != "1" {
		return
	}
	fmt.Println("2026-10-11T00:00:00.000Z starting")
	fmt.Fprintln(os.Stderr, "2026-10-11T00:00:00.001Z refused: raw inner error")
	fmt.Println(strings.Repeat("x", 70000))
	fmt.Print("2026-10-11T00:00:00.002Z shutdown complete")
	if os.Getenv("OPENCTRL_TEST_FAIL") == "1" {
		os.Exit(3)
	}
	os.Exit(0)
}

func TestManagedOutputAndExitStatus(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			m := &Master{statePath: filepath.Join(t.TempDir(), "state.gob")}
			subscriber := testSubscriber()
			m.subscribers.Store(subscriber, subscriber)
			m.instanceCmd = func(ctx context.Context, _, _ string) *exec.Cmd {
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestChildOutputHelper$")
				cmd.Env = append(os.Environ(), "OPENCTRL_TEST_CHILD=1", fmt.Sprintf("OPENCTRL_TEST_FAIL=%d", map[bool]int{true: 1, false: 0}[fail]))
				return cmd
			}
			inst := &instance{ID: "child", Type: "egress", URL: "egress://0123456789abcdef0123456789abcdef@localhost:2000", Status: "stopped"}
			m.instances.Store(inst.ID, inst)
			m.startInstance(inst)
			inst.mu.Lock()
			exited := inst.exited
			inst.mu.Unlock()
			if exited == nil {
				t.Fatal("child did not start")
			}
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Fatal("child output blocked Wait")
			}
			want := "stopped"
			if fail {
				want = "error"
			}
			if got := inst.snapshot().Status; got != want {
				t.Fatalf("status %s, want %s", got, want)
			}
			var logs strings.Builder
			finalSeen := false
			for event := subscriber.next(); event != nil; event = subscriber.next() {
				if event.Type == "log" {
					if finalSeen {
						t.Fatal("log arrived after final state")
					}
					logs.WriteString(event.Logs)
					logs.WriteByte('\n')
				}
				if event.Type == "update" && event.Instance.Status == want {
					finalSeen = true
				}
			}
			for _, line := range []string{"starting", "refused: raw inner error", strings.Repeat("x", 70000), "shutdown complete"} {
				text := strings.ReplaceAll(logs.String(), "\n", "")
				if !strings.Contains(text, line) {
					t.Fatalf("missing output %s", line[:min(30, len(line))])
				}
			}
			if !finalSeen {
				t.Fatal("final state missing")
			}
		})
	}
}

func TestStatisticsAreIdempotentAndDoNotOwnStatus(t *testing.T) {
	m := &Master{}
	cmd := &exec.Cmd{}
	inst := &instance{ID: "x", Status: "running", cmd: cmd, stopped: make(chan struct{}), tcpRXBase: 100, udpTXBase: 200}
	m.instances.Store(inst.ID, inst)
	sample := telemetrySnapshot{TCPUpload: 30, UDPDownload: 40, TCPActive: 3, UDPActive: 2}
	m.applyTelemetrySnapshot(inst, cmd, sample)
	m.applyTelemetrySnapshot(inst, cmd, sample)
	if inst.TCPRX != 130 || inst.UDPTX != 240 {
		t.Fatal("reconnect duplicated counters")
	}
	m.clearActiveTelemetry(inst, cmd)
	if inst.Status != "running" || inst.TCPS != 0 || inst.UDPS != 0 || inst.TCPRX != 130 {
		t.Fatal("telemetry loss changed process status or total")
	}
	old := cmd
	inst.cmd = &exec.Cmd{}
	m.applyTelemetrySnapshot(inst, old, telemetrySnapshot{TCPUpload: 999})
	if inst.TCPRX != 130 {
		t.Fatal("old process overwrote new owner")
	}
	inst.tcpRXBase = inst.TCPRX
	inst.tcpRXReset = 0
	m.applyTelemetrySnapshot(inst, inst.cmd, telemetrySnapshot{TCPUpload: 5})
	if inst.TCPRX != 135 {
		t.Fatal("restart lost cumulative bytes")
	}
}

func TestStoppedRequestStillReceivesCurrentProcessLogs(t *testing.T) {
	m := &Master{}
	subscriber := testSubscriber()
	m.subscribers.Store(subscriber, subscriber)
	cmd := &exec.Cmd{}
	stopped := make(chan struct{})
	close(stopped)
	inst := &instance{ID: "x", Status: "running", cmd: cmd, stopped: stopped}
	m.sendTelemetryLog(inst, cmd, "shutdown complete")
	event := subscriber.next()
	if event == nil || event.Logs != "shutdown complete" {
		t.Fatal("shutdown log suppressed")
	}
	inst.cmd = &exec.Cmd{}
	m.sendTelemetryLog(inst, cmd, "stale")
	if subscriber.next() != nil {
		t.Fatal("old process log leaked")
	}
}

func TestSlowSSEClosesAndDoesNotBlockPublisher(t *testing.T) {
	subscriber := testSubscriber()
	for range 11 {
		subscriber.enqueue(&instanceEvent{Type: "log", Logs: "raw"})
	}
	select {
	case <-subscriber.done:
	default:
		t.Fatal("full log queue did not close")
	}
	if subscriber.next() != nil {
		t.Fatal("closed consumer retained delivery")
	}
}

func TestIncompleteStatisticsTimesOutWithoutChangingProcessStatus(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	m := &Master{}
	cmd := &exec.Cmd{}
	inst := &instance{ID: "x", Status: "running", cmd: cmd, stopped: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- m.consumeTelemetry(context.Background(), inst, cmd, left) }()
	if _, err := right.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("partial frame succeeded")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("partial frame did not time out")
	}
	if inst.Status != "running" {
		t.Fatal("timeout changed status")
	}
}

func TestSnapshotFrameUsesNW3Fields(t *testing.T) {
	value := telemetrySnapshot{TCPUpload: 42, TCPActive: 1}
	payload, _ := json.Marshal(telemetryEnvelope{Type: "snapshot", Data: mustJSON(t, value)})
	var frame strings.Builder
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(payload)))
	frame.Write(length)
	frame.Write(payload)
	decoded, err := readTelemetryFrame(strings.NewReader(frame.String()))
	if err != nil {
		t.Fatal(err)
	}
	var sample telemetrySnapshot
	if json.Unmarshal(decoded.Data, &sample) != nil || sample.TCPUpload != 42 || sample.TCPActive != 1 {
		t.Fatal("wrong snapshot")
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestLongUnicodeOutputPreservesTextAcrossFragments(t *testing.T) {
	m := &Master{}
	cmd := &exec.Cmd{}
	inst := &instance{ID: "unicode", cmd: cmd}
	subscriber := testSubscriber()
	m.subscribers.Store(subscriber, subscriber)
	output := &processLog{master: m, instance: inst, cmd: cmd}
	text := strings.Repeat("诊断", 12000)
	for start := 0; start < len(text); start += 997 {
		part := text[start:min(start+997, len(text))]
		if _, err := output.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	output.flush()
	var actual strings.Builder
	for event := subscriber.next(); event != nil; event = subscriber.next() {
		actual.WriteString(event.Logs)
	}
	if actual.String() != text {
		t.Fatal("UTF-8 text changed at fragment boundary")
	}
}
