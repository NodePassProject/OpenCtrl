package master

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"time"
)

func (inst *instance) snapshot() *instance {
	if inst == nil {
		return nil
	}

	inst.mu.Lock()
	defer inst.mu.Unlock()

	return inst.snapshotLocked()
}

func (inst *instance) snapshotLocked() *instance {
	return &instance{
		ID:      inst.ID,
		Alias:   inst.Alias,
		Type:    inst.Type,
		Status:  inst.Status,
		URL:     inst.URL,
		Config:  inst.Config,
		Restart: inst.Restart,
		Meta: meta{
			Peer: inst.Meta.Peer,
			Tags: cloneTags(inst.Meta.Tags),
		},
		Mode:  0,
		Ping:  inst.Ping,
		Pool:  inst.Pool,
		TCPS:  inst.TCPS,
		UDPS:  inst.UDPS,
		TCPRX: inst.TCPRX,
		TCPTX: inst.TCPTX,
		UDPRX: inst.UDPRX,
		UDPTX: inst.UDPTX,
	}
}

func (m *Master) findInstance(id string) (*instance, bool) {
	value, exists := m.instances.Load(id)
	if !exists {
		return nil, false
	}
	return value.(*instance), true
}

func (m *Master) currentInstance(inst *instance) *instance {
	if value, exists := m.instances.Load(inst.ID); exists {
		return value.(*instance)
	}
	return inst
}

func (m *Master) startInstance(inst *instance) {
	inst = m.currentInstance(inst)
	inst.lifecycleMu.Lock()
	defer inst.lifecycleMu.Unlock()

	m.startInstanceLocked(inst)
}

func (m *Master) startInstanceLocked(inst *instance) {
	inst.mu.Lock()
	if inst.deleted || inst.Status != "stopped" {
		inst.mu.Unlock()
		return
	}
	parsedURL, urlErr := url.Parse(inst.URL)
	if urlErr != nil || (parsedURL.Scheme != "portal" && parsedURL.Scheme != "vector") || parsedURL.Scheme != inst.Type {
		log.Printf("Master.startInstanceLocked: unsupported Nowhere instance [%v]", inst.ID)
		inst.Status = "error"
		inst.runtimeFailure = false
		inst.mu.Unlock()
		m.sendSSEEvent("update", inst)
		return
	}

	inst.tcpRXBase = inst.TCPRX
	inst.tcpTXBase = inst.TCPTX
	inst.udpRXBase = inst.UDPRX
	inst.udpTXBase = inst.UDPTX
	inst.tcpRXReset, inst.tcpTXReset, inst.udpRXReset, inst.udpTXReset = 0, 0, 0, 0
	inst.lastSequence = 0
	inst.telemetryIdentity = ""
	inst.Ping, inst.Pool, inst.TCPS, inst.UDPS = 0, 0, 0, 0
	inst.lastTelemetry = time.Time{}
	inst.telemetryInterval = 0
	inst.telemetryReady = false
	inst.runtimeFailure = false
	inst.Mode = 0

	execPath := m.binPath
	if execPath == "" {
		var err error
		execPath, err = os.Executable()
		if err != nil {
			log.Printf("Master.startInstanceLocked: get path failed: %v [%v]", err, inst.ID)
			inst.Status = "error"
			inst.runtimeFailure = true
			m.instances.Store(inst.ID, inst)
			inst.mu.Unlock()
			m.sendSSEEvent("update", inst)
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmdFactory := m.instanceCmd
	if cmdFactory == nil {
		cmdFactory = func(ctx context.Context, execPath, instanceURL string) *exec.Cmd {

			return exec.CommandContext(ctx, execPath, instanceURL)
		}
	}
	cmd := cmdFactory(ctx, execPath, inst.URL)
	inst.cancelFunc = cancel
	cmd.Stdout = nil
	cmd.Stderr = nil

	log.Printf("Master.startInstanceLocked: instance starting: %v [%v]", inst.URL, inst.ID)

	if err := cmd.Start(); err != nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		if err != nil {
			log.Printf("Master.startInstanceLocked: instance error: %v [%v]", err, inst.ID)
		} else {
			log.Printf("Master.startInstanceLocked: instance start failed [%v]", inst.ID)
		}
		inst.Status = "error"
		inst.runtimeFailure = true
		inst.cmd = nil
		inst.cancelFunc = nil
		m.instances.Store(inst.ID, inst)
		inst.mu.Unlock()
		m.sendSSEEvent("update", inst)
		cancel()
		return
	}

	inst.cmd = cmd
	inst.stopped = make(chan struct{})
	inst.exited = make(chan struct{})
	inst.Status = "running"
	startedAt := time.Now()
	stoppedCh := inst.stopped
	exitedCh := inst.exited

	m.instances.Store(inst.ID, inst)
	inst.mu.Unlock()

	go m.runTelemetry(ctx, inst, cmd)
	go m.monitorInstance(inst, cmd, stoppedCh, exitedCh, startedAt)

	m.sendSSEEvent("update", inst)
}

func (m *Master) monitorInstance(inst *instance, cmd *exec.Cmd, stoppedCh <-chan struct{}, exitedCh chan struct{}, startedAt time.Time) {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stopped := false
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()
	defer close(exitedCh)

	for {
		select {
		case <-stoppedCh:

			stopped = true
			stoppedCh = nil
		case err := <-done:
			if !stopped && stoppedCh != nil {
				select {
				case <-stoppedCh:
					stopped = true
				default:
				}
			}
			if value, exists := m.instances.Load(inst.ID); exists {
				inst = value.(*instance)
				sendUpdate := false
				var cancel context.CancelFunc
				inst.mu.Lock()
				if inst.cmd == cmd && !stopped {
					cancel = inst.cancelFunc
					if err != nil {
						log.Printf("Master.monitorInstance: instance error: %v [%v]", err, inst.ID)
						inst.Status = "error"
						inst.cmd = nil
						inst.stopped = make(chan struct{})
						inst.exited = nil
						inst.cancelFunc = nil
						inst.lastTelemetry = time.Time{}
						inst.runtimeFailure = true
						inst.Ping = 0
						inst.Pool = 0
						inst.TCPS = 0
						inst.UDPS = 0
					} else {
						resetStoppedInstanceLocked(inst)
					}
					m.instances.Store(inst.ID, inst)
					sendUpdate = true
				}
				inst.mu.Unlock()
				if cancel != nil {
					cancel()
				}
				if sendUpdate {
					m.sendSSEEvent("update", inst)
				}
			}
			return
		case <-ticker.C:
			if !stopped && stoppedCh != nil {
				select {
				case <-stoppedCh:
					stopped = true
					stoppedCh = nil
				default:
				}
			}
			sendUpdate := false
			inst.mu.Lock()
			if inst.cmd == cmd && !stopped {
				freshness := 15 * time.Second
				if interval := 3 * inst.telemetryInterval; interval > freshness {
					freshness = interval
				}
				startupExpired := time.Since(startedAt) > 15*time.Second
				stale := startupExpired && !inst.telemetryReady ||
					inst.lastTelemetry.IsZero() && startupExpired ||
					!inst.lastTelemetry.IsZero() && time.Since(inst.lastTelemetry) > freshness
				if stale {
					inst.Status = "error"
					inst.Ping, inst.Pool, inst.TCPS, inst.UDPS = 0, 0, 0, 0
					m.instances.Store(inst.ID, inst)
					sendUpdate = true
				} else if !stale && inst.telemetryReady && inst.Status == "error" && !inst.runtimeFailure {
					inst.Status = "running"
					m.instances.Store(inst.ID, inst)
					sendUpdate = true
				} else if !inst.lastTelemetry.IsZero() {
					sendUpdate = true
				}
			}
			inst.mu.Unlock()
			if sendUpdate {
				m.sendSSEEvent("update", inst)
			}
		}
	}
}

func (m *Master) restartFailedInstance(inst *instance) {
	inst.lifecycleMu.Lock()
	defer inst.lifecycleMu.Unlock()
	inst.mu.Lock()
	value, exists := m.instances.Load(inst.ID)
	eligible := exists && value == inst && inst.Restart && inst.Status == "error" && inst.runtimeFailure && !inst.deleted
	inst.mu.Unlock()
	if eligible {
		m.stopInstanceLocked(inst)
		m.startInstanceLocked(inst)
	}
}

func (m *Master) stopInstance(instance *instance) {
	instance = m.currentInstance(instance)
	instance.lifecycleMu.Lock()
	defer instance.lifecycleMu.Unlock()

	m.stopInstanceLocked(instance)
}

func (m *Master) stopInstanceLocked(instance *instance) {
	instance.mu.Lock()
	if instance.Status == "stopped" {
		instance.mu.Unlock()
		return
	}

	if instance.cmd == nil || instance.cmd.Process == nil {

		resetStoppedInstanceLocked(instance)
		m.instances.Store(instance.ID, instance)
		instance.mu.Unlock()
		m.saveStateAsync()
		m.sendSSEEvent("update", instance)
		return
	}

	if instance.stopped != nil {
		select {
		case <-instance.stopped:
		default:
			close(instance.stopped)
		}
	}

	process := instance.cmd.Process
	cancelFunc := instance.cancelFunc
	exitedCh := instance.exited
	instance.mu.Unlock()

	if runtime.GOOS == "windows" {
		process.Signal(os.Interrupt)
	} else {
		process.Signal(syscall.SIGTERM)
	}

	if exitedCh != nil {
		select {
		case <-exitedCh:
			log.Printf("Master.stopInstanceLocked: instance stopped [%v]", instance.ID)
		case <-time.After(gracefulTimeout):
			if cancelFunc != nil {

				cancelFunc()
				cancelFunc = nil
			}
			process.Kill()
			<-exitedCh
			log.Printf("Master.stopInstanceLocked: instance force killed [%v]", instance.ID)
		}
	} else {
		process.Kill()
		log.Printf("Master.stopInstanceLocked: instance force killed without exit notification [%v]", instance.ID)
	}
	if cancelFunc != nil {
		cancelFunc()
	}

	instance.mu.Lock()
	resetStoppedInstanceLocked(instance)
	m.instances.Store(instance.ID, instance)
	instance.mu.Unlock()

	m.saveStateAsync()

	m.sendSSEEvent("update", instance)
}

func (m *Master) restartInstance(instance *instance) {
	instance = m.currentInstance(instance)
	instance.lifecycleMu.Lock()
	defer instance.lifecycleMu.Unlock()

	m.stopInstanceLocked(instance)
	m.startInstanceLocked(instance)
}

func (m *Master) normalizeInstanceURL(rawURL string) (string, string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("Master.normalizeInstanceURL: invalid URL format: %w", err)
	}
	if parsedURL.Scheme != "portal" && parsedURL.Scheme != "vector" {
		return "", "", fmt.Errorf("Master.normalizeInstanceURL: scheme must be portal or vector")
	}
	return parsedURL.Scheme, parsedURL.String(), nil
}
