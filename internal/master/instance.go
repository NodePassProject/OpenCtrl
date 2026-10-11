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
	if urlErr != nil || !supportedRole(parsedURL.Scheme) || parsedURL.Scheme != inst.Type {
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
	inst.telemetryIdentity = ""
	inst.Ping, inst.Pool, inst.TCPS, inst.UDPS = 0, 0, 0, 0
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
	stdout := &processLog{master: m, instance: inst, cmd: cmd}
	stderr := &processLog{master: m, instance: inst, cmd: cmd}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.WaitDelay = 2 * time.Second
	inst.cmd = cmd

	log.Printf("Master.startInstanceLocked: instance starting [%v]", inst.ID)

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
	stoppedCh := inst.stopped
	exitedCh := inst.exited

	m.instances.Store(inst.ID, inst)
	inst.mu.Unlock()

	go m.runTelemetry(ctx, inst, cmd)
	go m.monitorInstance(inst, cmd, stoppedCh, exitedCh, stdout, stderr)

	m.sendSSEEvent("update", inst)
}

func (m *Master) monitorInstance(inst *instance, cmd *exec.Cmd, stoppedCh <-chan struct{}, exitedCh chan struct{}, stdout, stderr *processLog) {
	defer close(exitedCh)
	err := cmd.Wait()
	stdout.flush()
	stderr.flush()
	inst.mu.Lock()
	stopped := false
	select {
	case <-stoppedCh:
		stopped = true
	default:
	}
	current := inst.cmd == cmd && !inst.deleted
	var cancel context.CancelFunc
	if current && !stopped {
		cancel = inst.cancelFunc
		if err != nil {
			inst.Status = "error"
			inst.cmd = nil
			inst.exited = nil
			inst.cancelFunc = nil
			inst.runtimeFailure = true
			inst.Ping, inst.Pool, inst.TCPS, inst.UDPS = 0, 0, 0, 0
		} else {
			resetStoppedInstanceLocked(inst)
		}
	}
	inst.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if current && !stopped {
		if err := m.saveState(); err != nil {
			log.Printf("save exited instance failed: %v", err)
		}
		m.sendSSEEvent("update", inst)
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
			select {
			case <-exitedCh:
			case <-time.After(3 * time.Second):
			}
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
	if !supportedRole(parsedURL.Scheme) {
		return "", "", fmt.Errorf("Master.normalizeInstanceURL: scheme must be portal, vector or egress")
	}
	return parsedURL.Scheme, parsedURL.String(), nil
}

func supportedRole(role string) bool { return role == "portal" || role == "vector" || role == "egress" }
