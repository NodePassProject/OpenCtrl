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
		Mode:  inst.Mode,
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

	inst.tcpRXBase = inst.TCPRX
	inst.tcpTXBase = inst.TCPTX
	inst.udpRXBase = inst.UDPRX
	inst.udpTXBase = inst.UDPTX
	inst.lastCheckpoint = time.Time{}

	execPath := m.binPath
	if execPath == "" {
		var err error
		execPath, err = os.Executable()
		if err != nil {
			log.Printf("Master.startInstanceLocked: get path failed: %v [%v]", err, inst.ID)
			inst.Status = "error"
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

	writer := &instanceLogWriter{
		instanceID: inst.ID,
		instance:   inst,
		target:     os.Stdout,
		master:     m,
		checkpoint: checkpointRegexp,
	}
	cmd.Stdout, cmd.Stderr = writer, writer

	log.Printf("Master.startInstanceLocked: instance starting: %v [%v]", inst.URL, inst.ID)

	if err := cmd.Start(); err != nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		if err != nil {
			log.Printf("Master.startInstanceLocked: instance error: %v [%v]", err, inst.ID)
		} else {
			log.Printf("Master.startInstanceLocked: instance start failed [%v]", inst.ID)
		}
		inst.Status = "error"
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

	go m.monitorInstance(inst, cmd, stoppedCh, exitedCh)

	m.sendSSEEvent("update", inst)
}

func (m *Master) monitorInstance(inst *instance, cmd *exec.Cmd, stoppedCh <-chan struct{}, exitedCh chan struct{}) {
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
						inst.lastCheckpoint = time.Time{}
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
			sendUpdate := false
			inst.mu.Lock()
			if inst.cmd == cmd && inst.Status == "running" && !stopped &&
				!inst.lastCheckpoint.IsZero() && time.Since(inst.lastCheckpoint) > 3*reportInterval {
				inst.Status = "error"
				m.instances.Store(inst.ID, inst)
				sendUpdate = true
			}
			inst.mu.Unlock()
			if sendUpdate {
				m.sendSSEEvent("update", inst)
			}
		}
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
	if parsedURL.Scheme == "" {
		return "", "", fmt.Errorf("Master.normalizeInstanceURL: missing URL scheme")
	}
	if parsedURL.Scheme == "master" {
		return "", "", fmt.Errorf("Master.normalizeInstanceURL: master cannot manage master URL")
	}

	return parsedURL.Scheme, parsedURL.String(), nil
}
