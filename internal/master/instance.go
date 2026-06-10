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

// snapshot returns a detached copy of the API-visible instance state.
//
// The master stores mutable *instance pointers in sync.Map so lifecycle
// goroutines and request handlers can coordinate on the same record. API
// responses and SSE payloads must not expose that mutable pointer directly;
// this method copies only the JSON-visible fields and deep-copies tags.
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

// findInstance loads the current instance pointer for an ID.
//
// The returned pointer is still mutable shared state. Callers that inspect or
// modify fields must either call snapshot or hold instance.mu.
func (m *Master) findInstance(id string) (*instance, bool) {
	value, exists := m.instances.Load(id)
	if !exists {
		return nil, false
	}
	return value.(*instance), true
}

// currentInstance refreshes a possibly stale pointer from the registry.
//
// Handlers often receive an instance pointer before acquiring lifecycleMu. A
// concurrent PUT/DELETE can replace or mark records during that window, so
// lifecycle operations refresh from the registry before locking.
func (m *Master) currentInstance(inst *instance) *instance {
	if value, exists := m.instances.Load(inst.ID); exists {
		return value.(*instance)
	}
	return inst
}

// startInstance serializes lifecycle transitions before launching a child.
//
// This wrapper owns lifecycleMu. Code that already holds lifecycleMu, such as
// restart and PUT replacement, must call startInstanceLocked to avoid deadlock.
func (m *Master) startInstance(inst *instance) {
	inst = m.currentInstance(inst)
	inst.lifecycleMu.Lock()
	defer inst.lifecycleMu.Unlock()

	m.startInstanceLocked(inst)
}

// startInstanceLocked starts a child process for an already lifecycle-locked
// instance.
//
// Lock order is lifecycleMu first, then instance.mu. The function keeps
// instance.mu while preparing the command so no request handler can observe a
// half-started state. It releases the lock before the monitor goroutine begins
// reporting status.
func (m *Master) startInstanceLocked(inst *instance) {
	inst.mu.Lock()
	if inst.deleted || inst.Status != "stopped" {
		inst.mu.Unlock()
		return
	}

	// Preserve cumulative traffic counters across process restarts. Child
	// checkpoint counters usually start at zero for each new process, so the
	// current totals become the base added to future checkpoint deltas.
	inst.tcpRXBase = inst.TCPRX
	inst.tcpTXBase = inst.TCPTX
	inst.udpRXBase = inst.UDPRX
	inst.udpTXBase = inst.UDPTX
	inst.lastCheckpoint = time.Time{}

	// The master-level bin parameter selects the managed child executable. When
	// omitted, the current binary is reused so OpenCtrl can manage built-in cores
	// without a separate launcher.
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
			// Pass the child URL as a single opaque argument. The controller must
			// not split, normalize, or interpret protocol-specific URL details.
			return exec.CommandContext(ctx, execPath, instanceURL)
		}
	}
	cmd := cmdFactory(ctx, execPath, inst.URL)
	inst.cancelFunc = cancel

	// Child output is the telemetry bus. CHECK_POINT lines update metrics;
	// ordinary lines are forwarded to stdout and SSE log events.
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

	// Publish the running state only after cmd.Start succeeds and produces a
	// valid process handle.
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

// monitorInstance reconciles process exit, operator-initiated stops, and stale
// checkpoint streams back into the registry.
//
// stoppedCh is captured at launch time. If an instance is restarted, the new
// process receives a new channel, which prevents this monitor from reacting to
// later lifecycle events for a different process.
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
			// A closed stoppedCh means the master initiated the stop. The later
			// cmd.Wait result should not turn that intentional exit into an error.
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
			// A running child that previously emitted checkpoints but then goes
			// quiet is treated as degraded. Children that never emitted a
			// checkpoint are left alone because some protocol binaries may take
			// time to establish their first report.
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

// stopInstance serializes lifecycle transitions before stopping a child.
//
// This wrapper owns lifecycleMu. Code that already holds lifecycleMu must call
// stopInstanceLocked to preserve the package's lock order.
func (m *Master) stopInstance(instance *instance) {
	instance = m.currentInstance(instance)
	instance.lifecycleMu.Lock()
	defer instance.lifecycleMu.Unlock()

	m.stopInstanceLocked(instance)
}

// stopInstanceLocked asks the child to exit gracefully before falling back to a
// force kill. The caller must hold instance.lifecycleMu.
//
// The method closes instance.stopped before signalling the process. That marks
// the later cmd.Wait result as intentional, so monitorInstance will not publish
// an error state for a normal operator stop.
func (m *Master) stopInstanceLocked(instance *instance) {
	instance.mu.Lock()
	if instance.Status == "stopped" {
		instance.mu.Unlock()
		return
	}

	if instance.cmd == nil || instance.cmd.Process == nil {
		// Recover inconsistent state where Status is not stopped but no process
		// handle is available. This keeps API state self-healing.
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
				// Cancel CommandContext before Kill so os/exec can release any
				// context-linked resources even if the process ignores SIGTERM.
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

// restartInstance performs a stop/start cycle under a single lifecycle lock.
//
// Holding lifecycleMu across both operations prevents an interleaving PATCH,
// PUT, or DELETE from observing the transient stopped state as stable.
func (m *Master) restartInstance(instance *instance) {
	instance = m.currentInstance(instance)
	instance.lifecycleMu.Lock()
	defer instance.lifecycleMu.Unlock()

	m.stopInstanceLocked(instance)
	m.startInstanceLocked(instance)
}

// normalizeInstanceURL validates controller-level URL requirements while
// leaving protocol-specific parsing to the child binary.
//
// Returning parsedURL.String keeps Go's URL normalization limited to syntax
// validation. The master only rejects empty schemes and recursive master URLs.
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
