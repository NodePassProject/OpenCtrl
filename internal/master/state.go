package master

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// saveState writes the current instance registry to the master state file.
//
// It wraps saveStateToPath so callers get a stable method for the canonical
// state location while periodic maintenance can reuse the same implementation
// for backups.
func (m *Master) saveState() error {
	if err := m.saveStateToPath(m.statePath); err != nil {
		return fmt.Errorf("Master.saveState: %w", err)
	}
	return nil
}

// saveStateAsync persists state without blocking request or lifecycle paths.
//
// State saves are best effort outside shutdown. The in-memory registry remains
// authoritative for the running process, and failures are logged for operators.
func (m *Master) saveStateAsync() {
	go func() {
		if err := m.saveState(); err != nil {
			log.Printf("Master.saveStateAsync: save state failed: %v", err)
		}
	}()
}

// saveStateToPath snapshots all instances and atomically replaces the target
// gob file. An empty registry removes the target so stale state is not loaded
// on the next start.
//
// The method snapshots under each instance lock and serializes writes with
// stateMu. The on-disk replacement uses a temporary file in the same directory
// followed by os.Rename, which is the simplest portable atomic replace pattern
// for this use case.
func (m *Master) saveStateToPath(filePath string) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	// Store snapshots rather than live pointers so gob encoding cannot observe
	// concurrent mutations while walking the object graph.
	persistentData := make(map[string]*instance)

	m.instances.Range(func(key, value any) bool {
		persistentData[key.(string)] = value.(*instance).snapshot()
		return true
	})

	if len(persistentData) == 0 {
		// An empty registry means there is nothing to restore. Removing the file
		// prevents stale state from reappearing after the next restart.
		if _, err := os.Stat(filePath); err == nil {
			if err := os.Remove(filePath); err != nil {
				return fmt.Errorf("Master.saveStateToPath: remove empty state failed: %w", err)
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("Master.saveStateToPath: stat empty state failed: %w", err)
		}
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("Master.saveStateToPath: mkdirAll failed: %w", err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(filePath), "gob-*.tmp")
	if err != nil {
		return fmt.Errorf("Master.saveStateToPath: create temp failed: %w", err)
	}
	tempPath := tempFile.Name()

	removeTemp := func() {
		// Cleanup is intentionally best effort; a later loadState pass removes
		// orphaned gob-*.tmp files in the state directory.
		if _, err := os.Stat(tempPath); err == nil {
			os.Remove(tempPath)
		}
	}

	encoder := gob.NewEncoder(tempFile)
	if err := encoder.Encode(persistentData); err != nil {
		tempFile.Close()
		removeTemp()
		return fmt.Errorf("Master.saveStateToPath: encode failed: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		removeTemp()
		return fmt.Errorf("Master.saveStateToPath: close temp file failed: %w", err)
	}

	if err := os.Rename(tempPath, filePath); err != nil {
		removeTemp()
		return fmt.Errorf("Master.saveStateToPath: rename temp file failed: %w", err)
	}

	return nil
}

// loadState restores persisted instances and restarts those configured for
// automatic recovery.
//
// Runtime-only fields are reconstructed after decoding because gob only stores
// exported fields. All restored child instances begin as stopped, then opt into
// auto-start through Restart.
func (m *Master) loadState() {
	// Previous crashes may leave temp files next to the real state. They are not
	// valid restore candidates and can be removed before reading.
	if tmpFiles, _ := filepath.Glob(filepath.Join(filepath.Dir(m.statePath), "gob-*.tmp")); tmpFiles != nil {
		for _, f := range tmpFiles {
			os.Remove(f)
		}
	}

	if _, err := os.Stat(m.statePath); os.IsNotExist(err) {
		return
	}

	file, err := os.Open(m.statePath)
	if err != nil {
		log.Printf("Master.loadState: open file failed: %v", err)
		return
	}
	defer file.Close()

	var persistentData map[string]*instance
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&persistentData); err != nil {
		log.Printf("Master.loadState: decode file failed: %v", err)
		return
	}

	for id, instance := range persistentData {
		if id == apiKeyID {
			// The reserved API-key record is not a child process and therefore
			// has no runtime process channels to reconstruct.
			m.instances.Store(id, instance)
			continue
		}

		// Never trust a persisted running/error state after process restart. The
		// child process is gone, so the in-memory lifecycle starts clean.
		instance.stopped = make(chan struct{})
		instance.Status = "stopped"

		m.instances.Store(id, instance)

		if instance.Restart {
			log.Printf("Master.loadState: auto-starting instance: %v [%v]", instance.URL, instance.ID)
			m.startInstance(instance)
			// Small pacing keeps a large restored registry from launching every
			// child at exactly the same instant.
			time.Sleep(baseDuration)
		}
	}

	log.Printf("Master.loadState: loaded %v instances from %v", len(persistentData), m.statePath)
}
