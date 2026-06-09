package master

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func (m *Master) saveState() error {
	if err := m.saveStateToPath(m.statePath); err != nil {
		return fmt.Errorf("Master.saveState: %w", err)
	}
	return nil
}

func (m *Master) saveStateAsync() {
	go func() {
		if err := m.saveState(); err != nil {
			log.Printf("Master.saveStateAsync: save state failed: %v", err)
		}
	}()
}

func (m *Master) saveStateToPath(filePath string) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	persistentData := make(map[string]*instance)

	m.instances.Range(func(key, value any) bool {
		persistentData[key.(string)] = value.(*instance).snapshot()
		return true
	})

	if len(persistentData) == 0 {
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

func (m *Master) loadState() {
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
			m.instances.Store(id, instance)
			continue
		}

		instance.stopped = make(chan struct{})
		instance.Status = "stopped"

		m.instances.Store(id, instance)

		if instance.Restart {
			log.Printf("Master.loadState: auto-starting instance: %v [%v]", instance.URL, instance.ID)
			m.startInstance(instance)
			time.Sleep(baseDuration)
		}
	}

	log.Printf("Master.loadState: loaded %v instances from %v", len(persistentData), m.statePath)
}
