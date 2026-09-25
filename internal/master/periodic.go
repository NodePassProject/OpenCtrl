package master

import (
	"fmt"
	"log"
	"time"
)

func (m *Master) startPeriodicTasks() {
	ticker := time.NewTicker(reloadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			backupPath := fmt.Sprintf("%s.backup", m.statePath)
			if err := m.saveStateToPath(backupPath); err != nil {
				log.Printf("Master.startPeriodicTasks: backup state failed: %v", err)
			} else {
				log.Printf("Master.startPeriodicTasks: state backup saved: %v", backupPath)
			}

			var errorInstances []*instance
			m.instances.Range(func(key, value any) bool {
				if id := key.(string); id != apiKeyID {
					instance := value.(*instance)
					instance.mu.Lock()
					needsRestart := instance.Restart && instance.Status == "error" && instance.runtimeFailure && !instance.deleted
					instance.mu.Unlock()
					if needsRestart {
						errorInstances = append(errorInstances, instance)
					}
				}
				return true
			})

			for _, instance := range errorInstances {
				m.restartFailedInstance(instance)
			}
		case <-m.periodicDone:
			return
		}
	}
}
