package master

import (
	"fmt"
	"log"
	"time"
)

// startPeriodicTasks performs low-frequency maintenance: state backups and
// automatic restarts for recoverable error instances.
//
// This loop intentionally runs on reloadInterval instead of reportInterval.
// Checkpoint freshness is handled by monitorInstance; this maintenance path is
// for durable backups and coarse-grained recovery, not fast health checks.
func (m *Master) startPeriodicTasks() {
	ticker := time.NewTicker(reloadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Keep a simple sibling backup of the current gob file. The backup
			// uses the same atomic write path as the primary state file.
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
					needsRestart := instance.Restart && instance.Status == "error" && !instance.deleted
					instance.mu.Unlock()
					if needsRestart {
						errorInstances = append(errorInstances, instance)
					}
				}
				return true
			})

			// Restart outside sync.Map iteration and outside instance.mu so
			// lifecycle locks are acquired in the normal order.
			for _, instance := range errorInstances {
				m.restartInstance(instance)
			}
		case <-m.periodicDone:
			return
		}
	}
}
