// Child output forwarding with bounded line fragments and exit-time flushing.

package master

import (
	"bytes"
	"os/exec"
	"sync"
	"unicode/utf8"
)

const logFragmentSize = 16 * 1024

type processLog struct {
	mu       sync.Mutex
	master   *Master
	instance *instance
	cmd      *exec.Cmd
	pending  []byte
}

func (p *processLog) Write(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	size := len(data)
	for len(data) > 0 {
		limit := min(len(data), logFragmentSize-len(p.pending))
		part := data[:limit]
		newline := bytes.IndexByte(part, '\n')
		if newline >= 0 {
			limit = newline + 1
		}
		p.pending = append(p.pending, data[:limit]...)
		data = data[limit:]
		if newline >= 0 {
			p.emit()
		} else if len(p.pending) == logFragmentSize {
			last := len(p.pending) - 1
			for last > 0 && !utf8.RuneStart(p.pending[last]) {
				last--
			}
			if !utf8.FullRune(p.pending[last:]) {
				tail := append([]byte(nil), p.pending[last:]...)
				p.pending = p.pending[:last]
				p.emit()
				p.pending = append(p.pending, tail...)
			} else {
				p.emit()
			}
		}
	}
	return size, nil
}

func (p *processLog) emit() {
	line := bytes.TrimSuffix(p.pending, []byte("\n"))
	p.master.sendTelemetryLog(p.instance, p.cmd, string(line))
	p.pending = p.pending[:0]
}

func (p *processLog) flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pending) > 0 {
		p.emit()
	}
}
