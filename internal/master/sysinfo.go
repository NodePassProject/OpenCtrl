package master

import (
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// diskPartitionRegexp filters partition rows so disk counters represent whole
// block devices instead of double-counted child partitions.
//
// Linux diskstats reports both disks and partitions. Counting both inflates
// totals, so this expression matches common partition naming schemes while
// leaving whole devices to be accumulated.
var diskPartitionRegexp = regexp.MustCompile(`^(?:sd[a-z]+|vd[a-z]+|xvd[a-z]+|hd[a-z]+)\d+$|^(?:nvme\d+n\d+|mmcblk\d+)p\d+$`)

// masterInfo returns the stable info payload consumed by OpenCtrl clients.
//
// The payload intentionally uses map[string]any to preserve the historical
// response shape and field names. Non-Linux platforms still receive every field
// with zero or sentinel values so UI code can avoid platform branching.
func (m *Master) masterInfo() map[string]any {
	m.mu.RLock()
	alias := m.alias
	m.mu.RUnlock()

	info := map[string]any{
		"mid":        m.masterID,
		"alias":      alias,
		"os":         runtime.GOOS,
		"arch":       runtime.GOARCH,
		"noc":        runtime.NumCPU(),
		"cpu":        -1,
		"mem_total":  uint64(0),
		"mem_used":   uint64(0),
		"swap_total": uint64(0),
		"swap_used":  uint64(0),
		"netrx":      uint64(0),
		"nettx":      uint64(0),
		"diskr":      uint64(0),
		"diskw":      uint64(0),
		"sysup":      uint64(0),
		"ver":        m.version,
		"name":       m.hostname,
		"uptime":     uint64(time.Since(m.startTime).Seconds()),
		// The log field remains for client compatibility after removal of the
		// master-side startup log parameter.
		"log": "default",
		"tls": m.tlsMode.String(),
		"crt": m.crtPath,
		"key": m.keyPath,
	}

	if runtime.GOOS == "linux" {
		sysInfo := linuxSysInfo()
		info["cpu"] = sysInfo.CPU
		info["mem_total"] = sysInfo.MemTotal
		info["mem_used"] = sysInfo.MemUsed
		info["swap_total"] = sysInfo.SwapTotal
		info["swap_used"] = sysInfo.SwapUsed
		info["netrx"] = sysInfo.NetRX
		info["nettx"] = sysInfo.NetTX
		info["diskr"] = sysInfo.DiskR
		info["diskw"] = sysInfo.DiskW
		info["sysup"] = sysInfo.SysUp
	}

	return info
}

// linuxSysInfo collects lightweight Linux host metrics directly from /proc.
//
// Unsupported platforms return the same zero-value schema so API clients do
// not need platform-specific response handling.
//
// The collector avoids third-party dependencies and treats missing or malformed
// proc files as partial data instead of endpoint failure. That keeps /info
// usable in containers and restricted environments.
func linuxSysInfo() systemInfo {
	info := systemInfo{
		CPU:       -1,
		MemTotal:  0,
		MemUsed:   0,
		SwapTotal: 0,
		SwapUsed:  0,
		NetRX:     0,
		NetTX:     0,
		DiskR:     0,
		DiskW:     0,
		SysUp:     0,
	}

	if runtime.GOOS != "linux" {
		return info
	}

	readStat := func() (idle, total uint64) {
		data, err := os.ReadFile("/proc/stat")
		if err != nil {
			return
		}
		for line := range strings.SplitSeq(string(data), "\n") {
			if strings.HasPrefix(line, "cpu ") {
				fields := strings.Fields(line)
				for i, v := range fields[1:] {
					val, _ := strconv.ParseUint(v, 10, 64)
					total += val
					if i == 3 {
						idle = val
					}
				}
				break
			}
		}
		return
	}
	// CPU usage is sampled over a short interval. /proc/stat exposes cumulative
	// jiffies, so two reads are required to compute an instantaneous percentage.
	idle1, total1 := readStat()
	time.Sleep(baseDuration)
	idle2, total2 := readStat()
	if deltaIdle, deltaTotal := idle2-idle1, total2-total1; deltaTotal > 0 {
		info.CPU = min(int((deltaTotal-deltaIdle)*100/deltaTotal), 100)
	}

	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		var memTotal, memAvailable, swapTotal, swapFree uint64
		for line := range strings.SplitSeq(string(data), "\n") {
			if fields := strings.Fields(line); len(fields) >= 2 {
				if val, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					val *= 1024
					switch fields[0] {
					case "MemTotal:":
						memTotal = val
					case "MemAvailable:":
						memAvailable = val
					case "SwapTotal:":
						swapTotal = val
					case "SwapFree:":
						swapFree = val
					}
				}
			}
		}
		info.MemTotal = memTotal
		info.MemUsed = memTotal - memAvailable
		info.SwapTotal = swapTotal
		info.SwapUsed = swapTotal - swapFree
	}

	if data, err := os.ReadFile("/proc/net/dev"); err == nil {
		for _, line := range strings.Split(string(data), "\n")[2:] {
			if fields := strings.Fields(line); len(fields) >= 10 {
				ifname := strings.TrimSuffix(fields[0], ":")
				// Skip loopback, common container veth devices, and bridge
				// interfaces so host traffic is not dominated by internal
				// plumbing.
				if strings.HasPrefix(ifname, "lo") || strings.HasPrefix(ifname, "veth") ||
					strings.HasPrefix(ifname, "docker") || strings.HasPrefix(ifname, "podman") ||
					strings.HasPrefix(ifname, "br-") || strings.HasPrefix(ifname, "virbr") {
					continue
				}
				if val, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					info.NetRX += val
				}
				if val, err := strconv.ParseUint(fields[9], 10, 64); err == nil {
					info.NetTX += val
				}
			}
		}
	}

	if data, err := os.ReadFile("/proc/diskstats"); err == nil {
		for line := range strings.SplitSeq(string(data), "\n") {
			if fields := strings.Fields(line); len(fields) >= 14 {
				deviceName := fields[2]
				// Skip virtual, aggregate, and partition rows. Linux reports
				// sectors for read/write counters, and the conventional sector
				// size for these fields is 512 bytes.
				if strings.HasPrefix(deviceName, "loop") || strings.HasPrefix(deviceName, "ram") ||
					strings.HasPrefix(deviceName, "dm-") || strings.HasPrefix(deviceName, "md") ||
					diskPartitionRegexp.MatchString(deviceName) {
					continue
				}
				if val, err := strconv.ParseUint(fields[5], 10, 64); err == nil {
					info.DiskR += val * 512
				}
				if val, err := strconv.ParseUint(fields[9], 10, 64); err == nil {
					info.DiskW += val * 512
				}
			}
		}
	}

	if data, err := os.ReadFile("/proc/uptime"); err == nil {
		if fields := strings.Fields(string(data)); len(fields) > 0 {
			if uptime, err := strconv.ParseFloat(fields[0], 64); err == nil {
				info.SysUp = uint64(uptime)
			}
		}
	}

	return info
}
