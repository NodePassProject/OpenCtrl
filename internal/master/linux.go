//go:build linux

package master

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func linuxProcessIncarnation(pid uint32) uint64 {
	payload, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	text := string(payload)
	close := strings.LastIndexByte(text, ')')
	if close < 0 {
		return 0
	}
	fields := strings.Fields(text[close+2:])
	if len(fields) <= 19 {
		return 0
	}
	value, _ := strconv.ParseUint(fields[19], 10, 64)
	return value
}

func validateProcessIncarnation(instance discoveredInstance) bool {
	value := linuxProcessIncarnation(instance.PID)
	return value != 0 && value == instance.Incarnation
}

func telemetryNamespace() (string, error) {
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	pidNamespace, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return "", err
	}
	userNamespace, err := os.Readlink("/proc/self/ns/user")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s;%s;%s", strings.TrimSpace(string(bootID)), pidNamespace, userNamespace), nil
}

func validateTelemetryPeer(conn net.Conn, pid uint32) error {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("unexpected telemetry connection type")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return err
	}
	var cred *syscall.Ucred
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		cred, socketErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return err
	}
	if socketErr != nil {
		return socketErr
	}
	if cred.Uid != uint32(os.Geteuid()) || cred.Pid != int32(pid) {
		return fmt.Errorf("telemetry server identity mismatch")
	}
	return nil
}
