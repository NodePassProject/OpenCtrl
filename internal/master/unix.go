//go:build unix && !linux

package master

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"syscall"
	"unsafe"
)

func telemetryNamespace() (string, error) { return "", nil }

func validateProcessIncarnation(_ discoveredInstance) bool { return true }

func validateTelemetryPeer(conn net.Conn, _ uint32) error {
	stream, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("unexpected telemetry connection type")
	}
	raw, err := stream.SyscallConn()
	if err != nil {
		return err
	}
	var credentials [256]byte
	length := uint32(len(credentials))
	var callErr syscall.Errno
	if err := raw.Control(func(fd uintptr) {
		_, _, callErr = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, 0, 1,
			uintptr(unsafe.Pointer(&credentials[0])), uintptr(unsafe.Pointer(&length)), 0)
	}); err != nil {
		return err
	}
	if callErr != 0 {
		return callErr
	}
	if length < 8 || binary.NativeEndian.Uint32(credentials[:4]) != 0 ||
		binary.NativeEndian.Uint32(credentials[4:8]) != uint32(os.Geteuid()) {
		return fmt.Errorf("telemetry server user mismatch")
	}
	return nil
}
