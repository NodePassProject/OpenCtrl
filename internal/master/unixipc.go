//go:build unix

package master

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

func telemetryDirectory() (string, error) {
	return filepath.Join(os.TempDir(), fmt.Sprintf("nowhere-telemetry-%d", os.Geteuid())), nil
}

func telemetryTransport() string { return "unix_socket" }

func telemetryEndpoint(id string) string {
	directory, _ := telemetryDirectory()
	return filepath.Join(directory, id[:16]+".sock")
}

func validateTelemetryDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm() != 0700 || stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("unsafe telemetry directory")
	}
	return nil
}

func readTelemetryRegistry(path string) ([]byte, error) {
	if err := validateTelemetryDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || stat.Uid != uint32(os.Geteuid()) || info.Size() > maxRegistrySize {
		return nil, fmt.Errorf("unsafe telemetry registry")
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxRegistrySize+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > maxRegistrySize {
		return nil, fmt.Errorf("oversized telemetry registry")
	}
	return payload, nil
}

func dialTelemetry(ctx context.Context, endpoint string, pid uint32) (net.Conn, error) {
	directory, _ := telemetryDirectory()
	if filepath.Dir(endpoint) != directory || filepath.Ext(endpoint) != ".sock" {
		return nil, fmt.Errorf("invalid telemetry endpoint")
	}
	info, err := os.Lstat(endpoint)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0600 || stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("unsafe telemetry socket")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", endpoint)
	if err != nil {
		return nil, err
	}
	if err := validateTelemetryPeer(conn, pid); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func validateTelemetryProcess(instance discoveredInstance) bool {
	if instance.PID == 0 || instance.UID != uint32(os.Geteuid()) {
		return false
	}
	process, err := os.FindProcess(int(instance.PID))
	if err != nil || process.Signal(syscall.Signal(0)) != nil {
		return false
	}
	return validateProcessIncarnation(instance)
}
