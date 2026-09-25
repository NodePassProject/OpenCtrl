//go:build windows

package master

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const processQueryLimitedInformation = 0x1000

var getNamedPipeServerProcessID = syscall.NewLazyDLL("kernel32.dll").NewProc("GetNamedPipeServerProcessId")

var (
	securityAPI          = syscall.NewLazyDLL("advapi32.dll")
	getNamedSecurityInfo = securityAPI.NewProc("GetNamedSecurityInfoW")
	descriptorToString   = securityAPI.NewProc("ConvertSecurityDescriptorToStringSecurityDescriptorW")
	stringToDescriptor   = securityAPI.NewProc("ConvertStringSecurityDescriptorToSecurityDescriptorW")
)

func securityText(descriptor *byte) (string, error) {
	var value *uint16
	var length uint32
	ok, _, err := descriptorToString.Call(uintptr(unsafe.Pointer(descriptor)), 1, 5,
		uintptr(unsafe.Pointer(&value)), uintptr(unsafe.Pointer(&length)))
	if ok == 0 {
		return "", err
	}
	defer syscall.LocalFree(syscall.Handle(unsafe.Pointer(value)))
	return syscall.UTF16ToString(unsafe.Slice(value, length)), nil
}

func validateTelemetryACL(path string) error {
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes, err := syscall.GetFileAttributes(name)
	if err != nil {
		return err
	}
	if attributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("unsafe telemetry object")
	}
	var descriptor *byte
	status, _, _ := getNamedSecurityInfo.Call(uintptr(unsafe.Pointer(name)), 1, 5, 0, 0, 0, 0,
		uintptr(unsafe.Pointer(&descriptor)))
	if status != 0 {
		return syscall.Errno(status)
	}
	defer syscall.LocalFree(syscall.Handle(unsafe.Pointer(descriptor)))
	sid, err := currentUserSID()
	if err != nil {
		return err
	}
	user, err := sid.String()
	if err != nil {
		return err
	}
	text, err := syscall.UTF16PtrFromString("O:" + user + "D:P(A;;FA;;;" + user + ")")
	if err != nil {
		return err
	}
	var expected *byte
	ok, _, err := stringToDescriptor.Call(uintptr(unsafe.Pointer(text)), 1,
		uintptr(unsafe.Pointer(&expected)), 0)
	if ok == 0 {
		return err
	}
	defer syscall.LocalFree(syscall.Handle(unsafe.Pointer(expected)))
	actualText, err := securityText(descriptor)
	if err != nil {
		return err
	}
	expectedText, err := securityText(expected)
	if err != nil {
		return err
	}
	if actualText != expectedText {
		return fmt.Errorf("unsafe telemetry permissions")
	}
	return nil
}

func currentUserSID() (*syscall.SID, error) {
	process, err := syscall.GetCurrentProcess()
	if err != nil {
		return nil, err
	}
	var token syscall.Token
	if err := syscall.OpenProcessToken(process, syscall.TOKEN_QUERY, &token); err != nil {
		return nil, err
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		return nil, err
	}
	return user.User.Sid.Copy()
}

func telemetryUserKey() (string, error) {
	sid, err := currentUserSID()
	if err != nil {
		return "", err
	}
	value, err := sid.String()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", digest[:16]), nil
}

func telemetryDirectory() (string, error) {
	key, err := telemetryUserKey()
	if err != nil {
		return "", err
	}
	return filepath.Join(os.TempDir(), "nowhere-telemetry-"+key), nil
}

func telemetryTransport() string { return "named_pipe" }

func telemetryEndpoint(id string) string {
	key, _ := telemetryUserKey()
	return `\\.\pipe\nowhere-telemetry-` + key + "-" + id
}

func telemetryNamespace() (string, error) { return telemetryUserKey() }

func validateTelemetryDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("unsafe telemetry directory")
	}
	return validateTelemetryACL(path)
}

func readTelemetryRegistry(path string) ([]byte, error) {
	if err := validateTelemetryDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxRegistrySize {
		return nil, fmt.Errorf("unsafe telemetry registry")
	}
	if err := validateTelemetryACL(path); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxRegistrySize+1))
	if err == nil && len(data) > maxRegistrySize {
		err = fmt.Errorf("oversized telemetry registry")
	}
	return data, err
}

type pipeAddr string

func (pipeAddr) Network() string  { return "named_pipe" }
func (p pipeAddr) String() string { return string(p) }

type pipeConn struct {
	*os.File
	endpoint string
}

func (c *pipeConn) LocalAddr() net.Addr  { return pipeAddr("local") }
func (c *pipeConn) RemoteAddr() net.Addr { return pipeAddr(c.endpoint) }

func dialTelemetry(ctx context.Context, endpoint string, pid uint32) (net.Conn, error) {
	prefix := `\\.\pipe\nowhere-telemetry-`
	key, err := telemetryUserKey()
	if err != nil || !strings.HasPrefix(endpoint, prefix+key+"-") {
		return nil, fmt.Errorf("invalid telemetry endpoint")
	}
	var file *os.File
	name, err := syscall.UTF16PtrFromString(endpoint)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var handle syscall.Handle
		handle, err = syscall.CreateFile(name, syscall.GENERIC_READ|syscall.GENERIC_WRITE, 0, nil,
			syscall.OPEN_EXISTING, syscall.FILE_FLAG_OVERLAPPED|0x00100000, 0)
		if err == nil {
			file = os.NewFile(uintptr(handle), endpoint)
			break
		}
		if !errors.Is(err, syscall.Errno(231)) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	var serverPID uint32
	result, _, callErr := getNamedPipeServerProcessID.Call(file.Fd(), uintptr(unsafe.Pointer(&serverPID)))
	if result == 0 || serverPID != pid {
		file.Close()
		return nil, fmt.Errorf("telemetry server process mismatch: %v", callErr)
	}
	serverSID, err := processUserSID(pid)
	currentSID, currentErr := currentUserSID()
	if err != nil || currentErr != nil || !equalSID(serverSID, currentSID) {
		file.Close()
		return nil, fmt.Errorf("telemetry server user mismatch")
	}
	return &pipeConn{File: file, endpoint: endpoint}, nil
}

func equalSID(left, right *syscall.SID) bool {
	if left == nil || right == nil {
		return false
	}
	leftValue, leftErr := left.String()
	rightValue, rightErr := right.String()
	return leftErr == nil && rightErr == nil && leftValue == rightValue
}

func processUserSID(pid uint32) (*syscall.SID, error) {
	process, err := syscall.OpenProcess(processQueryLimitedInformation, false, pid)
	if err != nil {
		return nil, err
	}
	defer syscall.CloseHandle(process)
	var token syscall.Token
	if err := syscall.OpenProcessToken(process, syscall.TOKEN_QUERY, &token); err != nil {
		return nil, err
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		return nil, err
	}
	return user.User.Sid.Copy()
}

func validateTelemetryProcess(instance discoveredInstance) bool {
	process, err := syscall.OpenProcess(processQueryLimitedInformation, false, instance.PID)
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(process)
	var created, exited, kernel, user syscall.Filetime
	if syscall.GetProcessTimes(process, &created, &exited, &kernel, &user) != nil {
		return false
	}
	ticks := uint64(created.HighDateTime)<<32 | uint64(created.LowDateTime)
	if ticks < 116_444_736_000_000_000 {
		return false
	}
	return (ticks-116_444_736_000_000_000)/10_000 == instance.Incarnation
}
