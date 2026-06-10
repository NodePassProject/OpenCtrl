package master

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net/url"
	"sync"
	"time"
)

// tlsMode mirrors the public tls query parameter.
//
// The numeric values are intentionally stable because they are surfaced through
// the info endpoint and used in master:// URL configuration.
type tlsMode int

const (
	// tlsNone disables TLS when the caller explicitly allows plaintext. It is
	// useful for local-only deployments and reverse-proxy termination.
	tlsNone tlsMode = iota
	// tlsSelfSigned generates an in-memory certificate for private deployments.
	// The certificate is not persisted and changes on each process start.
	tlsSelfSigned
	// tlsCATrusted loads operator-provided certificate files from crt/key URL
	// parameters and supports periodic reload.
	tlsCATrusted
)

// serverTLSOptions captures the policy differences between server entrypoints.
//
// allowNone lets the master API run in plaintext, while stricter callers can
// require TLS by setting it false. nextProtos is passed through for protocols
// that need ALPN without changing the shared TLS loader.
type serverTLSOptions struct {
	defaultMode tlsMode
	allowNone   bool
	nextProtos  []string
}

// newTLSConfig returns an in-memory self-signed certificate chain.
//
// The certificate is sufficient for encrypted private control-plane traffic
// where trust is established out of band. It uses ECDSA P-256 and a one-year
// validity window to keep generation fast and the TLS configuration modern.
func newTLSConfig() (*tls.Config, error) {
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("Master.newTLSConfig: generate private key failed: %w", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("Master.newTLSConfig: generate serial failed: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	crtBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &private.PublicKey, private)
	if err != nil {
		return nil, fmt.Errorf("Master.newTLSConfig: create certificate failed: %w", err)
	}

	keyBytes, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, fmt.Errorf("Master.newTLSConfig: marshal private key failed: %w", err)
	}

	// Use PKCS#8 for broad tooling compatibility and keep both PEM blocks only
	// in memory.
	crtPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: crtBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})

	cert, err := tls.X509KeyPair(crtPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("Master.newTLSConfig: parse key pair failed: %w", err)
	}

	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}

// newServerTLSConfig resolves the tls/crt/key URL parameters into a server TLS
// configuration and the mode reported by the API.
//
// Empty tls uses opts.defaultMode. tls=0 is only accepted when allowNone is
// true. tls=2 requires crt/key to load successfully at startup so the server
// never begins with an invalid certificate configuration.
func newServerTLSConfig(parsedURL *url.URL, opts serverTLSOptions) (tlsMode, *tls.Config, error) {
	mode := opts.defaultMode
	switch parsedURL.Query().Get("tls") {
	case "":
	case "0":
		mode = tlsNone
	case "1":
		mode = tlsSelfSigned
	case "2":
		mode = tlsCATrusted
	default:
		return tlsNone, nil, fmt.Errorf("Master.newServerTLSConfig: invalid tls mode")
	}
	if mode == tlsNone {
		if opts.allowNone {
			return tlsNone, nil, nil
		}
		return tlsNone, nil, fmt.Errorf("Master.newServerTLSConfig: tls=1 or tls=2 required")
	}

	var tlsConfig *tls.Config
	if mode == tlsCATrusted {
		crtFile, keyFile := parsedURL.Query().Get("crt"), parsedURL.Query().Get("key")
		cert, err := tls.LoadX509KeyPair(crtFile, keyFile)
		if err != nil {
			return tlsNone, nil, fmt.Errorf("Master.newServerTLSConfig: load certificate failed: %w", err)
		}

		var (
			mu         sync.RWMutex
			cachedCert = cert
			lastReload = time.Now()
		)
		tlsConfig = &tls.Config{
			GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
				// Reload lazily from the handshake path. Double-check under the
				// write lock so concurrent handshakes do not all hit disk after
				// the interval elapses.
				mu.RLock()
				shouldReload := time.Since(lastReload) >= reloadInterval
				mu.RUnlock()
				if shouldReload {
					mu.Lock()
					if time.Since(lastReload) >= reloadInterval {
						if newCert, err := tls.LoadX509KeyPair(crtFile, keyFile); err != nil {
							log.Printf("Master.newServerTLSConfig: certificate reload failed: %v", err)
						} else {
							log.Printf("Master.newServerTLSConfig: certificate reloaded: %v", crtFile)
							cachedCert = newCert
						}
						lastReload = time.Now()
					}
					mu.Unlock()
				}

				mu.RLock()
				cert := cachedCert
				mu.RUnlock()
				return &cert, nil
			},
		}
	} else {
		var err error
		tlsConfig, err = newTLSConfig()
		if err != nil {
			return tlsNone, nil, fmt.Errorf("Master.newServerTLSConfig: create self-signed config failed: %w", err)
		}
	}

	// TLS 1.3 is the minimum supported version for the control-plane server.
	tlsConfig.MinVersion = tls.VersionTLS13
	tlsConfig.NextProtos = opts.nextProtos
	return mode, tlsConfig, nil
}

// String returns the stable wire value used by the info endpoint.
//
// It deliberately returns the URL parameter values instead of Go enum names so
// clients can round-trip the mode back into master:// configuration.
func (m tlsMode) String() string {
	switch m {
	case tlsSelfSigned:
		return "1"
	case tlsCATrusted:
		return "2"
	default:
		return "0"
	}
}
