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

type tlsMode int

const (
	tlsNone tlsMode = iota

	tlsSelfSigned

	tlsCATrusted
)

type serverTLSOptions struct {
	defaultMode tlsMode
	allowNone   bool
	nextProtos  []string
}

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

	crtPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: crtBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})

	cert, err := tls.X509KeyPair(crtPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("Master.newTLSConfig: parse key pair failed: %w", err)
	}

	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}

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

	tlsConfig.MinVersion = tls.VersionTLS13
	tlsConfig.NextProtos = opts.nextProtos
	return mode, tlsConfig, nil
}

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
