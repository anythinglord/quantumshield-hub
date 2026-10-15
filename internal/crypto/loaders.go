package crypto

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// LoadKeyPair loads a PEM-encoded certificate chain and matching private key
// from disk (typically a leaf chain plus its key). Every failure is wrapped
// with %w so callers can inspect the underlying error.
func LoadKeyPair(certFile, keyFile string) (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("crypto: load key pair %q: %w", certFile, err)
	}
	return cert, nil
}

// LoadCertPool builds a certificate pool from one or more PEM files. Each file
// is read and appended in order; the function fails if any file is unreadable
// or if any file yields zero parseable certificates (AppendCertsFromPEM
// reports a bool, never an error, so an explicit error is created).
func LoadCertPool(pemFiles ...string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, path := range pemFiles {
		pem, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("crypto: read cert pool file %q: %w", path, err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("crypto: no certificates parsed from %q", path)
		}
	}
	return pool, nil
}
