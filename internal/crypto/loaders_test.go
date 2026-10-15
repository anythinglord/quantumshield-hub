package crypto

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadKeyPairInvalid verifies every failure path returns a non-nil error
// and never a usable certificate (spec Requirement 3 — Invalid PEM fails).
func TestLoadKeyPairInvalid(t *testing.T) {
	dir := t.TempDir()
	nonPEM := filepath.Join(dir, "garbage.pem")
	if err := os.WriteFile(nonPEM, []byte("this is not a PEM block"), 0o600); err != nil {
		t.Fatalf("seed non-PEM file: %v", err)
	}

	tests := []struct {
		name     string
		certFile string
		keyFile  string
	}{
		{
			name:     "nonexistent paths",
			certFile: filepath.Join(dir, "missing.crt"),
			keyFile:  filepath.Join(dir, "missing.key"),
		},
		{
			name:     "non-PEM content",
			certFile: nonPEM,
			keyFile:  nonPEM,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert, err := LoadKeyPair(tt.certFile, tt.keyFile)
			if err == nil {
				t.Fatalf("LoadKeyPair(%q, %q) = nil error, want non-nil", tt.certFile, tt.keyFile)
			}
			if cert.Certificate != nil {
				t.Errorf("LoadKeyPair returned populated certificate on failure: %v", cert.Certificate)
			}
		})
	}
}

// TestLoadCertPoolInvalid verifies unreadable files and files with no valid
// PEM certificates both fail with a nil pool (spec Requirement 3 — Invalid
// PEM fails).
func TestLoadCertPoolInvalid(t *testing.T) {
	dir := t.TempDir()
	nonPEM := filepath.Join(dir, "not-a-cert.pem")
	if err := os.WriteFile(nonPEM, []byte("-----BEGIN NOTHING-----\nAAAA\n-----END NOTHING-----\n"), 0o600); err != nil {
		t.Fatalf("seed non-PEM file: %v", err)
	}

	tests := []struct {
		name  string
		files []string
	}{
		{
			name:  "nonexistent path",
			files: []string{filepath.Join(dir, "missing.crt")},
		},
		{
			name:  "non-PEM content",
			files: []string{nonPEM},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := LoadCertPool(tt.files...)
			if err == nil {
				t.Fatalf("LoadCertPool(%v) = nil error, want non-nil", tt.files)
			}
			if pool != nil {
				t.Errorf("LoadCertPool returned non-nil pool on failure: %p", pool)
			}
		})
	}
}
