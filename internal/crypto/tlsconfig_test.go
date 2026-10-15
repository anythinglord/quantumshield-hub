package crypto

import (
	"crypto/tls"
	"crypto/x509"
	"reflect"
	"testing"
)

// TestServerTLSConfig verifies the server-side mTLS policy never weakens
// transport security (spec Requirement 1 — Enforcement).
func TestServerTLSConfig(t *testing.T) {
	clientCAs := x509.NewCertPool()
	serverCert := tls.Certificate{Certificate: [][]byte{{0xAA, 0xBB}}}

	cfg := ServerTLSConfig(serverCert, clientCAs)

	if cfg == nil {
		t.Fatal("ServerTLSConfig returned nil")
	}
	if got, want := cfg.MinVersion, uint16(tls.VersionTLS13); got != want {
		t.Errorf("MinVersion = %#x, want %#x (TLS 1.3)", got, want)
	}
	if got, want := cfg.ClientAuth, tls.RequireAndVerifyClientCert; got != want {
		t.Errorf("ClientAuth = %v, want %v", got, want)
	}
	if want := []tls.CurveID{tls.X25519MLKEM768}; !reflect.DeepEqual(cfg.CurvePreferences, want) {
		t.Errorf("CurvePreferences = %v, want %v", cfg.CurvePreferences, want)
	}
	if cfg.ClientCAs != clientCAs {
		t.Errorf("ClientCAs = %p, want the injected pool %p", cfg.ClientCAs, clientCAs)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("Certificates length = %d, want 1", len(cfg.Certificates))
	}
	if !reflect.DeepEqual(cfg.Certificates[0].Certificate, serverCert.Certificate) {
		t.Errorf("attached server certificate = %v, want %v", cfg.Certificates[0].Certificate, serverCert.Certificate)
	}
}

// TestClientTLSConfig verifies the client-side policy carries identity,
// trust roots, and the target server name under TLS 1.3 (spec Requirement 2 —
// Client identity and trust).
func TestClientTLSConfig(t *testing.T) {
	rootCAs := x509.NewCertPool()
	clientCert := tls.Certificate{Certificate: [][]byte{{0x01, 0x02, 0x03}}}
	const serverName = "ingestion-service.quantumshield.svc.cluster.local"

	cfg := ClientTLSConfig(clientCert, rootCAs, serverName)

	if cfg == nil {
		t.Fatal("ClientTLSConfig returned nil")
	}
	if got, want := cfg.MinVersion, uint16(tls.VersionTLS13); got != want {
		t.Errorf("MinVersion = %#x, want %#x (TLS 1.3)", got, want)
	}
	if cfg.RootCAs != rootCAs {
		t.Errorf("RootCAs = %p, want the injected pool %p", cfg.RootCAs, rootCAs)
	}
	if got, want := cfg.ServerName, serverName; got != want {
		t.Errorf("ServerName = %q, want %q", got, want)
	}
	if want := []tls.CurveID{tls.X25519MLKEM768}; !reflect.DeepEqual(cfg.CurvePreferences, want) {
		t.Errorf("CurvePreferences = %v, want %v", cfg.CurvePreferences, want)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("Certificates length = %d, want 1", len(cfg.Certificates))
	}
	if !reflect.DeepEqual(cfg.Certificates[0].Certificate, clientCert.Certificate) {
		t.Errorf("attached client certificate = %v, want %v", cfg.Certificates[0].Certificate, clientCert.Certificate)
	}
}
