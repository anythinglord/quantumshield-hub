package crypto

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// pki holds the filesystem paths of a generated PKI fixture.
type pki struct {
	dir        string
	rootCA     string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

var (
	testPKI *pki
	pkiSkip string
	pkiErr  error
)

// This file owns the hermetic PKI fixture used by the integration tests. The
// fixture is generated into a temp directory by scripts/gen-certs.sh and torn
// down after the test binary finishes. The TestMain and every integration
// subtest degrade to a skip (never a failure) when openssl, the script, or
// -short mode make generation impossible, so the unit tier stays green.

func TestMain(m *testing.M) {
	flag.Parse()

	switch {
	case testing.Short():
		pkiSkip = "short mode (-short)"
	case !hasOpenSSL():
		pkiSkip = "openssl not found in PATH"
	default:
		script, ok := locateScript()
		if !ok {
			pkiSkip = "scripts/gen-certs.sh not found"
			break
		}
		p, err := generatePKI(script)
		if err != nil {
			// Record the failure; only integration subtests surface it as fatal.
			pkiErr = err
			break
		}
		testPKI = p
	}

	code := m.Run()
	if testPKI != nil {
		_ = os.RemoveAll(testPKI.dir)
	}
	os.Exit(code)
}

// hasOpenSSL reports whether the openssl CLI is available in PATH.
func hasOpenSSL() bool {
	_, err := exec.LookPath("openssl")
	return err == nil
}

// locateScript resolves scripts/gen-certs.sh relative to this source file so
// the fixture works regardless of the test working directory, falling back to
// a package-relative path.
func locateScript() (string, bool) {
	if _, file, _, ok := runtime.Caller(0); ok {
		candidate := filepath.Join(filepath.Dir(file), "..", "..", "scripts", "gen-certs.sh")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, true
		}
	}
	candidate := filepath.Join("..", "..", "scripts", "gen-certs.sh")
	if _, err := os.Stat(candidate); err == nil {
		abs, err := filepath.Abs(candidate)
		if err != nil {
			return candidate, true
		}
		return abs, true
	}
	return "", false
}

// generatePKI runs the PKI script into a fresh temp dir and returns the fixture
// paths. The script is idempotent and exits 0 for an empty directory.
func generatePKI(script string) (*pki, error) {
	dir, err := os.MkdirTemp("", "qs-certs-*")
	if err != nil {
		return nil, fmt.Errorf("crypto: create temp PKI dir: %w", err)
	}

	cmd := exec.Command(script)
	cmd.Env = envWith("CERTS_DIR", dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("crypto: generate PKI: %w: %s", err, out)
	}

	return &pki{
		dir:        dir,
		rootCA:     filepath.Join(dir, "ca", "root-ca.crt"),
		serverCert: filepath.Join(dir, "server", "server-chain.crt"),
		serverKey:  filepath.Join(dir, "server", "server.key"),
		clientCert: filepath.Join(dir, "client", "client-chain.crt"),
		clientKey:  filepath.Join(dir, "client", "client.key"),
	}, nil
}

// envWith returns the process environment with key set to val, replacing any
// pre-existing occurrence so the child sees exactly one value.
func envWith(key, val string) []string {
	prefix := key + "="
	env := make([]string, 0, len(os.Environ())+1)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, prefix) {
			continue
		}
		env = append(env, kv)
	}
	return append(env, prefix+val)
}

// requirePKI skips the calling integration test when the fixture is
// unavailable, or fails it when generation genuinely broke.
func requirePKI(t *testing.T) {
	t.Helper()
	if pkiSkip != "" {
		t.Skipf("skipping mTLS integration test: %s", pkiSkip)
	}
	if pkiErr != nil {
		t.Fatalf("mTLS PKI fixture generation failed: %v", pkiErr)
	}
}

// runHandshake performs one loopback TLS handshake with the given configs and
// returns the negotiated connection states plus any handshake errors. It never
// fails the test itself, so callers can assert on rejection paths.
func runHandshake(serverCfg, clientCfg *tls.Config) (clientState, serverState tls.ConnectionState, clientErr, serverErr error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return tls.ConnectionState{}, tls.ConnectionState{}, err, err
	}
	defer ln.Close()

	type serverResult struct {
		state tls.ConnectionState
		err   error
	}
	serverCh := make(chan serverResult, 1)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverCh <- serverResult{err: err}
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

		tlsConn := tls.Server(conn, serverCfg)
		if err := tlsConn.Handshake(); err != nil {
			serverCh <- serverResult{err: err}
			return
		}
		serverCh <- serverResult{state: tlsConn.ConnectionState()}
	}()

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 10*time.Second)
	if err != nil {
		<-serverCh
		return tls.ConnectionState{}, tls.ConnectionState{}, err, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	tlsClient := tls.Client(conn, clientCfg)
	if err := tlsClient.Handshake(); err != nil {
		clientErr = err
	} else {
		clientState = tlsClient.ConnectionState()
	}

	res := <-serverCh
	return clientState, res.state, clientErr, res.err
}

// TestPKILoaders loads a real certificate chain and CA pool from the generated
// PKI (spec Requirement 3 — Valid PEM loads).
func TestPKILoaders(t *testing.T) {
	requirePKI(t)

	serverCert, err := LoadKeyPair(testPKI.serverCert, testPKI.serverKey)
	if err != nil {
		t.Fatalf("LoadKeyPair(server): %v", err)
	}
	if got := len(serverCert.Certificate); got < 2 {
		t.Errorf("server chain length = %d, want >= 2 (leaf + intermediate)", got)
	}

	clientCert, err := LoadKeyPair(testPKI.clientCert, testPKI.clientKey)
	if err != nil {
		t.Fatalf("LoadKeyPair(client): %v", err)
	}
	if got := len(clientCert.Certificate); got < 2 {
		t.Errorf("client chain length = %d, want >= 2 (leaf + intermediate)", got)
	}

	roots, err := LoadCertPool(testPKI.rootCA)
	if err != nil {
		t.Fatalf("LoadCertPool(root): %v", err)
	}
	// CertPool.Subjects() is deprecated; assert the loaded pool matches a pool
	// built from the same PEM instead.
	rootPEM, err := os.ReadFile(testPKI.rootCA)
	if err != nil {
		t.Fatalf("read root CA: %v", err)
	}
	want := x509.NewCertPool()
	if !want.AppendCertsFromPEM(rootPEM) {
		t.Fatal("root CA file yielded no parseable certificates")
	}
	if !roots.Equal(want) {
		t.Error("LoadCertPool(root) did not load the certificates in the root CA file")
	}
}

// TestStrictMTLSHandshake proves a loopback mTLS handshake over the generated
// PKI (spec Requirement 8).
func TestStrictMTLSHandshake(t *testing.T) {
	requirePKI(t)

	serverCert, err := LoadKeyPair(testPKI.serverCert, testPKI.serverKey)
	if err != nil {
		t.Fatalf("LoadKeyPair(server): %v", err)
	}
	clientCert, err := LoadKeyPair(testPKI.clientCert, testPKI.clientKey)
	if err != nil {
		t.Fatalf("LoadKeyPair(client): %v", err)
	}
	roots, err := LoadCertPool(testPKI.rootCA)
	if err != nil {
		t.Fatalf("LoadCertPool(root): %v", err)
	}

	serverCfg := ServerTLSConfig(serverCert, roots)
	clientCfg := ClientTLSConfig(clientCert, roots, "localhost")

	t.Run("mutual TLS negotiates hybrid PQC", func(t *testing.T) {
		clientState, serverState, clientErr, serverErr := runHandshake(serverCfg, clientCfg)
		if clientErr != nil {
			t.Fatalf("client handshake: %v", clientErr)
		}
		if serverErr != nil {
			t.Fatalf("server handshake: %v", serverErr)
		}

		if got, want := clientState.Version, uint16(tls.VersionTLS13); got != want {
			t.Errorf("client TLS version = %#x, want %#x", got, want)
		}
		if got, want := clientState.CurveID, tls.X25519MLKEM768; got != want {
			t.Errorf("client negotiated curve = %v, want %v", got, want)
		}
		if len(clientState.VerifiedChains) == 0 {
			t.Error("client VerifiedChains empty, want the verified server chain")
		}

		if got, want := serverState.Version, uint16(tls.VersionTLS13); got != want {
			t.Errorf("server TLS version = %#x, want %#x", got, want)
		}
		if got, want := serverState.CurveID, tls.X25519MLKEM768; got != want {
			t.Errorf("server negotiated curve = %v, want %v", got, want)
		}
		if len(serverState.VerifiedChains) == 0 {
			t.Error("server VerifiedChains empty, want the verified client chain")
		}
	})

	t.Run("cert-less client rejected", func(t *testing.T) {
		noCertCfg := ClientTLSConfig(tls.Certificate{}, roots, "localhost")
		_, _, _, serverErr := runHandshake(serverCfg, noCertCfg)
		if serverErr == nil {
			t.Error("server accepted a client without a certificate, want rejection")
		}
	})
}
