// Standalone demo tool for showcasing registry error diagnostics.
// Starts local test servers that simulate various registry failure modes
// and runs errdiag.Classify against real errors from go-containerregistry.
//
// Usage: go run ./scripts/demo-registry-errors/
//
// Press Enter to step through each scenario.
// NOT intended to be committed to the project.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/pkg/registry/errdiag"
)

const (
	bold  = "\033[1m"
	dim   = "\033[2m"
	reset = "\033[0m"
	cyan  = "\033[36m"
)

type demo struct {
	name    string            // scenario title
	request string            // what request is being made
	setup   string            // how the server is configured
	fn      func() error      // actual demo function
}

var demos = []demo{
	{
		name:    "EOF (proxy/middleware terminating connection)",
		request: "HEAD http://<server>/v2/test/manifests/latest",
		setup:   "Server accepts TCP, then closes connection immediately (Hijack + Close)",
		fn:      demoEOF,
	},
	{
		name:    "TLS/certificate verification failed",
		request: "HEAD https://<server>/v2/test/manifests/latest (no CA trust)",
		setup:   "httptest.NewTLSServer with self-signed cert, client does NOT skip TLS verify",
		fn:      demoTLSCert,
	},
	{
		name:    "Authentication failed (HTTP 401)",
		request: "HEAD http://<server>/v2/test/manifests/latest",
		setup:   "Server returns 401 with {\"errors\":[{\"code\":\"UNAUTHORIZED\"}]}",
		fn:      demoAuth401,
	},
	{
		name:    "Access denied (HTTP 403)",
		request: "HEAD http://<server>/v2/test/manifests/latest",
		setup:   "Server returns 403 with {\"errors\":[{\"code\":\"DENIED\"}]}",
		fn:      demoAuth403,
	},
	{
		name:    "Rate limited (HTTP 429)",
		request: "HEAD http://<server>/v2/test/manifests/latest",
		setup:   "Server returns 429 with {\"errors\":[{\"code\":\"TOOMANYREQUESTS\"}]}",
		fn:      demoRateLimit,
	},
	{
		name:    "Server error (HTTP 500)",
		request: "HEAD http://<server>/v2/test/manifests/latest",
		setup:   "Server returns 500 with {\"errors\":[{\"code\":\"UNKNOWN\"}]}",
		fn:      demoServerError500,
	},
	{
		name:    "Server error (HTTP 502)",
		request: "HEAD http://<server>/v2/test/manifests/latest",
		setup:   "Server returns 502 Bad Gateway (raw HTML body)",
		fn:      demoServerError502,
	},
	{
		name:    "Server error (HTTP 503)",
		request: "HEAD http://<server>/v2/test/manifests/latest",
		setup:   "Server returns 503 with {\"errors\":[{\"code\":\"UNAVAILABLE\"}]}",
		fn:      demoServerError503,
	},
	{
		name:    "DNS resolution failure",
		request: "HEAD https://nonexistent.invalid:443/v2/test/manifests/latest",
		setup:   "No server - hostname uses .invalid TLD (RFC 2606, guaranteed unresolvable)",
		fn:      demoDNS,
	},
	{
		name:    "Connection refused",
		request: "HEAD http://<closed-port>/v2/test/manifests/latest",
		setup:   "Bind port with net.Listen, close it, then connect - nothing accepts",
		fn:      demoConnRefused,
	},
	{
		name:    "Operation timeout",
		request: "HEAD http://<server>/v2/test/manifests/latest (timeout 1s)",
		setup:   "Server handler sleeps forever, client has context.WithTimeout(1s)",
		fn:      demoTimeout,
	},
}

func main() {
	_ = os.Setenv("FORCE_COLOR", "1")
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println(bold + "=== Registry Error Diagnostics Demo ===" + reset)
	fmt.Println(dim + "Press Enter to step through each scenario" + reset)
	fmt.Println()

	for i, d := range demos {
		// Clear screen and move cursor to top
		fmt.Print("\033[2J\033[H")

		fmt.Println(bold + "=== Registry Error Diagnostics Demo ===" + reset)
		fmt.Printf(dim+"Scenario %d/%d"+reset+"\n\n", i+1, len(demos))

		fmt.Printf(bold+"--- %s ---"+reset+"\n", d.name)
		fmt.Printf(cyan+"  Request: "+reset+"%s\n", d.request)
		fmt.Printf(cyan+"  Setup:   "+reset+"%s\n", d.setup)

		if err := d.fn(); err != nil {
			fmt.Fprintf(os.Stderr, "  demo setup error: %v\n", err)
		}

		if i < len(demos)-1 {
			fmt.Print(dim + "Press Enter for next..." + reset)
			scanner.Scan()
		}
	}

	fmt.Println("\n" + bold + "=== Done ===" + reset)
}

func headImage(ctx context.Context, host string, insecure bool) error {
	var opts []name.Option
	if insecure {
		opts = append(opts, name.Insecure)
	}
	ref, err := name.ParseReference(host+"/test:latest", opts...)
	if err != nil {
		return err
	}
	_, err = remote.Head(ref, remote.WithContext(ctx))
	return err
}

func showDiagnostic(err error) {
	diag := errdiag.Classify(err)
	if diag != nil {
		fmt.Fprint(os.Stderr, diag.Format())
	} else {
		fmt.Fprintf(os.Stderr, "  [unclassified] %v\n", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}{
		Errors: []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{{code, msg}},
	})
}

// --- Demo functions ---

func demoEOF() error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, _ := h.Hijack()
		conn.Close()
	}))
	defer server.Close()

	err := headImage(context.Background(), trimHTTP(server.URL), true)
	showDiagnostic(err)
	return nil
}

func demoTLSCert() error {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	ref, _ := name.ParseReference(trimHTTPS(server.URL)+"/test:latest", name.StrictValidation)
	_, err := remote.Head(ref)
	showDiagnostic(err)
	return nil
}

func demoAuth401() error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
	}))
	defer server.Close()

	err := headImage(context.Background(), trimHTTP(server.URL), true)
	showDiagnostic(err)
	return nil
}

func demoAuth403() error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusForbidden, "DENIED", "requested access to the resource is denied")
	}))
	defer server.Close()

	err := headImage(context.Background(), trimHTTP(server.URL), true)
	showDiagnostic(err)
	return nil
}

func demoRateLimit() error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusTooManyRequests, "TOOMANYREQUESTS", "rate limit exceeded")
	}))
	defer server.Close()

	err := headImage(context.Background(), trimHTTP(server.URL), true)
	showDiagnostic(err)
	return nil
}

func demoServerError500() error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusInternalServerError, "UNKNOWN", "internal server error")
	}))
	defer server.Close()

	err := headImage(context.Background(), trimHTTP(server.URL), true)
	showDiagnostic(err)
	return nil
}

func demoServerError502() error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "<html>502 Bad Gateway</html>")
	}))
	defer server.Close()

	err := headImage(context.Background(), trimHTTP(server.URL), true)
	showDiagnostic(err)
	return nil
}

func demoServerError503() error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "service temporarily unavailable")
	}))
	defer server.Close()

	err := headImage(context.Background(), trimHTTP(server.URL), true)
	showDiagnostic(err)
	return nil
}

func demoDNS() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := headImage(ctx, "nonexistent.invalid:443", false)
	showDiagnostic(err)
	return nil
}

func demoConnRefused() error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	addr := listener.Addr().String()
	listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	connErr := headImage(ctx, addr, true)
	showDiagnostic(connErr)
	return nil
}

func demoTimeout() error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(30 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := headImage(ctx, trimHTTP(server.URL), true)
	showDiagnostic(err)
	return nil
}

func trimHTTP(url string) string  { return strings.TrimPrefix(url, "http://") }
func trimHTTPS(url string) string { return strings.TrimPrefix(url, "https://") }
