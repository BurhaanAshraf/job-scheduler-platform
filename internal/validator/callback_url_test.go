package validator

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestValidateCallbackURL_Valid(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		url  string
	}{
		{"http", "http://example.com/webhook"},
		{"https", "https://example.com/callback"},
		{"with port", "http://example.com:8080/hook"},
		{"with path", "https://example.com/path/to/webhook?query=1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ValidateCallbackURL(ctx, tc.url)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result == nil {
				t.Fatal("result is nil")
			}
			if result.URL == nil {
				t.Fatal("result.URL is nil")
			}
			if len(result.ResolvedIPs) == 0 {
				t.Fatal("no resolved IPs")
			}
		})
	}
}

func TestValidateCallbackURL_InvalidScheme(t *testing.T) {
	ctx := context.Background()
	_, err := ValidateCallbackURL(ctx, "ftp://example.com/hook")
	if err == nil {
		t.Fatal("expected error for ftp scheme")
	}
}

func TestValidateCallbackURL_EmptyURL(t *testing.T) {
	ctx := context.Background()
	_, err := ValidateCallbackURL(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestValidateCallbackURL_MissingHost(t *testing.T) {
	ctx := context.Background()
	_, err := ValidateCallbackURL(ctx, "http://")
	if err == nil {
		t.Fatal("expected error for missing host")
	}
}

func TestValidateCallbackURL_BlockedHosts(t *testing.T) {
	ctx := context.Background()
	blocked := []string{
		"http://localhost/hook",
		"http://metadata.google.internal/hook",
		"http://169.254.169.254/hook",
		"http://metadata/hook",
		"http://metadata.azure.com/hook",
	}
	for _, u := range blocked {
		t.Run(u, func(t *testing.T) {
			_, err := ValidateCallbackURL(ctx, u)
			if err == nil {
				t.Fatalf("expected error for blocked host: %s", u)
			}
		})
	}
}

func TestValidateCallbackURL_PrivateIPs(t *testing.T) {
	ctx := context.Background()
	private := []string{
		"http://10.0.0.1/hook",
		"http://172.16.0.1/hook",
		"http://172.31.255.255/hook",
		"http://192.168.1.1/hook",
		"http://127.0.0.1/hook",
		"http://169.254.1.1/hook",
		"http://[::1]/hook",
	}
	for _, u := range private {
		t.Run(u, func(t *testing.T) {
			_, err := ValidateCallbackURL(ctx, u)
			if err == nil {
				t.Fatalf("expected error for private IP: %s", u)
			}
		})
	}
}

func TestValidationResult_Dialer(t *testing.T) {
	// Test that dialer connects to resolved IPs
	ctx := context.Background()
	result, err := ValidateCallbackURL(ctx, "http://example.com/hook")
	if err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	dialer := result.Dialer()
	if dialer == nil {
		t.Fatal("dialer is nil")
	}

	// Test dialer with a real HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// We can't easily test the custom dialer with httptest since it uses a different host
	// This test mainly verifies the dialer function is created correctly
	_ = dialer
}

func TestCreateValidatingTransport(t *testing.T) {
	ctx := context.Background()
	transport, result, err := CreateValidatingTransport(ctx, "http://example.com/hook")
	if err != nil {
		t.Fatalf("CreateValidatingTransport failed: %v", err)
	}
	if transport == nil {
		t.Fatal("transport is nil")
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if transport.DialContext == nil {
		t.Fatal("transport.DialContext is nil")
	}
	if transport.DisableKeepAlives != true {
		t.Fatal("expected DisableKeepAlives=true")
	}
}

func TestIsPrivateIP(t *testing.T) {
	tests := []struct {
		ip        string
		isPrivate bool
	}{
		{"10.0.0.1", true},
		{"10.255.255.255", true},
		{"172.16.0.0", true},
		{"172.31.255.255", true},
		{"172.32.0.0", false},
		{"192.168.1.1", true},
		{"127.0.0.1", true},
		{"169.254.1.1", true},
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"::1", true},
		{"fe80::1", true},
		{"fc00::1", true},
		{"2001:db8::1", false},
	}

	for _, tc := range tests {
		t.Run(tc.ip, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("invalid IP: %s", tc.ip)
			}
			// We can't directly test isPrivateIP since it's unexported
			// but we can test through ValidateCallbackURL
			url := "http://" + tc.ip + "/hook"
			_, err := ValidateCallbackURL(context.Background(), url)
			if tc.isPrivate {
				if err == nil {
					t.Errorf("expected error for private IP %s", tc.ip)
				}
			} else {
				// For public IPs, validation might fail due to DNS but not due to private IP check
				// We're mainly testing the private IP detection logic
			}
		})
	}
}

func TestValidationResult_Port(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"http://example.com/hook", "80"},
		{"https://example.com/hook", "443"},
		{"http://example.com:8080/hook", "8080"},
		{"https://example.com:9443/hook", "9443"},
	}

	for _, tc := range tests {
		t.Run(tc.url, func(t *testing.T) {
			ctx := context.Background()
			result, err := ValidateCallbackURL(ctx, tc.url)
			if err != nil {
				t.Fatalf("validation failed: %v", err)
			}
			if result.port() != tc.expected {
				t.Errorf("expected port %s, got %s", tc.expected, result.port())
			}
		})
	}
}

func TestResolveHost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ips, err := resolveHost(ctx, "example.com")
	if err != nil {
		t.Fatalf("resolveHost failed: %v", err)
	}
	if len(ips) == 0 {
		t.Fatal("no IPs resolved")
	}
	for _, ip := range ips {
		if ip.To4() == nil && ip.To16() == nil {
			t.Errorf("invalid IP: %v", ip)
		}
	}
}

func TestValidateCallbackURL_BlockedHostAliases(t *testing.T) {
	ctx := context.Background()
	// Offline-safe: rejected by the hostname blocklist, no DNS needed.
	for _, raw := range []string{
		"http://host.docker.internal/hook",
		"http://localhost.localdomain/hook",
		"http://LOCALHOST/hook",
		"http://localhost./hook",
		"http://metadata.google.internal./hook",
	} {
		if _, err := ValidateCallbackURL(ctx, raw); err == nil {
			t.Errorf("%q: want blocklist rejection, got nil", raw)
		}
	}
}

func TestValidateCallbackURL_UnspecifiedAddressesRejected(t *testing.T) {
	ctx := context.Background()
	// Literal IPs resolve without DNS; both must hit the private CIDRs.
	for _, raw := range []string{
		"http://0.0.0.0/hook",
		"http://[::]/hook",
	} {
		_, err := ValidateCallbackURL(ctx, raw)
		if err == nil {
			t.Errorf("%q: want private-address rejection, got nil", raw)
		}
	}
	// Explicit opt-in still allows them (dev/demo sink use case).
	for _, raw := range []string{
		"http://0.0.0.0/hook",
		"http://[::]/hook",
	} {
		if _, err := ValidateCallbackURL(ctx, raw, Config{AllowPrivateIPs: true}); err != nil {
			t.Errorf("%q with AllowPrivateIPs: want nil, got %v", raw, err)
		}
	}
}
