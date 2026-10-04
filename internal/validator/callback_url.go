package validator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	ErrInvalidURL     = errors.New("invalid callback URL")
	ErrPrivateAddress = errors.New("callback URL resolves to private address")
	ErrBlockedHost    = errors.New("callback URL host is blocked")
	ErrDNSResolution  = errors.New("DNS resolution failed")
	ErrTimeout        = errors.New("validation timeout")
)

const (
	validationTimeout = 5 * time.Second
	dialTimeout       = 3 * time.Second
)

// Config holds optional settings for URL validation.
// Use Config{AllowPrivateIPs: true} in tests to allow localhost/private IPs.
type Config struct {
	AllowPrivateIPs bool
}

var (
	blockedHosts = map[string]struct{}{
		"localhost":                {},
		"metadata.google.internal": {},
		"169.254.169.254":          {}, // AWS metadata
		"metadata":                 {}, // Azure metadata
		"metadata.azure.com":       {},
	}
	privateCIDRs = []*net.IPNet{
		mustParseCIDR("10.0.0.0/8"),
		mustParseCIDR("172.16.0.0/12"),
		mustParseCIDR("192.168.0.0/16"),
		mustParseCIDR("127.0.0.0/8"),
		mustParseCIDR("169.254.0.0/16"),
		mustParseCIDR("::1/128"),
		mustParseCIDR("fe80::/10"),
		mustParseCIDR("fc00::/7"),
	}
)

func mustParseCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return n
}

// ValidateCallbackURL performs comprehensive validation of a callback URL
// including SSRF protection (private IP blocking) and DNS rebinding prevention.
func ValidateCallbackURL(ctx context.Context, rawURL string, opts ...Config) (*ValidationResult, error) {
	cfg := Config{}
	if len(opts) > 0 {
		cfg = opts[0]
	}

	ctx, cancel := context.WithTimeout(ctx, validationTimeout)
	defer cancel()

	if strings.TrimSpace(rawURL) == "" {
		return nil, ErrInvalidURL
	}

	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: scheme must be http or https", ErrInvalidURL)
	}

	if parsed.Host == "" {
		return nil, fmt.Errorf("%w: missing host", ErrInvalidURL)
	}

	hostname := parsed.Hostname()
	if _, blocked := blockedHosts[strings.ToLower(hostname)]; blocked {
		return nil, fmt.Errorf("%w: %s", ErrBlockedHost, hostname)
	}

	// Resolve DNS once and capture IPs
	ips, err := resolveHost(ctx, hostname)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDNSResolution, err)
	}

	// Check all resolved IPs against private ranges (unless allowed)
	if !cfg.AllowPrivateIPs {
		for _, ip := range ips {
			if isPrivateIP(ip) {
				return nil, fmt.Errorf("%w: %s resolves to %s", ErrPrivateAddress, hostname, ip)
			}
		}
	}

	return &ValidationResult{
		URL:           parsed,
		ResolvedIPs:   ips,
		ValidatedHost: hostname,
	}, nil
}

// ValidationResult holds the validated URL and its resolved IPs
// for DNS rebinding prevention.
type ValidationResult struct {
	URL           *url.URL
	ResolvedIPs   []net.IP
	ValidatedHost string
}

// Dialer returns an http.DialContext that connects directly to the
// pre-resolved IPs, preventing DNS rebinding attacks.
func (r *ValidationResult) Dialer() func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		// addr is "host:port" - we ignore the host and use our resolved IPs
		var lastErr error
		for _, ip := range r.ResolvedIPs {
			dialer := &net.Dialer{
				Timeout: dialTimeout,
			}
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), r.port()))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		return nil, fmt.Errorf("all resolved IPs failed: %w", lastErr)
	}
}

func (r *ValidationResult) port() string {
	if r.URL.Port() != "" {
		return r.URL.Port()
	}
	if r.URL.Scheme == "https" {
		return "443"
	}
	return "80"
}

func resolveHost(ctx context.Context, host string) ([]net.IP, error) {
	r := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: dialTimeout}
			return d.DialContext(ctx, network, address)
		},
	}
	ips, err := r.LookupIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, errors.New("no IPs returned")
	}
	return ips, nil
}

func isPrivateIP(ip net.IP) bool {
	for _, cidr := range privateCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// CreateValidatingTransport creates an HTTP transport that validates
// the callback URL and uses a custom dialer to prevent DNS rebinding.
func CreateValidatingTransport(ctx context.Context, callbackURL string, opts ...Config) (*http.Transport, *ValidationResult, error) {
	result, err := ValidateCallbackURL(ctx, callbackURL, opts...)
	if err != nil {
		return nil, nil, err
	}

	transport := &http.Transport{
		DialContext:           result.Dialer(),
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          10,
		IdleConnTimeout:       30 * time.Second,
		DisableKeepAlives:     true, // Prevent connection reuse across different validated URLs
	}

	return transport, result, nil
}
