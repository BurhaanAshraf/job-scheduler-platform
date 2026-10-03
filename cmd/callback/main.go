package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	maxBodyBytes = 1 << 20 // 1 MiB per callback; matches worker executor bound.
	maxHits      = 200     // in-memory ring; demo traffic only, not durable.
)

// Hit is one received callback delivery.
type Hit struct {
	Time      time.Time `json:"time"`
	Method    string    `json:"method"`
	Path      string    `json:"path"`
	JobID     string    `json:"job_id"`
	Attempt   string    `json:"attempt"`
	IdemKey   string    `json:"idempotency_key"`
	Body      string    `json:"body"`
	BodyTrunc bool      `json:"body_truncated"`
}

type Sink struct {
	mu        sync.Mutex
	hits      []Hit
	failFirst int // demo chaos knob: fail this many /hook calls, then succeed.
}

func (s *Sink) record(h Hit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits = append([]Hit{h}, s.hits...)
	if len(s.hits) > maxHits {
		s.hits = s.hits[:maxHits]
	}
}

func (s *Sink) snapshot() []Hit {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Hit, len(s.hits))
	copy(out, s.hits)
	return out
}

func readBody(w http.ResponseWriter, r *http.Request) (body []byte, trunc, ok bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes+1024)
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return nil, false, false
	}
	if len(body) > maxBodyBytes {
		return body[:maxBodyBytes], true, true
	}
	return body, false, true
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	sink := &Sink{}
	if n := os.Getenv("SINK_FAIL_FIRST"); n != "" {
		fmt.Sscanf(n, "%d", &sink.failFirst)
	}

	// POST /hook — the demo callback. Records everything, usually 200.
	http.HandleFunc("POST /hook", func(w http.ResponseWriter, r *http.Request) {
		body, trunc, ok := readBody(w, r)
		if !ok {
			return
		}
		sink.mu.Lock()
		fail := sink.failFirst > 0
		if fail {
			sink.failFirst--
		}
		sink.mu.Unlock()
		hit := Hit{
			Time:      time.Now().UTC(),
			Method:    r.Method,
			Path:      r.URL.Path,
			JobID:     r.Header.Get("X-Job-ID"),
			Attempt:   r.Header.Get("X-Job-Attempt"),
			IdemKey:   r.Header.Get("Idempotency-Key"),
			Body:      string(body),
			BodyTrunc: trunc,
		}
		sink.record(hit)
		log.Info("callback received", "job_id", hit.JobID, "attempt", hit.Attempt, "bytes", len(body), "failed", fail)
		if fail {
			http.Error(w, "demo failure (SINK_FAIL_FIRST)", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// GET /hits — JSON feed of recent deliveries (for tests/demos).
	http.HandleFunc("GET /hits", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sink.snapshot())
	})

	// GET /reset — clear history (demo convenience).
	http.HandleFunc("GET /reset", func(w http.ResponseWriter, r *http.Request) {
		sink.mu.Lock()
		sink.hits = nil
		sink.mu.Unlock()
		_, _ = w.Write([]byte("ok"))
	})

	// GET / — human page showing recent callbacks live.
	http.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		buf.WriteString(`<!doctype html><html><head><meta charset="utf-8"><meta http-equiv="refresh" content="5"><title>Callback sink</title><style>body{font-family:monospace;max-width:900px;margin:2em auto;padding:0 1em}li{margin:.4em 0;border-bottom:1px solid #ddd;padding-bottom:.4em}</style></head><body><h1>Public callback sink</h1><p>POST jobs here as <code>/hook</code>. Auto-refreshes. Total: `)
		hits := sink.snapshot()
		fmt.Fprintf(&buf, "%d</p><ol>", len(hits))
		for _, h := range hits {
			fmt.Fprintf(&buf, "<li><b>%s</b> job=<b>%s</b> attempt=%s idem=%s<br>%s%s</li>",
				h.Time.Format("15:04:05"), html.EscapeString(h.JobID), html.EscapeString(h.Attempt),
				html.EscapeString(h.IdemKey), html.EscapeString(h.Body), map[bool]string{true: "…(truncated)", false: ""}[h.BodyTrunc])
		}
		buf.WriteString("</ol></body></html>")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = buf.WriteTo(w)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Info("callback sink listening", "addr", ":"+port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Error("sink stopped", "error", err)
		os.Exit(1)
	}
}
