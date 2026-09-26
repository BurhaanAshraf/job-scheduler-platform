package scheduler

import (
	"testing"
	"time"
)

func TestNextRunAt(t *testing.T) {
	loc := time.UTC

	tests := []struct {
		name string
		expr string
		from time.Time
		want time.Time
	}{
		{
			name: "every five minutes",
			expr: "*/5 * * * *",
			from: time.Date(2026, 9, 23, 14, 2, 0, 0, loc),
			want: time.Date(2026, 9, 23, 14, 5, 0, 0, loc),
		},
		{
			name: "daily at midnight",
			expr: "0 0 * * *",
			from: time.Date(2026, 9, 23, 14, 30, 0, 0, loc),
			want: time.Date(2026, 9, 24, 0, 0, 0, 0, loc),
		},
		{
			name: "weekly sunday midnight",
			expr: "0 0 * * 0",
			from: time.Date(2026, 9, 23, 14, 30, 0, 0, loc),
			want: time.Date(2026, 9, 27, 0, 0, 0, 0, loc),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NextRunAt(tt.expr, tt.from)
			if err != nil {
				t.Fatalf("nextRunAt() error = %v", err)
			}

			if !got.Equal(tt.want) {
				t.Fatalf("nextRunAt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNextRunAt_InvalidExpression(t *testing.T) {
	from := time.Date(2026, 9, 23, 14, 0, 0, 0, time.UTC)

	_, err := NextRunAt("not-a-cron", from)
	if err == nil {
		t.Fatal("nextRunAt() error = nil, want error")
	}
}
