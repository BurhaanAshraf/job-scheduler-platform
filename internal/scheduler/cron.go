package scheduler

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

func NextRunAt(expr string, from time.Time) (time.Time, error) {
	schedule, err := cron.ParseStandard(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cron expression: %w", err)
	}

	return schedule.Next(from), nil
}
