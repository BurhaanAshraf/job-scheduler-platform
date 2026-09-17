package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const promoteDueScript = `
local job_ids = redis.call(
	"ZRANGEBYSCORE",
	KEYS[1],
	"-inf",
	ARGV[1]
)

for _, job_id in ipairs(job_ids) do
	local payload = redis.call(
		"HGET",
		KEYS[2],
		job_id
	)

	if payload then
		redis.call(
			"XADD",
			KEYS[3],
			"*",
			"job_id",
			job_id,
			"payload",
			payload
		)

		redis.call("ZREM",KEYS[1],job_id)

		redis.call("HDEL",KEYS[2],job_id)
	end
end

return #job_ids
`

func PromoteDue(
	ctx context.Context,
	client *redis.Client,
	now time.Time,
) (int64, error) {
	result, err := client.Eval(
		ctx,
		promoteDueScript,
		[]string{
			ScheduledSet,
			ScheduledPayloads,
			ReadyStream,
		},
		now.Unix(),
	).Int64()

	if err != nil {
		return 0, fmt.Errorf("promote due jobs: %w", err)
	}

	return result, nil
}
