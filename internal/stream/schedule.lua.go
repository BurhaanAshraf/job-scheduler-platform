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
	ARGV[1],
	"LIMIT",
	0,
	500
)

local promoted = {}
for _, job_id in ipairs(job_ids) do
	local scheduled = redis.call(
		"HGET",
		KEYS[2],
		job_id
	)

	if scheduled then
		local ok, data = pcall(cjson.decode, scheduled)
		if ok then
			redis.call(
				"XADD",
				KEYS[3],
				"MAXLEN",
				"~",
				"100000",
				"*",
				"job_id",
				job_id,
				"payload",
				data.payload,
				"queue_generation",
				tostring(data.queue_generation)
			)

			redis.call("ZREM", KEYS[1], job_id)

			redis.call("HDEL", KEYS[2], job_id)
			table.insert(promoted, job_id)
		else
			-- Poison entry (corrupt JSON): drop it so one bad payload
			-- cannot head-of-line-block every promotion tick. The DB row
			-- still exists for reconciliation; the error surfaces via the
			-- caller's failed-promotion log, not a silent stall.
			redis.call("ZREM", KEYS[1], job_id)
			redis.call("HDEL", KEYS[2], job_id)
		end
	else
		-- Orphaned ZSET member (payload lost via eviction/manual DEL):
		-- remove it so it is not returned by every future tick forever.
		redis.call("ZREM", KEYS[1], job_id)
	end
end

return promoted`

// PromoteDueWithIDs atomically promotes due jobs and returns the promoted
// job IDs so callers can log per-job correlation lines (10.3).
func PromoteDueWithIDs(
	ctx context.Context,
	client *redis.Client,
	now time.Time,
) ([]string, error) {
	result, err := client.Eval(
		ctx,
		promoteDueScript,
		[]string{
			ScheduledSet,
			ScheduledPayloads,
			ReadyStream,
		},
		now.Unix(),
	).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("promote due jobs: %w", err)
	}
	return result, nil
}

func PromoteDue(
	ctx context.Context,
	client *redis.Client,
	now time.Time,
) (int64, error) {
	ids, err := PromoteDueWithIDs(ctx, client, now)
	if err != nil {
		return 0, err
	}
	return int64(len(ids)), nil
}
