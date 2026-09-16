# Redis Keys

This document defines the Redis key and consumer-group naming conventions
used by the job scheduler.

## Ready Stream

### Stream

`jobs:ready`

Contains jobs that are ready to be processed by workers.

### Consumer Group

`workers`

All worker instances consume from the `jobs:ready` stream through the
`workers` consumer group.

## Rate Limiting

### Key Pattern

`rate_limit:<client_id>:<window>`

Where:

- `<client_id>` is the authenticated API client identity.
- `<window>` identifies the fixed rate-limit window.

These keys are used by the API rate limiter.

## Naming Rules

- Use lowercase names.
- Use `:` as the namespace separator.
- Keep stream names stable because multiple binaries depend on them.
- Consumer-group names are documented separately from Redis key names.
