# Worker Reliability

## PostgreSQL and Redis acknowledgement ordering

A successfully executed job is finalized in this order:

1. Execute the HTTP callback.
2. Update the job status to `done` in PostgreSQL.
3. Acknowledge the Redis stream message with `XACK`.

PostgreSQL is updated before `XACK` because the database contains the durable job state.

### PostgreSQL update fails

If the PostgreSQL status update fails:

- Do not acknowledge the Redis message.
- The message remains pending in the consumer group's Pending Entries List.
- Later pending-message recovery can retry it.

### XACK fails after PostgreSQL succeeds

If PostgreSQL successfully records the job as `done` but `XACK` fails:

- The job remains `done` in PostgreSQL.
- The Redis message remains pending.
- Pending-message recovery may encounter the message again.
- The worker must consult the PostgreSQL job state before executing the callback again, so a successfully completed job is not unnecessarily re-executed.

The PostgreSQL state is authoritative for whether the job has completed.
