# ecs/ — legacy console task definitions (retired)

The hand-written `taskdef-*.json` and IAM policy files that used to live
here were removed: they had drifted from the real infrastructure (wrong
secret ARNs, stale log groups, no `SCHEDULER_INSTANCE_ID` handling) and
nothing referenced them.

**Terraform (`terraform/modules/`) is the sole source of truth** for task
definitions, execution/task roles, and policies. Do not recreate JSON copies
here — change the modules and let CI (`terraform validate`) review them.
