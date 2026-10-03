# Rollback runbook (14.4)

Previous task-definition revisions are immutable — rollback = point the
service back at the last good revision and let ECS replace tasks.

## Find revisions

```bash
R=ap-south-2
aws ecs describe-services --cluster job-scheduler --services job-scheduler-api \
  --region $R --query 'services[0].deployments[*].taskDefinition'
# e.g. [.../job-scheduler-api:9, .../job-scheduler-api:8]  (9 = bad, 8 = last good)
```

## Roll back one service (example: api to revision 8)

```bash
aws ecs update-service --cluster job-scheduler --service job-scheduler-api \
  --task-definition job-scheduler-api:8 --force-new-deployment --region $R
aws ecs wait services-stable --cluster job-scheduler \
  --services job-scheduler-api --region $R
curl -s http://job-scheduler-alb-172264349.ap-south-2.elb.amazonaws.com/healthz
# expect {"status":"ok"}
```

Repeat for `job-scheduler-scheduler` / `job-scheduler-worker` if the bad
deploy touched them. The deploy workflow (`deploy.yml`) tags every image with
the commit SHA, so a revision maps 1:1 to the merged commit — check
`git log` to confirm the revision you restore predates the bad merge.
