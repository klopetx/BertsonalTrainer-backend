# Roadmap (Target: P1 Complete)

This roadmap captures what remains to consider the project **P1 complete** (daily batch + Gold analytics outputs, rankings, measurement evidence, and CI), based on the repo's own priorities and acceptance criteria.

Last reviewed: 2026-09-08

## Current Status (As-Is)

- Git: clean worktree on `main`, up to date with `origin/main`.
- P0 streaming vertical slice exists:
  - `simulator -> Kafka -> Spark Structured Streaming -> MinIO Bronze/Silver -> PostgreSQL serving.provisional_scores`.
- P1 in progress:
  - Spark batch job exists and writes preliminary Gold outputs.
  - Scheduler service exists (polling loop) and can invoke the batch.
- CI is not present (no `.github/workflows/`).

## P1 Complete Definition

P1 complete means:

- A deterministic, idempotent daily batch that produces **all Gold tables** described in `docs/postgres-model.md`.
- Weekly/monthly rankings materialized from Gold daily scores.
- Scheduler behavior matches the agreed cutoff policy (without inventing readiness rules).
- Acceptance criteria are measured and recorded (evidence-backed).
- CI runs the main automated checks.

## Blockers / Open Decisions (Must Be Approved)

These are explicitly called out as open in the repo docs. Do not guess defaults.

1. Scheduler post-cutoff readiness delay/check.
   - Reference: `docs/architecture.md` (scheduler section)

## Remaining Work To Reach P1 Complete

### 1) Complete Gold Outputs (Weekly/Monthly Rankings)

Gold schemas are documented as including weekly and monthly rankings, but they are not currently produced by the batch.

Deliverables:

- Implement and write:
  - `gold.weekly_rankings`
  - `gold.monthly_rankings`
- Ensure **replace-by-scope** is transactional across all affected scopes:
  - `business_date`
  - derived `week_start_date` (Monday)
  - derived `month_start_date` (first day of month)

References:

- `docs/postgres-model.md`
- `docs/batch-and-scoring.md`

### 2) Final Scoring (Current Model)

The current scoring model is:

- originality via `daily_word_score` / `daily_score`
- rhyme difficulty via hardness (dictionary-size multiplier)
- `final_score = hardness_weighted_daily_score`

Deliverables:

- Keep the components auditable and ensure batch reprocessing yields identical results for the same Silver input (idempotent).

References:

- `docs/batch-and-scoring.md`
- `docs/postgres-model.md`

### 3) Scheduler Behavior: Cutoff + Readiness

The cutoff guardrails exist (`--force` for local/demo), but the post-cutoff readiness rule is still an open decision.

Deliverables:

- Implement the approved readiness delay/check after cutoff.
- Confirm logs clearly show whether a run is skipped due to cutoff, readiness, or no data.

References:

- `docs/architecture.md`
- `docs/batch-and-scoring.md`

### 4) Acceptance Criteria Evidence (Measured + Recorded)

Evidence must be recorded (commands, measurements, PASS/FAIL). Profiles to validate:

- 20 users/day (demo)
- 500 users/day (development)
- 1,000 users/day (nominal)
- 60,000 users/day (load test)

Required measurements:

- Provisional scoring latency (nominal): p95 <= 5s, p99 <= 10s
- Accelerated-load throughput: >= 100 events/s average
- Accelerated-load completion: <= 10 min for 60,000 sessions
- Daily batch runtime: <= 10 min (60,000 profile)
- Gold rerun idempotency: no diffs after rerun (0 duplicates)

Reference:

- `docs/acceptance-criteria.md`

### 5) CI (Pre-Delivery Requirement)

Add CI workflows that run the main automated checks.

Deliverables:

- GitHub Actions workflow(s) in `.github/workflows/` running `./scripts/test.sh` (and any lightweight lint/type checks if already established).

Reference:

- `docs/acceptance-criteria.md` (SMART-6)

### 6) Python Version Consistency (Documentation + Tooling)

Repo runtime is Python 3.11 (docs + containers + local tooling aligned).

Deliverables:

- Keep docs/scripts pinned to Python 3.11 (avoid ambiguous `py -3` selection).

References:

- `AGENTS.md`
- `README.md`
- `scripts/test.sh`

## Milestones

### Milestone 1: Approvals (Unblock Implementation)

- Approve scheduler readiness delay/check after cutoff.

Output:

- Updated docs reflecting the decisions (and code aligned to them).

### Milestone 2: Gold Completion (Rankings + Scope Replacement)

- Implement `gold.weekly_rankings` and `gold.monthly_rankings`.
- Ensure transactional replace-by-scope across day/week/month.
- Add/extend tests for ranking correctness and rerun idempotency.

### Milestone 3: Acceptance Evidence

- Execute the four workload profiles.
- Record SLI/SLO measurements and PASS/FAIL evidence.

### Milestone 4: CI + Reproducibility Polish

- Add GitHub Actions workflows.
- Align Python version references.
- Confirm scripted demo flow matches acceptance criteria.

## Next Actions (Concrete)

1. Document the approved scheduler readiness rule (decision record + doc updates).
2. Implement weekly/monthly rankings in the batch and add tests.
3. Add Gold rerun idempotency integration test(s) (same date -> identical Gold state).
4. Add CI workflow running `./scripts/test.sh`.
5. Run and record acceptance evidence for the 4 profiles.
