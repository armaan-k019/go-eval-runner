CREATE TABLE IF NOT EXISTS runs (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    suite        text        NOT NULL,
    job_type     text        NOT NULL,
    config       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    max_attempts int         NOT NULL,
    timeout_ms   int         NOT NULL,
    no_cache     bool        NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- status: queued -> leased -> succeeded | failed (or back to queued on retry)
--         waiting (parked behind an identical in-flight job) -> cached | queued
--         cached (answered from result_cache, executor never called)
CREATE TABLE IF NOT EXISTS jobs (
    id               bigserial PRIMARY KEY,
    run_id           uuid        NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    case_id          text        NOT NULL,
    job_type         text        NOT NULL,
    payload          jsonb       NOT NULL,
    cache_key        text        NOT NULL,
    status           text        NOT NULL CHECK (status IN ('queued','leased','waiting','succeeded','failed','cached')),
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL,
    next_run_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    leased_by        text,
    leased_at        timestamptz,
    lease_expires_at timestamptz,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at      timestamptz,
    result           jsonb,
    error            text,
    score            double precision,
    passed           bool
);

CREATE INDEX IF NOT EXISTS jobs_queue_idx   ON jobs (next_run_at, id) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_lease_idx   ON jobs (lease_expires_at) WHERE status = 'leased';
CREATE INDEX IF NOT EXISTS jobs_waiting_idx ON jobs (cache_key) WHERE status = 'waiting';
CREATE INDEX IF NOT EXISTS jobs_inflight_idx ON jobs (cache_key) WHERE status IN ('queued','leased');
CREATE INDEX IF NOT EXISTS jobs_run_idx     ON jobs (run_id, status);

-- One row per executor attempt, including failed and expired ones.
CREATE TABLE IF NOT EXISTS attempts (
    job_id      bigint      NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     int         NOT NULL,
    run_id      uuid        NOT NULL,
    worker_id   text        NOT NULL,
    leased_at   timestamptz NOT NULL,
    reported_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    outcome     text        NOT NULL CHECK (outcome IN ('success','error','lease_expired')),
    error       text,
    exec_us     bigint,
    PRIMARY KEY (job_id, attempt)
);
CREATE INDEX IF NOT EXISTS attempts_run_idx ON attempts (run_id);

-- Successful results keyed by sha256(job_type, config, payload).
CREATE TABLE IF NOT EXISTS result_cache (
    cache_key  text PRIMARY KEY,
    result     jsonb       NOT NULL,
    score      double precision,
    passed     bool,
    source_job bigint,
    hits       bigint      NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
