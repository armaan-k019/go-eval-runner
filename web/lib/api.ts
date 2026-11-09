// Types and URLs for the apiserver's REST and WebSocket endpoints.

export type RunSummary = {
  run_id: string;
  suite: string;
  job_type: string;
  created_at: string;
  total: number;
  pending: number;
  succeeded: number;
  cached: number;
  failed: number;
};

export type RunStats = {
  run_id: string;
  suite: string;
  done: boolean;
  total: number;
  queued: number;
  leased: number;
  waiting: number;
  succeeded: number;
  cached: number;
  failed: number;
  retries: number;
  lease_expirations: number;
  executor_calls: number;
  wall_seconds: number;
  jobs_per_minute: number;
  attempt_p50_ms: number;
  attempt_p95_ms: number;
  overhead_p50_ms: number;
  overhead_p95_ms: number;
  exec_p50_ms: number;
  exec_p95_ms: number;
  mean_score: number;
  pass_rate: number;
  cache_saved: number;
};

// Browser-facing base URL of the apiserver.
export const publicApiUrl = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8080";

// Base URL used by server-side rendering. Inside a cluster this is the
// apiserver Service, which the browser cannot reach directly.
export const serverApiUrl = process.env.API_URL ?? publicApiUrl;

export function wsUrl(runId: string): string {
  return publicApiUrl.replace(/^http/, "ws") + `/api/runs/${encodeURIComponent(runId)}/ws`;
}

export async function listRuns(base: string): Promise<RunSummary[]> {
  const res = await fetch(`${base}/api/runs?limit=50`, { cache: "no-store" });
  if (!res.ok) throw new Error(`list runs: HTTP ${res.status}`);
  const body = (await res.json()) as { runs: RunSummary[] };
  return body.runs;
}

export type SubmitBody = {
  suite: string;
  job_type: string;
  config: Record<string, unknown>;
  cases: { case_id: string; payload: Record<string, unknown> }[];
  no_cache?: boolean;
};

export async function submitRun(body: SubmitBody): Promise<{ run_id: string; jobs: number; cache_hits: number }> {
  const res = await fetch(`${publicApiUrl}/api/runs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error ?? `HTTP ${res.status}`);
  return json;
}

export function fmt(n: number, digits = 0): string {
  return n.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits });
}
