"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { fmt, listRuns, publicApiUrl, type RunSummary } from "@/lib/api";
import { ProgressBar } from "./progress-bar";

// Run list, server-rendered on first load, then refreshed from the REST API.
export function RunTable({ initial }: { initial: RunSummary[] }) {
  const [runs, setRuns] = useState(initial);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const next = await listRuns(publicApiUrl);
        if (alive) {
          setRuns(next);
          setError(null);
        }
      } catch (e) {
        if (alive) setError((e as Error).message);
      }
    };
    const id = setInterval(tick, 2000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  return (
    <div className="panel scroll">
      {error && <p className="err">{error}</p>}
      <table>
        <thead>
          <tr>
            <th>Suite</th>
            <th>Type</th>
            <th>Created</th>
            <th>Progress</th>
            <th className="num">Jobs</th>
            <th className="num">Pending</th>
            <th className="num">Cached</th>
            <th className="num">Failed</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((r) => (
            <tr key={r.run_id}>
              <td>
                <Link href={`/runs/${r.run_id}`}>{r.suite || r.run_id.slice(0, 8)}</Link>
              </td>
              <td className="mono">{r.job_type}</td>
              <td>{new Date(r.created_at).toLocaleTimeString()}</td>
              <td>
                <ProgressBar total={r.total} succeeded={r.succeeded} cached={r.cached} failed={r.failed} />
              </td>
              <td className="num">{fmt(r.total)}</td>
              <td className="num">{fmt(r.pending)}</td>
              <td className="num">{fmt(r.cached)}</td>
              <td className="num">{fmt(r.failed)}</td>
            </tr>
          ))}
          {runs.length === 0 && (
            <tr>
              <td colSpan={8}>No runs yet.</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
